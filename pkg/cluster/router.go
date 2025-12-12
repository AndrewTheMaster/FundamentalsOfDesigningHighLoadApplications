package cluster

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-zookeeper/zk"
)

// удалённый клиент
type Remote interface {
	PutString(key, value string) error
	GetString(key string) (string, bool, error)
	Delete(key string) error
}

// фабрика удалённых клиентов
type ClientFactory func(target string) (Remote, error)

// Шардовая Raft-нода: нам важен только KV-интерфейс.
type ShardRaftNode interface {
	PutString(key, value string) error
	GetString(key string) (string, bool, error)
	Delete(key string) error
}

type Router struct {
	LocalAddr  string
	Ring       *HashRing                 //кольцо по нодам — используется старым API и ZK-membership
	ShardRing  *HashRing                 //кольцо по логическим шардам "shard-0".."shard-N"
	Directory  *ShardDirectory           // каталог лидеров: shardID -> addr (через ZooKeeper)
	ShardNodes map[ShardID]ShardRaftNode // локальные Raft-ноды по шардам для этой машины
	DB         KV                        //для простого режима без шардинга
	NewClient  ClientFactory             // addr → RemoteStore (HTTP/gRPC-клиент)

	mu sync.RWMutex
}

// ===== 1. Старый API: Consistent Hashing по нодам =====
func (r *Router) owner(key string) (string, error) {
	r.mu.RLock()
	ring := r.Ring
	r.mu.RUnlock()

	if ring == nil {
		return "", fmt.Errorf("ring is not initialized")
	}

	node, ok := r.Ring.GetNode(key)
	if !ok {
		return "", fmt.Errorf("ring is empty")
	}
	return node, nil
}

func (r *Router) UpdateRing(newRing *HashRing) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Ring = newRing
	fmt.Println("[router] ring updated; nodes:", newRing.ListNodes())
}

func (r *Router) log(method, key, target string, local bool) {
	where := "remote"
	if local {
		where = "local"
	}

	fmt.Printf("[router] %-6s key=%s → %s (%s)\n", method, key, target, where)
}

func (r *Router) PutString(key, value string) error {
	target, err := r.owner(key)
	if err != nil {
		return err
	}

	local := target == r.LocalAddr
	r.log("PUT", key, target, local)

	if local {
		return r.DB.PutString(key, value)
	}

	cl, err := r.NewClient(target)
	if err != nil {
		return fmt.Errorf("router: create client: %w", err)
	}

	return cl.PutString(key, value)
}

func (r *Router) GetString(key string) (string, bool, error) {
	target, err := r.owner(key)
	if err != nil {
		return "", false, err
	}

	local := target == r.LocalAddr
	r.log("GET", key, target, local)

	if local {
		return r.DB.GetString(key)
	}

	cl, err := r.NewClient(target)
	if err != nil {
		return "", false, fmt.Errorf("router: create client: %w", err)
	}

	return cl.GetString(key)
}

func (r *Router) Delete(key string) error {
	target, err := r.owner(key)
	if err != nil {
		return err
	}

	local := target == r.LocalAddr
	r.log("DELETE", key, target, local)

	if local {
		return r.DB.Delete(key)
	}

	cl, err := r.NewClient(target)
	if err != nil {
		return fmt.Errorf("router: create client: %w", err)
	}

	return cl.Delete(key)
}

// ===== 2. key -> shardID -> leader через ZooKeeper =====
// shardForKey: Consistent Hashing по логическим shard-имёнам.
func (r *Router) shardForKey(key string) (ShardID, error) {
	// Если отдельно не инициализировали кольцо по шардам,
	// по-умолчанию используем node-кольцо, считая его кольцом по шардам.
	ring := r.ShardRing
	if ring == nil {
		ring = r.Ring
	}
	if ring == nil {
		return 0, fmt.Errorf("shard ring is not initialized")
	}

	id, err := ShardFromRing(ring, key)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (r *Router) localShardNode(id ShardID) (ShardRaftNode, bool) {
	if r.ShardNodes == nil {
		return nil, false
	}
	n, ok := r.ShardNodes[id]
	return n, ok
}

// Put — вариант B: key → shardID → raftGroup → leader
func (r *Router) Put(ctx context.Context, key, value string) error {
	shardID, err := r.shardForKey(key)
	if err != nil {
		return err
	}

	if r.Directory == nil {
		// запасной режим: старый вариант без шардинга
		return r.PutString(key, value)
	}

	leader, err := r.Directory.LeaderFor(shardID)
	if err != nil {
		return fmt.Errorf("resolve leader for shard %d: %w", shardID, err)
	}

	// лидер на этой ноде → идём в локальную Raft-группу
	if leader == r.LocalAddr {
		if node, ok := r.localShardNode(shardID); ok {
			return node.PutString(key, value) // внутри будет raft.Propose
		}
		return fmt.Errorf("no local shard node for shard %d", shardID)
	}

	// лидер на другой ноде → идём по RPC
	cl, err := r.NewClient(leader)
	if err != nil {
		return fmt.Errorf("router: create client for %s: %w", leader, err)
	}
	return cl.PutString(key, value)
}

func (r *Router) Get(ctx context.Context, key string) (string, bool, error) {
	shardID, err := r.shardForKey(key)
	if err != nil {
		return "", false, err
	}

	if r.Directory == nil {
		// запасной режим: старый вариант без шардинга
		return r.GetString(key)
	}

	leader, err := r.Directory.LeaderFor(shardID)
	if err != nil {
		return "", false, fmt.Errorf("resolve leader for shard %d: %w", shardID, err)
	}

	if leader == r.LocalAddr {
		if node, ok := r.localShardNode(shardID); ok {
			return node.GetString(key)
		}
		return "", false, fmt.Errorf("no local shard node for shard %d", shardID)
	}

	cl, err := r.NewClient(leader)
	if err != nil {
		return "", false, fmt.Errorf("router: create client: %w", err)
	}
	return cl.GetString(key)
}

// ===== 3. ShardDirectory: shardID -> leader (ZooKeeper) =====

// ShardDirectory хранит в ZooKeeper соответствие shardID -> адрес лидера
// в виде znode:  {base}/shard-{id}/leader
type ShardDirectory struct {
	zk      *zk.Conn
	base    string
	mu      sync.RWMutex
	leaders map[ShardID]string
}

// NewShardDirectory создаёт каталог лидеров по шард-ID.
func NewShardDirectory(conn *zk.Conn, base string) *ShardDirectory {
	if base == "" {
		base = "/lsmdb/shards"
	}
	return &ShardDirectory{
		zk:      conn,
		base:    base,
		leaders: make(map[ShardID]string),
	}
}

func (d *ShardDirectory) leaderPath(id ShardID) string {
	return fmt.Sprintf("%s/shard-%d/leader", d.base, id)
}

// LeaderFor возвращает адрес лидера для shardID,
// сначала смотря в локальный кэш, затем — в ZooKeeper.
func (d *ShardDirectory) LeaderFor(id ShardID) (string, error) {
	d.mu.RLock()
	if l, ok := d.leaders[id]; ok && l != "" {
		d.mu.RUnlock()
		return l, nil
	}
	d.mu.RUnlock()

	data, _, err := d.zk.Get(d.leaderPath(id))
	if err != nil {
		return "", err
	}
	leader := string(data)

	d.mu.Lock()
	d.leaders[id] = leader
	d.mu.Unlock()

	return leader, nil
}

// RegisterLeader регистрирует текущую ноду как лидера шарда в ZK.
// Используем ephemeral znode, чтобы при падении лидера запись исчезла.
func (d *ShardDirectory) RegisterLeader(id ShardID, addr string) error {
	if d == nil || d.zk == nil {
		return fmt.Errorf("shard directory is not initialized")
	}
	path := d.leaderPath(id)
	data := []byte(addr)

	// Пытаемся создать ephemeral-znode.
	_, err := d.zk.Create(path, data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNodeExists {
		// Если уже существует — просто обновим данные.
		_, stat, err := d.zk.Get(path)
		if err != nil {
			return err
		}
		_, err = d.zk.Set(path, data, stat.Version)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	d.mu.Lock()
	d.leaders[id] = addr
	d.mu.Unlock()
	return nil
}
