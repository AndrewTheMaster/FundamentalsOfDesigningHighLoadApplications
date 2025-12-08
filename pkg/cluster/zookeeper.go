package cluster

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
)

type ZKMembership struct {
	conn     *zk.Conn
	rootPath string
	local    string // node addr
}

// servers: ["zk1:2181", "zk2:2181"]
func NewZKMembership(servers []string, rootPath, localAddr string) (*ZKMembership, error) {
	conn, _, err := zk.Connect(servers, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("zk connect: %w", err)
	}
	return &ZKMembership{
		conn:     conn,
		rootPath: rootPath,
		local:    localAddr,
	}, nil
}

func (m *ZKMembership) Close() error {
	m.conn.Close()
	return nil
}

func (m *ZKMembership) ensurePath(path string) error {
	parts := strings.Split(path, "/")
	cur := ""
	for _, p := range parts {
		if p == "" {
			continue
		}
		cur = cur + "/" + p
		exists, _, err := m.conn.Exists(cur)
		if err != nil {
			return err
		}
		if !exists {
			_, err = m.conn.Create(cur, nil, 0, zk.WorldACL(zk.PermAll))
			if err != nil && err != zk.ErrNodeExists {
				return err
			}
		}
	}
	return nil
}

// RegisterSelf создаёт ephemeral-узел для текущей ноды
func (m *ZKMembership) RegisterSelf() error {
	// Ждём, пока клиент реально подключится к ZK
	if err := m.waitConnected(10 * time.Second); err != nil {
		return err
	}

	if err := m.ensurePath(m.rootPath + "/nodes"); err != nil {
		return fmt.Errorf("ensure nodes path: %w", err)
	}

	nodePath := fmt.Sprintf("%s/nodes/%s", m.rootPath, m.local)

	_, err := m.conn.Create(nodePath, nil, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		return fmt.Errorf("create ephemeral node: %w", err)
	}

	fmt.Println("[zk] registered node:", nodePath)
	return nil
}

// readNodes читает список живых нод
func (m *ZKMembership) readNodes() ([]string, error) {
	children, _, err := m.conn.Children(m.rootPath + "/nodes")
	if err != nil {
		return nil, fmt.Errorf("zk children: %w", err)
	}
	return children, nil
}

// BuildRing строит HashRing на основе текущего списка нод
func (m *ZKMembership) BuildRing(replicas int) (*HashRing, error) {
	nodes, err := m.readNodes()
	if err != nil {
		return nil, err
	}
	ring := NewHashRing(replicas)
	for _, n := range nodes {
		ring.AddNode(n)
	}
	return ring, nil
}

// RunWatch запускает цикл: следит за изменениями /nodes и обновляет ring в Router
func (m *ZKMembership) RunWatch(ctx context.Context, r *Router, replicas int) {
	go func() {
		for {
			// первый раз читаем и подписываемся
			children, _, ch, err := m.conn.ChildrenW(m.rootPath + "/nodes")
			if err != nil {
				fmt.Println("[zk] ChildrenW error:", err)
				time.Sleep(2 * time.Second)
				continue
			}

			// пересобираем ring
			ring := NewHashRing(replicas)
			for _, n := range children {
				ring.AddNode(n)
			}
			r.UpdateRing(ring)

			select {
			case ev := <-ch:
				fmt.Println("[zk] event:", ev)
				// просто продолжаем цикл и перечитываем список нод
			case <-ctx.Done():
				fmt.Println("[zk] watch stopped")
				return
			}
		}
	}()
}

func (m *ZKMembership) waitConnected(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		st := m.conn.State()
		if st == zk.StateConnected || st == zk.StateHasSession {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("zk: not connected after %s, state=%v", timeout, st)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
