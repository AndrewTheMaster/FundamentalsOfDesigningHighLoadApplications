package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"lsmdb/pkg/raftadapter"
	"lsmdb/pkg/store"
	"sync"
)

// RaftCluster представляет один Raft кластер (группу реплик)
type RaftCluster struct {
	ID         string   // идентификатор кластера (например, "cluster1")
	LeaderAddr string   // адрес текущего лидера
	Replicas   []string // все адреса реплик в этом кластере
}

// ShardedRaftDB объединяет шардирование и репликацию через Raft
type ShardedRaftDB struct {
	localAddr string

	// Raft node (если эта нода участвует в каком-то кластере)
	raftNode *raftadapter.Node

	// Router для шардирования между кластерами
	ring *HashRing

	// Маппинг кластеров: clusterID -> RaftCluster
	clustersMu sync.RWMutex
	clusters   map[string]*RaftCluster

	// HTTP клиент для связи с другими кластерами
	clientFactory func(addr string) (Remote, error)

	// Локальное хранилище
	store iStoreAPI
}

type iStoreAPI interface {
	PutString(key, value string) error
	GetString(key string) (string, bool, error)
	Delete(key string) error
}

// NewShardedRaftDB создает новую шардированную БД с репликацией
func NewShardedRaftDB(
	localAddr string,
	raftNode *raftadapter.Node,
	store iStoreAPI,
	clientFactory func(addr string) (Remote, error),
) *ShardedRaftDB {
	return &ShardedRaftDB{
		localAddr:     localAddr,
		raftNode:      raftNode,
		ring:          NewHashRing(150), // 150 виртуальных нод
		clusters:      make(map[string]*RaftCluster),
		clientFactory: clientFactory,
		store:         store,
	}
}

// AddCluster добавляет Raft кластер в топологию шардирования
func (s *ShardedRaftDB) AddCluster(clusterID string, leaderAddr string, replicas []string) {
	s.clustersMu.Lock()
	defer s.clustersMu.Unlock()

	s.clusters[clusterID] = &RaftCluster{
		ID:         clusterID,
		LeaderAddr: leaderAddr,
		Replicas:   replicas,
	}

	s.ring.AddNode(clusterID)

	slog.Info(
		"cluster added to topology",
		"cluster_id", clusterID,
		"leader", leaderAddr,
		"replicas", len(replicas),
	)
}

// RemoveCluster удаляет кластер из топологии
func (s *ShardedRaftDB) RemoveCluster(clusterID string) {
	s.clustersMu.Lock()
	defer s.clustersMu.Unlock()

	delete(s.clusters, clusterID)
	s.ring.RemoveNode(clusterID)

	slog.Info("cluster removed from topology", "cluster_id", clusterID)
}

// UpdateClusterLeader обновляет адрес лидера кластера
func (s *ShardedRaftDB) UpdateClusterLeader(clusterID string, newLeaderAddr string) {
	s.clustersMu.Lock()
	defer s.clustersMu.Unlock()

	if cluster, ok := s.clusters[clusterID]; ok {
		cluster.LeaderAddr = newLeaderAddr
		slog.Info("cluster leader updated", "cluster_id", clusterID, "new_leader", newLeaderAddr)
	}
}

// getClusterForKey определяет, какой Raft кластер отвечает за ключ
func (s *ShardedRaftDB) getClusterForKey(key string) (*RaftCluster, error) {
	// Используем consistent hashing для определения кластера
	clusterID, ok := s.ring.GetNode(key)
	if !ok {
		return nil, fmt.Errorf("no cluster available for key: %s", key)
	}

	s.clustersMu.RLock()
	cluster, ok := s.clusters[clusterID]
	s.clustersMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("cluster not found: %s", clusterID)
	}

	return cluster, nil
}

// isLocalCluster проверяет, является ли этот кластер локальным
func (s *ShardedRaftDB) isLocalCluster(cluster *RaftCluster) bool {
	// Проверяем, входит ли наш адрес в список реплик этого кластера
	for _, addr := range cluster.Replicas {
		if addr == s.localAddr {
			return true
		}
	}
	return false
}

// Put записывает значение с шардированием и репликацией
func (s *ShardedRaftDB) Put(ctx context.Context, key, value string) error {
	cluster, err := s.getClusterForKey(key)
	if err != nil {
		return err
	}

	// Если это локальный кластер - используем Raft напрямую
	if s.isLocalCluster(cluster) {
		if s.raftNode == nil {
			return fmt.Errorf("raft node is not initialized")
		}

		cmd := raftadapter.NewCmd(store.InsertOp, []byte(key), []byte(value))
		return s.raftNode.Execute(ctx, cmd)
	}

	// Иначе отправляем запрос на лидера удаленного кластера
	client, err := s.clientFactory(cluster.LeaderAddr)
	if err != nil {
		return fmt.Errorf("failed to create client for %s: %w", cluster.LeaderAddr, err)
	}

	return client.PutString(key, value)
}

// Get читает значение с учетом шардирования
func (s *ShardedRaftDB) Get(ctx context.Context, key string) (string, bool, error) {
	cluster, err := s.getClusterForKey(key)
	if err != nil {
		return "", false, err
	}

	// Если это локальный кластер - читаем локально
	if s.isLocalCluster(cluster) {
		return s.store.GetString(key)
	}

	// Иначе читаем с любой реплики удаленного кластера
	// Пробуем последовательно все реплики
	var lastErr error
	for _, addr := range cluster.Replicas {
		client, err := s.clientFactory(addr)
		if err != nil {
			lastErr = err
			continue
		}

		value, found, err := client.GetString(key)
		if err == nil {
			return value, found, nil
		}
		lastErr = err
	}

	return "", false, fmt.Errorf("failed to read from all replicas: %w", lastErr)
}

// Delete удаляет значение с шардированием и репликацией
func (s *ShardedRaftDB) Delete(ctx context.Context, key string) error {
	cluster, err := s.getClusterForKey(key)
	if err != nil {
		return err
	}

	// Если это локальный кластер - используем Raft напрямую
	if s.isLocalCluster(cluster) {
		if s.raftNode == nil {
			return fmt.Errorf("raft node is not initialized")
		}

		cmd := raftadapter.NewCmd(store.DeleteOp, []byte(key), nil)
		return s.raftNode.Execute(ctx, cmd)
	}

	// Иначе отправляем запрос на лидера удаленного кластера
	client, err := s.clientFactory(cluster.LeaderAddr)
	if err != nil {
		return fmt.Errorf("failed to create client for %s: %w", cluster.LeaderAddr, err)
	}

	return client.Delete(key)
}

// GetLocalClusterID возвращает ID кластера, к которому принадлежит эта нода
func (s *ShardedRaftDB) GetLocalClusterID() string {
	s.clustersMu.RLock()
	defer s.clustersMu.RUnlock()

	for id, cluster := range s.clusters {
		if s.isLocalCluster(cluster) {
			return id
		}
	}
	return ""
}

// ListClusters возвращает список всех кластеров
func (s *ShardedRaftDB) ListClusters() []string {
	s.clustersMu.RLock()
	defer s.clustersMu.RUnlock()

	result := make([]string, 0, len(s.clusters))
	for id := range s.clusters {
		result = append(result, id)
	}
	return result
}
