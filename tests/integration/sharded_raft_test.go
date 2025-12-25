//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"log/slog"
	"lsmdb/internal/http"
	"lsmdb/pkg/cluster"
	"lsmdb/pkg/config"
	"lsmdb/pkg/raftadapter"
	"lsmdb/pkg/store"
	"lsmdb/pkg/wal"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const (
	cluster1Port1 = 18080
	cluster1Port2 = 18081
	cluster1Port3 = 18082
	cluster2Port1 = 19080
	cluster2Port2 = 19081
	cluster2Port3 = 19082
)

// TestNode представляет одну тестовую ноду
type TestNode struct {
	ID         uint64
	ClusterID  string
	Port       int
	DataDir    string
	Store      *store.Store
	RaftNode   *raftadapter.Node
	HTTPServer *http.Server
	ShardedDB  *cluster.ShardedRaftDB
	cancel     context.CancelFunc
	running    bool
	mu         sync.Mutex
}

// TestCluster представляет тестовый кластер
type TestCluster struct {
	ID    string
	Nodes []*TestNode
}

func (n *TestNode) Start(t *testing.T) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.running {
		return fmt.Errorf("node already running")
	}

	_, cancel := context.WithCancel(context.Background())
	n.cancel = cancel

	// Запускаем HTTP сервер для Raft коммуникации
	if err := n.HTTPServer.Start(); err != nil {
		cancel()
		return fmt.Errorf("failed to start HTTP server: %w", err)
	}

	n.running = true
	t.Logf(" Started node %s:%d on port %d", n.ClusterID, n.ID, n.Port)
	return nil
}

func (n *TestNode) Stop(t *testing.T) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.running {
		return nil
	}

	if n.cancel != nil {
		n.cancel()
	}

	if n.HTTPServer != nil {
		if err := n.HTTPServer.Stop(); err != nil {
			t.Logf("Warning: error stopping HTTP server %s:%d: %v", n.ClusterID, n.ID, err)
		}
	}

	n.running = false
	t.Logf("✗ Stopped node %s:%d", n.ClusterID, n.ID)
	return nil
}

func (n *TestNode) IsLeader() bool {
	return n.RaftNode.IsLeader()
}

// setupTestNode создает и настраивает тестовую ноду
func setupTestNode(t *testing.T, clusterID string, nodeID uint64, port int, peers []config.RaftPeerConfig) (*TestNode, error) {
	// Создаем временную директорию для данных
	dataDir := filepath.Join(os.TempDir(), fmt.Sprintf("lsmdb-test-%s-node%d-%d", clusterID, nodeID, time.Now().UnixNano()))
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}

	// Создаем конфигурацию
	cfg := config.Default()
	cfg.Persistence.RootPath = dataDir
	cfg.Server.Port = port
	cfg.Raft.ID = nodeID
	cfg.Raft.ElectionTick = 10
	cfg.Raft.HeartbeatTick = 1
	cfg.Raft.MaxInflightMsgs = 256
	cfg.Raft.MaxSizePerMsg = 1048576
	cfg.Raft.MaxCommittedSizePerReady = 1048576
	cfg.Raft.MaxUncommittedEntriesSize = 1048576
	cfg.Raft.CheckQuorum = true
	cfg.Raft.PreVote = true
	cfg.Raft.Peers = peers

	// Создаем WAL
	journal, err := wal.New(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	// Создаем Store
	db, err := store.New(&cfg, journal)
	if err != nil {
		journal.Close()
		return nil, fmt.Errorf("failed to create store: %w", err)
	}

	// Создаем Raft node
	raftNode, err := raftadapter.NewNode(&cfg.Raft, db)
	if err != nil {
		journal.Close()
		return nil, fmt.Errorf("failed to create raft node: %w", err)
	}

	// Создаем HTTP сервер
	httpServer := http.NewServer(raftNode, fmt.Sprintf("%d", port))
	httpServer.SetStore(db)

	node := &TestNode{
		ID:         nodeID,
		ClusterID:  clusterID,
		Port:       port,
		DataDir:    dataDir,
		Store:      db,
		RaftNode:   raftNode,
		HTTPServer: httpServer,
	}

	return node, nil
}

// setupTestCluster создает тестовый кластер с 3 нодами
func setupTestCluster(t *testing.T, clusterID string, basePort int) (*TestCluster, error) {
	// Конфигурация пиров для кластера
	peers := []config.RaftPeerConfig{
		{ID: 1, Address: fmt.Sprintf("http://localhost:%d", basePort)},
		{ID: 2, Address: fmt.Sprintf("http://localhost:%d", basePort+1)},
		{ID: 3, Address: fmt.Sprintf("http://localhost:%d", basePort+2)},
	}

	nodes := make([]*TestNode, 3)
	for i := 0; i < 3; i++ {
		node, err := setupTestNode(t, clusterID, uint64(i+1), basePort+i, peers)
		if err != nil {
			// Cleanup созданных нод при ошибке
			for j := 0; j < i; j++ {
				os.RemoveAll(nodes[j].DataDir)
			}
			return nil, fmt.Errorf("failed to setup node %d: %w", i+1, err)
		}
		nodes[i] = node
	}

	cluster := &TestCluster{
		ID:    clusterID,
		Nodes: nodes,
	}

	return cluster, nil
}

// startCluster запускает все ноды кластера
func (c *TestCluster) Start(t *testing.T) error {
	for _, node := range c.Nodes {
		if err := node.Start(t); err != nil {
			return fmt.Errorf("failed to start node %d: %w", node.ID, err)
		}
	}
	return nil
}

// stopCluster останавливает все ноды кластера
func (c *TestCluster) Stop(t *testing.T) {
	for _, node := range c.Nodes {
		node.Stop(t)
	}
}

// cleanup удаляет все данные кластера
func (c *TestCluster) Cleanup(t *testing.T) {
	for _, node := range c.Nodes {
		if err := os.RemoveAll(node.DataDir); err != nil {
			t.Logf("Warning: failed to cleanup %s: %v", node.DataDir, err)
		}
	}
}

// waitForLeader ждет появления лидера в кластере
func (c *TestCluster) waitForLeader(t *testing.T, timeout time.Duration) (*TestNode, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, node := range c.Nodes {
			if node.running && node.IsLeader() {
				t.Logf(" Leader elected in cluster %s: node %d", c.ID, node.ID)
				return node, nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil, fmt.Errorf("no leader elected in cluster %s within timeout", c.ID)
}

// getLeader возвращает текущего лидера кластера
func (c *TestCluster) getLeader() *TestNode {
	for _, node := range c.Nodes {
		if node.running && node.IsLeader() {
			return node
		}
	}
	return nil
}

func TestShardedRaftIntegration(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelWarn)

	t.Log("=== Starting Sharded Raft Integration Test ===")

	// 1. Создаем два кластера
	t.Log("\n[STEP 1] Creating two Raft clusters...")
	cluster1, err := setupTestCluster(t, "cluster1", cluster1Port1)
	if err != nil {
		t.Fatalf("Failed to setup cluster1: %v", err)
	}
	defer cluster1.Cleanup(t)

	cluster2, err := setupTestCluster(t, "cluster2", cluster2Port1)
	if err != nil {
		t.Fatalf("Failed to setup cluster2: %v", err)
	}
	defer cluster2.Cleanup(t)

	// 2. Запускаем кластеры
	t.Log("\n[STEP 2] Starting clusters...")
	if err := cluster1.Start(t); err != nil {
		t.Fatalf("Failed to start cluster1: %v", err)
	}
	defer cluster1.Stop(t)

	if err := cluster2.Start(t); err != nil {
		t.Fatalf("Failed to start cluster2: %v", err)
	}
	defer cluster2.Stop(t)

	// ждем выбора лидеров
	t.Log("\n[STEP 3] Waiting for leader election...")
	leader1, err := cluster1.waitForLeader(t, 10*time.Second)
	if err != nil {
		t.Fatalf("Failed to elect leader in cluster1: %v", err)
	}

	_, err = cluster2.waitForLeader(t, 10*time.Second)
	if err != nil {
		t.Fatalf("Failed to elect leader in cluster2: %v", err)
	}

	// создаем ShardedRaftDB для каждой ноды
	t.Log("\n[STEP 4] Setting up sharded databases...")
	setupShardedDB := func(node *TestNode, allClusters []*TestCluster) {
		addr := fmt.Sprintf("http://localhost:%d", node.Port)
		node.ShardedDB = cluster.NewShardedRaftDB(
			addr,
			node.RaftNode,
			node.Store,
			func(targetAddr string) (cluster.Remote, error) {
				return cluster.NewHTTPClient(targetAddr), nil
			},
		)

		// регистрируем оба кластера
		for _, cluster := range allClusters {
			leaderAddr := ""
			replicas := []string{}
			for _, n := range cluster.Nodes {
				replicas = append(replicas, fmt.Sprintf("http://localhost:%d", n.Port))
				if n.IsLeader() {
					leaderAddr = fmt.Sprintf("http://localhost:%d", n.Port)
				}
			}
			if leaderAddr == "" {
				leaderAddr = replicas[0] // fallback
			}
			node.ShardedDB.AddCluster(cluster.ID, leaderAddr, replicas)
		}
	}

	allClusters := []*TestCluster{cluster1, cluster2}
	for _, cluster := range allClusters {
		for _, node := range cluster.Nodes {
			setupShardedDB(node, allClusters)
		}
	}

	// добавляем случайные данные и проверяем шардирование
	t.Log("\n[STEP 5] Writing random data and verifying sharding...")
	testData := make(map[string]string)
	cluster1Keys := 0
	cluster2Keys := 0

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key:%d", i)
		value := fmt.Sprintf("value:%d", rand.Intn(10000))
		testData[key] = value

		// определяем, в какой кластер попадет ключ
		targetClusterID, _ := leader1.ShardedDB.GetClusterForKey(key)
		if targetClusterID == "cluster1" {
			cluster1Keys++
		} else {
			cluster2Keys++
		}

		// пишем через лидера первого кластера (ShardedDB сам решит, куда отправить)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := leader1.ShardedDB.Put(ctx, key, value); err != nil {
			cancel()
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
		cancel()
	}

	t.Logf("Written 100 keys: %d to cluster1, %d to cluster2", cluster1Keys, cluster2Keys)

	// проверяем, что данные действительно распределились
	if cluster1Keys == 0 || cluster2Keys == 0 {
		t.Errorf("Poor sharding: cluster1=%d, cluster2=%d", cluster1Keys, cluster2Keys)
	}

	// даем время на репликацию
	time.Sleep(2 * time.Second)

	// проверяем, что данные доступны через любую ноду
	t.Log("\n[STEP 6] Verifying data availability from different nodes...")
	verifyCount := 0
	for key, expectedValue := range testData {
		// проверяем через случайную ноду из случайного кластера
		randomCluster := allClusters[rand.Intn(len(allClusters))]
		randomNode := randomCluster.Nodes[rand.Intn(len(randomCluster.Nodes))]

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		value, found, err := randomNode.ShardedDB.Get(ctx, key)
		cancel()

		if err != nil {
			t.Errorf("Failed to get key %s from node %s:%d: %v", key, randomNode.ClusterID, randomNode.ID, err)
			continue
		}

		if !found {
			t.Errorf("Key %s not found on node %s:%d", key, randomNode.ClusterID, randomNode.ID)
			continue
		}

		if value != expectedValue {
			t.Errorf("Key %s: expected %s, got %s", key, expectedValue, value)
			continue
		}

		verifyCount++
	}

	t.Logf(" Verified %d/%d keys successfully", verifyCount, len(testData))

	// останавливаем мастер ноду в первом кластере
	t.Log("\n[STEP 7] Stopping leader node in cluster1...")
	oldLeaderID := leader1.ID
	if err := leader1.Stop(t); err != nil {
		t.Fatalf("Failed to stop leader: %v", err)
	}

	// ждем перевыбора нового лидера
	t.Log("\n[STEP 8] Waiting for new leader election in cluster1...")
	time.Sleep(5 * time.Second) // Даем время на выборы

	newLeader1, err := cluster1.waitForLeader(t, 15*time.Second)
	if err != nil {
		t.Fatalf("Failed to elect new leader in cluster1: %v", err)
	}

	if newLeader1.ID == oldLeaderID {
		t.Errorf("New leader has same ID as old leader: %d", oldLeaderID)
	}

	t.Logf("New leader elected in cluster1: node %d (was node %d)", newLeader1.ID, oldLeaderID)

	// обновляем информацию о новом лидере в ShardedDB
	t.Log("\n[STEP 9] Updating cluster topology with new leader...")
	newLeaderAddr := fmt.Sprintf("http://localhost:%d", newLeader1.Port)
	for _, cluster := range allClusters {
		for _, node := range cluster.Nodes {
			if node.running {
				node.ShardedDB.UpdateClusterLeader("cluster1", newLeaderAddr)
			}
		}
	}

	// добавляем новые данные после смены лидера
	t.Log("\n[STEP 10] Writing new data after leader change...")
	newTestData := make(map[string]string)
	for i := 100; i < 150; i++ {
		key := fmt.Sprintf("key:%d", i)
		value := fmt.Sprintf("new-value:%d", rand.Intn(10000))
		newTestData[key] = value

		time.Sleep(time.Millisecond * 100)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := newLeader1.ShardedDB.Put(ctx, key, value); err != nil {
			cancel()
			t.Logf("Warning: Failed to put key %s after failover: %v", key, err)
			continue
		}
		cancel()
	}

	t.Logf(" Written %d new keys after failover", len(newTestData))

	// Даем время на репликацию
	time.Sleep(2 * time.Second)

	// 11. Проверяем наличие старых и новых данных на новом лидере
	t.Log("\n[STEP 11] Verifying old and new data on new leader...")

	// Проверяем старые данные (которые должны были быть зареплицированы до падения)
	oldDataVerified := 0
	for key, expectedValue := range testData {
		// Проверяем только ключи, которые принадлежат cluster1
		targetClusterID, _ := newLeader1.ShardedDB.GetClusterForKey(key)
		if targetClusterID != "cluster1" {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		value, found, err := newLeader1.ShardedDB.Get(ctx, key)
		cancel()

		if err != nil {
			t.Errorf("Failed to get old key %s from new leader: %v", key, err)
			continue
		}

		if !found {
			t.Errorf("Old key %s not found on new leader", key)
			continue
		}

		if value != expectedValue {
			t.Errorf("Old key %s: expected %s, got %s", key, expectedValue, value)
			continue
		}

		oldDataVerified++
	}

	t.Logf("Verified %d old keys on new leader", oldDataVerified)

	// Проверяем новые данные
	newDataVerified := 0
	for key, expectedValue := range newTestData {
		targetClusterID, _ := newLeader1.ShardedDB.GetClusterForKey(key)
		if targetClusterID != "cluster1" {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		value, found, err := newLeader1.ShardedDB.Get(ctx, key)
		cancel()

		if err != nil {
			t.Errorf("Failed to get new key %s from new leader: %v", key, err)
			continue
		}

		if !found {
			t.Errorf("New key %s not found on new leader", key)
			continue
		}

		if value != expectedValue {
			t.Errorf("New key %s: expected %s, got %s", key, expectedValue, value)
			continue
		}

		newDataVerified++
	}

	t.Logf(" Verified %d new keys on new leader", newDataVerified)

	// 12. Финальные проверки
	t.Log("\n[STEP 12] Final verification...")

	if oldDataVerified == 0 {
		t.Error("No old data found on new leader - replication failed")
	}

	if newDataVerified == 0 {
		t.Error("No new data found on new leader - writes after failover failed")
	}

	// Проверяем работоспособность обоих кластеров
	if cluster2.getLeader() == nil {
		t.Error("Cluster2 lost its leader")
	}

	t.Log("\n=== Integration Test Completed Successfully ===")
	t.Logf("Summary:")
	t.Logf("  - Initial data: %d keys (%d in cluster1, %d in cluster2)",
		len(testData), cluster1Keys, cluster2Keys)
	t.Logf("  - Old data verified on new leader: %d keys", oldDataVerified)
	t.Logf("  - New data after failover: %d keys verified", newDataVerified)
	t.Logf("  - Leader failover: successful (old=%d, new=%d)", oldLeaderID, newLeader1.ID)
}
