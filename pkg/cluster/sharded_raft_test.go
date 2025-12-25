package cluster

import (
	"context"
	"fmt"
	"testing"
)

// Mock Store для тестирования
type mockStore struct {
	data map[string]string
}

func newMockStore() *mockStore {
	return &mockStore{
		data: make(map[string]string),
	}
}

func (m *mockStore) PutString(key, value string) error {
	m.data[key] = value
	return nil
}

func (m *mockStore) GetString(key string) (string, bool, error) {
	val, ok := m.data[key]
	return val, ok, nil
}

func (m *mockStore) Delete(key string) error {
	delete(m.data, key)
	return nil
}

// Mock Remote для тестирования
type mockRemote struct {
	store *mockStore
}

func (m *mockRemote) PutString(key, value string) error {
	return m.store.PutString(key, value)
}

func (m *mockRemote) GetString(key string) (string, bool, error) {
	return m.store.GetString(key)
}

func (m *mockRemote) Delete(key string) error {
	return m.store.Delete(key)
}

func TestShardedRaftDB_LocalCluster(t *testing.T) {
	store := newMockStore()

	// Создаем ShardedRaftDB без реального Raft (nil), только для тестирования маршрутизации
	db := NewShardedRaftDB(
		"http://localhost:8080",
		nil, // Без реального Raft для простоты
		store,
		func(addr string) (Remote, error) {
			return &mockRemote{store: newMockStore()}, nil
		},
	)

	// Добавляем только локальный кластер
	db.AddCluster("cluster1", "http://localhost:8080", []string{
		"http://localhost:8080",
		"http://localhost:8081",
	})

	ctx := context.Background()

	// Тест: определение локального кластера
	cluster, err := db.getClusterForKey("test_key")
	if err != nil {
		t.Fatalf("Failed to get cluster: %v", err)
	}

	if cluster.ID != "cluster1" {
		t.Errorf("Expected cluster1, got %s", cluster.ID)
	}

	if !db.isLocalCluster(cluster) {
		t.Error("Expected local cluster")
	}

	// Тест: чтение из локального хранилища
	store.PutString("local_key", "local_value")

	value, found, err := db.Get(ctx, "local_key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !found {
		t.Error("Expected to find key")
	}

	if value != "local_value" {
		t.Errorf("Expected 'local_value', got '%s'", value)
	}
}

func TestShardedRaftDB_MultipleClusterss(t *testing.T) {
	store1 := newMockStore()
	store2 := newMockStore()

	// Создаем ShardedRaftDB для первого кластера
	db1 := NewShardedRaftDB(
		"http://localhost:8080",
		nil,
		store1,
		func(addr string) (Remote, error) {
			// Имитируем удаленное подключение ко второму кластеру
			if addr == "http://localhost:9080" {
				return &mockRemote{store: store2}, nil
			}
			return &mockRemote{store: store1}, nil
		},
	)

	// Добавляем два кластера
	db1.AddCluster("cluster1", "http://localhost:8080", []string{
		"http://localhost:8080",
		"http://localhost:8081",
	})

	db1.AddCluster("cluster2", "http://localhost:9080", []string{
		"http://localhost:9080",
		"http://localhost:9081",
	})

	// Проверяем список кластеров
	clusters := db1.ListClusters()
	if len(clusters) != 2 {
		t.Errorf("Expected 2 clusters, got %d", len(clusters))
	}

	// Проверяем, что consistent hashing распределяет ключи
	key1 := "user:1"
	key2 := "user:2"

	cluster1, _ := db1.getClusterForKey(key1)
	cluster2, _ := db1.getClusterForKey(key2)

	// Ключи могут попасть в разные кластеры (хотя не гарантировано в тесте)
	t.Logf("key1='%s' -> cluster='%s'", key1, cluster1.ID)
	t.Logf("key2='%s' -> cluster='%s'", key2, cluster2.ID)
}

func TestShardedRaftDB_AddRemoveCluster(t *testing.T) {
	store := newMockStore()

	db := NewShardedRaftDB(
		"http://localhost:8080",
		nil,
		store,
		func(addr string) (Remote, error) {
			return &mockRemote{store: newMockStore()}, nil
		},
	)

	// Добавляем кластер
	db.AddCluster("cluster1", "http://localhost:8080", []string{
		"http://localhost:8080",
	})

	if len(db.ListClusters()) != 1 {
		t.Error("Expected 1 cluster")
	}

	// Удаляем кластер
	db.RemoveCluster("cluster1")

	if len(db.ListClusters()) != 0 {
		t.Error("Expected 0 clusters")
	}
}

func TestShardedRaftDB_UpdateLeader(t *testing.T) {
	store := newMockStore()

	db := NewShardedRaftDB(
		"http://localhost:8080",
		nil,
		store,
		func(addr string) (Remote, error) {
			return &mockRemote{store: newMockStore()}, nil
		},
	)

	db.AddCluster("cluster1", "http://localhost:8080", []string{
		"http://localhost:8080",
		"http://localhost:8081",
	})

	// Обновляем leader
	db.UpdateClusterLeader("cluster1", "http://localhost:8081")

	db.clustersMu.RLock()
	cluster := db.clusters["cluster1"]
	db.clustersMu.RUnlock()

	if cluster.LeaderAddr != "http://localhost:8081" {
		t.Errorf("Expected leader http://localhost:8081, got %s", cluster.LeaderAddr)
	}
}

func TestHashRing_ConsistentHashing(t *testing.T) {
	ring := NewHashRing(150)

	// Добавляем кластеры
	ring.AddNode("cluster1")
	ring.AddNode("cluster2")
	ring.AddNode("cluster3")

	// Проверяем, что один и тот же ключ всегда попадает в один кластер
	key := "user:12345"

	cluster1, _ := ring.GetNode(key)
	cluster2, _ := ring.GetNode(key)
	cluster3, _ := ring.GetNode(key)

	if cluster1 != cluster2 || cluster2 != cluster3 {
		t.Error("Same key should map to same cluster")
	}

	// Проверяем распределение множества ключей
	distribution := make(map[string]int)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key:%d", i)
		cluster, _ := ring.GetNode(key)
		distribution[cluster]++
	}

	t.Logf("Distribution: %v", distribution)

	// Каждый кластер должен получить примерно треть ключей (±20%)
	for cluster, count := range distribution {
		if count < 250 || count > 450 {
			t.Logf("Warning: cluster %s has unbalanced distribution: %d keys", cluster, count)
		}
	}
}

func TestHashRing_AddRemoveNode(t *testing.T) {
	ring := NewHashRing(100)

	ring.AddNode("node1")
	ring.AddNode("node2")

	// Запоминаем распределение ключей
	keysOnNode1 := 0
	keysOnNode2 := 0

	keys := make([]string, 100)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key:%d", i)
		keys[i] = key
		node, _ := ring.GetNode(key)
		if node == "node1" {
			keysOnNode1++
		} else {
			keysOnNode2++
		}
	}

	t.Logf("Before removal: node1=%d, node2=%d", keysOnNode1, keysOnNode2)

	// Удаляем node1
	ring.RemoveNode("node1")

	// Все ключи теперь должны быть на node2
	for _, key := range keys {
		node, ok := ring.GetNode(key)
		if !ok {
			t.Error("Failed to get node for key")
		}
		if node != "node2" {
			t.Errorf("Expected node2, got %s", node)
		}
	}
}
