//nolint:hugeParam // test only
package raftadapter

import (
	"context"
	"sync"
	"testing"
	"time"

	"lsmdb/pkg/config"
	"lsmdb/pkg/store"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// recordingStore — простой потокобезопасный mock для хранения ключ-значение
type recordingStore struct {
	mu sync.RWMutex
	m  map[string]string
}

func newRecordingStore() *recordingStore {
	return &recordingStore{m: make(map[string]string)}
}

func (s *recordingStore) PutString(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = value
	return nil
}

func (s *recordingStore) GetString(key string) (string, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[key]
	return v, ok, nil
}

func (s *recordingStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key)
	return nil
}

// inprocTransport маршрутизирует raft сообщений между нодами в памяти
type inprocTransport struct {
	nodesMu sync.RWMutex
	nodes   map[uint64]*Node
}

func newInprocTransport() *inprocTransport {
	return &inprocTransport{nodes: make(map[uint64]*Node)}
}

func (t *inprocTransport) Send(msg raftpb.Message) error {
	// доставляем сообщение напрямую в получателя
	t.nodesMu.RLock()
	target, ok := t.nodes[msg.To]
	t.nodesMu.RUnlock()
	if !ok {
		return nil
	}
	// вызываем Handle в отдельной горутине, чтобы не блокировать отправителя
	go func() {
		_ = target.Handle(context.Background(), msg)
	}()
	return nil
}

func (t *inprocTransport) AddPeer(id uint64, addr string) {
	_ = id
	_ = addr
}
func (t *inprocTransport) RemovePeer(id uint64)              { _ = id }
func (t *inprocTransport) UpdatePeer(id uint64, addr string) { _ = id; _ = addr }

// helper: wait until exactly one leader among nodes or timeout
func waitForLeader(t *testing.T, nodes []*Node, timeout time.Duration) *Node {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*Node
		for _, n := range nodes {
			if n.IsLeader() {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		// small sleep
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("leader not elected within %s", timeout)
	return nil
}

func TestReplication_3Nodes(t *testing.T) {
	// создаём 3 ноды с записями стор
	stores := []*recordingStore{newRecordingStore(), newRecordingStore(), newRecordingStore()}
	cfg := func(id uint64) *config.RaftConfig {
		peers := []config.RaftPeerConfig{
			{ID: 1, Address: "n1"},
			{ID: 2, Address: "n2"},
			{ID: 3, Address: "n3"},
		}
		return &config.RaftConfig{
			ID:                        id,
			ElectionTick:              10,
			HeartbeatTick:             2,
			MaxSizePerMsg:             1024,
			MaxCommittedSizePerReady:  4096,
			MaxUncommittedEntriesSize: 8192,
			MaxInflightMsgs:           256,
			Peers:                     peers,
		}
	}

	nodes := make([]*Node, 3)
	transport := newInprocTransport()

	// создаём Node и подменяем транспорт
	for i := 0; i < 3; i++ {
		n, err := NewNode(cfg(uint64(i+1)), stores[i])
		if err != nil {
			t.Fatalf("failed to create node %d: %v", i+1, err)
		}
		// подменяем транспорт на общий inproc
		n.transport = transport
		nodes[i] = n
	}

	// регистрируем ноды в транспорте
	for _, n := range nodes {
		transport.nodesMu.Lock()
		transport.nodes[n.ID] = n
		transport.nodesMu.Unlock()
	}

	// запустим Run для каждой ноды
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(len(nodes))
	for _, n := range nodes {
		go func(node *Node) {
			defer wg.Done()
			_ = node.Run(ctx)
		}(n)
	}

	// подождём выбора лидера
	leader := waitForLeader(t, nodes, 5*time.Second)
	t.Logf("leader elected: %d", leader.ID)

	// предложим команду на лидере
	cmd := NewCmd(store.InsertOp, []byte("k"), []byte("v"))
	if err := leader.Execute(context.Background(), cmd); err != nil {
		t.Fatalf("leader Execute failed: %v", err)
	}

	// подождём, что все сторы получили запись
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, s := range stores {
			if _, ok, _ := s.GetString("k"); !ok {
				all = false
				break
			}
		}
		if all {
			// success
			for _, n := range nodes {
				_ = n.Stop()
			}
			wg.Wait()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	// таймаут
	for i, s := range stores {
		v, ok, _ := s.GetString("k")
		t.Logf("store %d has key? %v value=%s", i+1, ok, v)
	}
	for _, n := range nodes {
		_ = n.Stop()
	}
	wg.Wait()
	t.Fatalf("replication did not reach all nodes in time")
}
