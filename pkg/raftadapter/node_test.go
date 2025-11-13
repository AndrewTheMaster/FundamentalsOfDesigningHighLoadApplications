//nolint:hugeParam // test only
package raftadapter

import (
	"sync"
	"testing"

	"lsmdb/pkg/config"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// mockStore реализует минимальный iStoreAPI для теста
type mockStore struct{}

func (m *mockStore) PutString(key, value string) error          { _ = key; _ = value; return nil }
func (m *mockStore) GetString(key string) (string, bool, error) { _ = key; return "", false, nil }
func (m *mockStore) Delete(key string) error                    { _ = key; return nil }

// mockTransport реализует iTransport и собирает вызовы
type mockTransport struct {
	mu       sync.Mutex
	addCalls []struct {
		id   uint64
		addr string
	}
	removeCalls []uint64
	updateCalls []struct {
		id   uint64
		addr string
	}
	sentMsgs []raftpb.Message
}

func (m *mockTransport) Send(msg raftpb.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sentMsgs = append(m.sentMsgs, msg)
	return nil
}

func (m *mockTransport) AddPeer(id uint64, addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addCalls = append(m.addCalls, struct {
		id   uint64
		addr string
	}{id: id, addr: addr})
}

func (m *mockTransport) RemovePeer(id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeCalls = append(m.removeCalls, id)
}

func (m *mockTransport) UpdatePeer(id uint64, addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.updateCalls = append(m.updateCalls, struct {
		id   uint64
		addr string
	}{id: id, addr: addr})
}

func TestNode_UpdateTransport(t *testing.T) {
	cfg := &config.RaftConfig{
		ID:                        1,
		ElectionTick:              10,
		HeartbeatTick:             2,
		MaxSizePerMsg:             1024,
		MaxCommittedSizePerReady:  4096,
		MaxUncommittedEntriesSize: 8192,
		MaxInflightMsgs:           256,
		CheckQuorum:               true,
		PreVote:                   false,
		Peers:                     []config.RaftPeerConfig{{ID: 1, Address: "http://127.0.0.1:8080"}},
	}

	n, err := NewNode(cfg, &mockStore{})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	// Заменим транспорт на мок
	mt := &mockTransport{}
	n.transport = mt

	// Добавим новый пир (id=2)
	ccAdd := raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: 2, Context: []byte("http://127.0.0.1:8081")}
	n.updateTransport(ccAdd)

	// Проверяем, что транспорт получил вызов AddPeer и что пир добавлен в карту
	if len(mt.addCalls) != 1 {
		t.Fatalf("expected 1 add call, got %d", len(mt.addCalls))
	}
	if mt.addCalls[0].id != 2 || mt.addCalls[0].addr != "http://127.0.0.1:8081" {
		t.Fatalf("unexpected add call data: %#v", mt.addCalls[0])
	}
	if addr, ok := n.Peers[2]; !ok || addr != "http://127.0.0.1:8081" {
		t.Fatalf("peer not added to node.Peers or wrong addr: %v, ok=%v", addr, ok)
	}

	// Обновим адрес пира (id=2)
	ccUpdate := raftpb.ConfChange{Type: raftpb.ConfChangeUpdateNode, NodeID: 2, Context: []byte("http://127.0.0.1:9000")}
	n.updateTransport(ccUpdate)

	if len(mt.updateCalls) != 1 {
		t.Fatalf("expected 1 update call, got %d", len(mt.updateCalls))
	}
	if mt.updateCalls[0].id != 2 || mt.updateCalls[0].addr != "http://127.0.0.1:9000" {
		t.Fatalf("unexpected update call data: %#v", mt.updateCalls[0])
	}
	if addr, ok := n.Peers[2]; !ok || addr != "http://127.0.0.1:9000" {
		t.Fatalf("peer not updated in node.Peers or wrong addr: %v, ok=%v", addr, ok)
	}

	// Удалим пир (id=2)
	ccRemove := raftpb.ConfChange{Type: raftpb.ConfChangeRemoveNode, NodeID: 2}
	n.updateTransport(ccRemove)

	if len(mt.removeCalls) != 1 {
		t.Fatalf("expected 1 remove call, got %d", len(mt.removeCalls))
	}
	if mt.removeCalls[0] != 2 {
		t.Fatalf("unexpected remove call id: %d", mt.removeCalls[0])
	}
	if _, ok := n.Peers[2]; ok {
		t.Fatalf("peer still present after removal")
	}
}
