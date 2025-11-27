package cluster

import (
	"fmt"
	"sync"
	"testing"
)

// ====== фейки для локального KV и удалённых клиентов ======

type fakeKV struct {
	mu   sync.Mutex
	data map[string]string
	puts int
	gets int
	dels int
}

func newFakeKV() *fakeKV {
	return &fakeKV{data: make(map[string]string)}
}

func (f *fakeKV) PutString(k, v string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[k] = v
	f.puts++
	return nil
}

func (f *fakeKV) GetString(k string) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	v, ok := f.data[k]
	return v, ok, nil
}

func (f *fakeKV) Delete(k string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.data, k)
	f.dels++
	return nil
}

// fakeRemote имитирует удалённый Remote-клиент
type fakeRemote struct {
	mu    sync.Mutex
	store map[string]string
	puts  int
	gets  int
	dels  int
}

func newFakeRemote() *fakeRemote {
	return &fakeRemote{store: make(map[string]string)}
}

func (r *fakeRemote) PutString(k, v string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.store[k] = v
	r.puts++
	return nil
}

func (r *fakeRemote) GetString(k string) (string, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.store[k]
	r.gets++
	return v, ok, nil
}

func (r *fakeRemote) Delete(k string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.store, k)
	r.dels++
	return nil
}

type scriptedRemote struct {
	*fakeRemote
	fail  bool
	calls struct {
		get int
		put int
		del int
	}
}

func (r *scriptedRemote) PutString(k, v string) error {
	r.calls.put++
	if r.fail {
		return fmt.Errorf("remote unavailable")
	}
	return r.fakeRemote.PutString(k, v)
}

func (r *scriptedRemote) GetString(k string) (string, bool, error) {
	r.calls.get++
	if r.fail {
		return "", false, fmt.Errorf("remote unavailable")
	}
	return r.fakeRemote.GetString(k)
}

func (r *scriptedRemote) Delete(k string) error {
	r.calls.del++
	if r.fail {
		return fmt.Errorf("remote unavailable")
	}
	return r.fakeRemote.Delete(k)
}

// findKeyForOwner — подбирает ключ, который по Ring.GetNode попадает на нужную ноду
func findKeyForOwner(r *HashRing, owner string) string {
	for i := 0; i < 1_000_000; i++ {
		k := fmt.Sprintf("k-%d", i)
		if n, ok := r.GetNode(k); ok && n == owner {
			return k
		}
	}
	return ""
}

func TestRouter_RoutesLocalAndRemote(t *testing.T) {
	// 1. Строим кольцо из трёх нод
	ring := NewHashRing(128)
	nodes := []string{"node1:8080", "node2:8080", "node3:8080"}
	for _, n := range nodes {
		ring.AddNode(n)
	}

	local := "node1:8080"

	localKey := findKeyForOwner(ring, local)
	if localKey == "" {
		t.Fatal("failed to find key for local node")
	}

	node2Key := findKeyForOwner(ring, "node2:8080")
	if node2Key == "" {
		t.Fatal("failed to find key for node2")
	}

	node3Key := findKeyForOwner(ring, "node3:8080")
	if node3Key == "" {
		t.Fatal("failed to find key for node3")
	}

	routerLocal := &Router{
		LocalAddr: local,
		Ring:      ring,
	}
	for expectedNode, key := range map[string]string{
		local:        localKey,
		"node2:8080": node2Key,
		"node3:8080": node3Key,
	} {
		got, err := routerLocal.owner(key)
		if err != nil {
			t.Fatalf("owner(%q) error: %v", key, err)
		}
		if got != expectedNode {
			t.Fatalf("owner(%q) = %s, want %s", key, got, expectedNode)
		}
	}

	// локальное KV + фабрика Remote-клиентов
	localKV := newFakeKV()
	remotes := map[string]*fakeRemote{}

	factory := func(target string) (Remote, error) {
		if r, ok := remotes[target]; ok {
			return r, nil
		}
		r := newFakeRemote()
		remotes[target] = r
		return r, nil
	}

	router := &Router{
		LocalAddr: local,
		Ring:      ring,
		DB:        localKV,
		NewClient: factory,
		// этот тест проверяет базовый routing без репликации
		ReplicationFactor: 1,
	}

	// один ключ — локально, два — на чужие ноды
	if err := router.PutString(localKey, "L"); err != nil {
		t.Fatalf("PutString(localKey) error: %v", err)
	}
	if err := router.PutString(node2Key, "V2"); err != nil {
		t.Fatalf("PutString(node2Key) error: %v", err)
	}
	if err := router.PutString(node3Key, "V3"); err != nil {
		t.Fatalf("PutString(node3Key) error: %v", err)
	}

	// локальный key ушёл в локальное KV
	if v, ok, err := router.GetString(localKey); err != nil || !ok || v != "L" {
		t.Fatalf("GetString(localKey) = %q, %v, %v; want L, true, nil", v, ok, err)
	}
	if localKV.puts != 1 || localKV.gets == 0 {
		t.Fatalf("localKV counters: puts=%d gets=%d, want puts=1 & gets>0", localKV.puts, localKV.gets)
	}

	// remote ключ для node2 ушёл через remote-клиента node2:8080
	r2 := remotes["node2:8080"]
	if r2 == nil {
		t.Fatalf("no remote client created for node2:8080")
	}
	if v, ok, err := router.GetString(node2Key); err != nil || !ok || v != "V2" {
		t.Fatalf("GetString(node2Key) = %q, %v, %v; want V2, true, nil", v, ok, err)
	}
	if r2.puts != 1 || r2.gets == 0 {
		t.Fatalf("remote(node2) counters: puts=%d gets=%d, want puts=1 & gets>0", r2.puts, r2.gets)
	}

	// remote-клиента для node3
	r3 := remotes["node3:8080"]
	if r3 == nil {
		t.Fatalf("no remote client created for node3:8080")
	}
	if v, ok, err := router.GetString(node3Key); err != nil || !ok || v != "V3" {
		t.Fatalf("GetString(node3Key) = %q, %v, %v; want V3, true, nil", v, ok, err)
	}
	if r3.puts != 1 || r3.gets == 0 {
		t.Fatalf("remote(node3) counters: puts=%d gets=%d, want puts=1 & gets>0", r3.puts, r3.gets)
	}

	// Delete и что он идёт туда же, куда и запись
	if err := router.Delete(localKey); err != nil {
		t.Fatalf("Delete(localKey) error: %v", err)
	}
	if _, ok, _ := router.GetString(localKey); ok {
		t.Fatalf("localKey still exists after delete")
	}

	if err := router.Delete(node2Key); err != nil {
		t.Fatalf("Delete(node2Key) error: %v", err)
	}
	if _, ok, _ := router.GetString(node2Key); ok {
		t.Fatalf("node2Key still exists after delete")
	}

	if err := router.Delete(node3Key); err != nil {
		t.Fatalf("Delete(node3Key) error: %v", err)
	}
	if _, ok, _ := router.GetString(node3Key); ok {
		t.Fatalf("node3Key still exists after delete")
	}
}

func TestRouter_ReplicatedGetFallback(t *testing.T) {
	ring := NewHashRing(128)
	nodes := []string{"node1:8080", "node2:8080", "node3:8080"}
	for _, n := range nodes {
		ring.AddNode(n)
	}

	key := findKeyForOwner(ring, "node2:8080")
	if key == "" {
		t.Fatal("failed to pick key for node2")
	}

	localKV := newFakeKV()
	remotes := map[string]*scriptedRemote{
		"node2:8080": {fakeRemote: newFakeRemote(), fail: true},
		"node3:8080": {fakeRemote: newFakeRemote(), fail: false},
	}
	remotes["node3:8080"].store[key] = "value"

	factory := func(target string) (Remote, error) {
		if r, ok := remotes[target]; ok {
			return r, nil
		}
		return nil, fmt.Errorf("unexpected target %s", target)
	}

	router := &Router{
		LocalAddr:         "node1:8080",
		Ring:              ring,
		DB:                localKV,
		NewClient:         factory,
		ReplicationFactor: 2,
	}

	value, ok, err := router.GetString(key)
	if err != nil {
		t.Fatalf("GetString returned error: %v", err)
	}
	if !ok {
		t.Fatalf("expected key to be found via fallback")
	}
	if value != "value" {
		t.Fatalf("value mismatch: got %q", value)
	}

	if remotes["node2:8080"].calls.get == 0 {
		t.Fatalf("expected router to attempt node2 before fallback")
	}
	if remotes["node3:8080"].calls.get == 0 {
		t.Fatalf("expected fallback remote to be queried")
	}
}
