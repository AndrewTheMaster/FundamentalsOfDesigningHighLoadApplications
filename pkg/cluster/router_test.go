package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

type fakeKV struct {
	mu   sync.Mutex
	data map[string]string
	puts int
	gets int
	dels int
}

func newFakeKV() *fakeKV {
	return &fakeKV{data: map[string]string{}}
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

type fakeRemote struct {
	target string
	puts   int
	gets   int
	dels   int
	store  map[string]string
}

func newFakeRemote(target string) *fakeRemote {
	return &fakeRemote{target: target, store: map[string]string{}}
}
func (r *fakeRemote) Put(ctx context.Context, k, v string) error {
	r.store[k] = v
	r.puts++
	return nil
}
func (r *fakeRemote) Get(ctx context.Context, k string) (string, bool, error) {
	v, ok := r.store[k]
	r.gets++
	return v, ok, nil
}
func (r *fakeRemote) Delete(ctx context.Context, k string) error {
	delete(r.store, k)
	r.dels++
	return nil
}
func (r *fakeRemote) Close() error {
	return nil
}

func findKeyForOwner(r *HashRing, owner string) string {
	for i := 0; i < 1_000_000; i++ {
		k := fmt.Sprintf("k-%d", i)
		if n, ok := r.GetNode(k); ok && n == owner {
			return k
		}
	}
	return ""
}

func TestRouter_LocalVsRemoteRouting(t *testing.T) {
	// кольцо из 3 нод
	ring := NewHashRing(128)
	nodes := []string{"node1:8080", "node2:8080", "node3:8080"}
	for _, n := range nodes {
		ring.AddNode(n)
	}

	// текущая нода — node1
	local := "node1:8080"

	localKV := newFakeKV()
	remotes := map[string]*fakeRemote{}
	factory := func(target string) (Remote, error) {
		if fr, ok := remotes[target]; ok {
			return fr, nil
		}
		fr := newFakeRemote(target)
		remotes[target] = fr
		return fr, nil
	}

	router := &Router{
		LocalAddr: local,
		Ring:      ring,
		DB:        localKV,
		NewClient: factory,
	}

	ctx := context.Background()
    // ключ, чей владелец локальный
	localKey := findKeyForOwner(ring, local)
	if localKey == "" {
		t.Fatal("failed to find local key")
	}
    // ключ, который уводит на node2
	remoteKey := findKeyForOwner(ring, "node2:8080")
	if remoteKey == "" {
		t.Fatal("failed to find remote key for node2")
	}
	// --- local ---
	if err := router.Put(ctx, localKey, "L"); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := router.Get(ctx, localKey); err != nil || !ok || v != "L" {
		t.Fatalf("local get mismatch: v=%q ok=%v err=%v", v, ok, err)
	}
	if err := router.Delete(ctx, localKey); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := router.Get(ctx, localKey); ok {
		t.Fatalf("local key still exists after delete")
	}
	if localKV.puts == 0 || localKV.gets == 0 || localKV.dels == 0 {
		t.Fatalf("local KV counters not incremented: puts=%d gets=%d dels=%d", localKV.puts, localKV.gets, localKV.dels)
	}
	// --- remote (node2) ---
	if err := router.Put(ctx, remoteKey, "R"); err != nil {
		t.Fatal(err)
	}
	v, ok, err := router.Get(ctx, remoteKey)
	if err != nil || !ok || v != "R" {
		t.Fatalf("remote get mismatch: v=%q ok=%v err=%v", v, ok, err)
	}
	if err := router.Delete(ctx, remoteKey); err != nil {
		t.Fatal(err)
	}

	fr := remotes["node2:8080"]
	if fr == nil || fr.puts == 0 || fr.gets == 0 || fr.dels == 0 {
		t.Fatalf("remote client for node2 not used as expected")
	}
}
