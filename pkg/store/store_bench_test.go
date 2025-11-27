package store

import (
	"fmt"
	"lsmdb/pkg/config"
	"lsmdb/pkg/wal"
	"math/rand"
	"testing"
)

func newTestStore(b testing.TB) (*Store, func()) {
	cfg := config.Default()
	cfg.Persistence.RootPath = b.TempDir()

	journal, err := wal.New(cfg.Persistence.RootPath)
	if err != nil {
		b.Fatalf("failed to create WAL: %v", err)
	}

	store, err := New(&cfg, journal)
	if err != nil {
		b.Fatalf("failed to create store: %v", err)
	}

	cleanup := func() {
		store.Close()
		if err := journal.Close(); err != nil {
			b.Fatalf("failed to close WAL: %v", err)
		}
	}

	return store, cleanup
}

func BenchmarkStoreWrite(b *testing.B) {
	store, cleanup := newTestStore(b)
	defer cleanup()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := store.PutString(fmt.Sprintf("key-%d", i), "value-"+fmt.Sprint(i)); err != nil {
			b.Fatalf("PutString failed: %v", err)
		}
	}
}

func BenchmarkStoreRead(b *testing.B) {
	store, cleanup := newTestStore(b)
	defer cleanup()

	const preloaded = 10_000
	for i := 0; i < preloaded; i++ {
		if err := store.PutString(fmt.Sprintf("key-%d", i), "value-"+fmt.Sprint(i)); err != nil {
			b.Fatalf("PutString failed: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	rng := rand.New(rand.NewSource(42))

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", rng.Intn(preloaded))
		if _, found, err := store.GetString(key); err != nil || !found {
			b.Fatalf("GetString failed: %v (found=%v)", err, found)
		}
	}
}
