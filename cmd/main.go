package main

import (
	"context"
	"fmt"
	"lsmdb/internal/config"
	"lsmdb/pkg/cluster"
	storecfg "lsmdb/pkg/config"
	"lsmdb/pkg/rpc"
	"lsmdb/pkg/store"
	"lsmdb/pkg/wal"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// timeProvider оставлен на будущее, сейчас нам не нужен для Store
type timeProvider struct{}

func (tp *timeProvider) Now() time.Time {
	return time.Now()
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// high-level конфиг ноды (storage, sharding, replication и т.п.)
	cfg := config.Default()

	fmt.Printf("LSMDB starting (Lab 5 Sharding + ZK). DataDir=%s\n", cfg.Storage.DataDir)

	localAddr := os.Getenv("LSMDB_NODE_ADDR")
	if localAddr == "" {
		fmt.Println("LSMDB_NODE_ADDR is not set")
		os.Exit(1)
	}

	zkServersEnv := os.Getenv("ZK_SERVERS")
	if zkServersEnv == "" {
		fmt.Println("ZK_SERVERS is not set")
		os.Exit(1)
	}
	zkServers := strings.Split(zkServersEnv, ",")

	// --- ZooKeeper membership ---
	membership, err := cluster.NewZKMembership(zkServers, "/lsmdb", localAddr)
	if err != nil {
		fmt.Printf("Failed to connect to ZooKeeper: %v\n", err)
		os.Exit(1)
	}
	defer membership.Close()

	if err := membership.RegisterSelf(); err != nil {
		fmt.Printf("Failed to register node in ZooKeeper: %v\n", err)
		os.Exit(1)
	}

	// первичная сборка кольца по нодам (HashRing over nodes)
	ring, err := membership.BuildRing(100)
	if err != nil {
		fmt.Printf("Failed to build ring from ZooKeeper: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Initial ring nodes:", ring.ListNodes())

	// --- WAL и конфиг стора ---
	journal, err := wal.New(cfg.Storage.WALDir)
	if err != nil {
		fmt.Printf("Failed to init WAL: %v\n", err)
		os.Exit(1)
	}

	// адаптируем high-level config к конфигу стора
	dbCfg := storecfg.Default()
	dbCfg.DB.Persistence.RootPath = cfg.Storage.DataDir

	// --- локальное LSM-хранилище ---
	db, err := store.New(&dbCfg, journal)
	if err != nil {
		panic(err)
	}

	// --- Router с кольцом по нодам ---
	router := &cluster.Router{
		LocalAddr: localAddr,
		Ring:      ring,
		DB:        db,
		NewClient: func(target string) (cluster.Remote, error) {
			baseURL := "http://" + target
			return rpc.NewRemoteStore(baseURL), nil
		},
	}

	// watcher обновляет кольцо при изменении состава нод в ZK
	membership.RunWatch(ctx, router, 100)

	// --- HTTP-сервер поверх Router ---
	server := rpc.NewServer(router, "8080")
	if err := server.Start(); err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("HTTP server is running on :8080 (with ZK-based sharding)")
	fmt.Println("Press Ctrl+C to stop...")

	<-ctx.Done()

	if err := server.Stop(); err != nil {
		fmt.Printf("Error stopping server: %v\n", err)
	}

	fmt.Println("LSMDB stopped")
	os.Exit(0)
}
