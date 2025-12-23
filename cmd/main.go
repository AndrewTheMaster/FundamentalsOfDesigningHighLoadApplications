package main

import (
	"context"
	"fmt"
	"lsmdb/internal/config"
	httpserver "lsmdb/internal/http"
	"lsmdb/pkg/cluster"
	cfgpkg "lsmdb/pkg/config"
	"lsmdb/pkg/raftadapter"
	rpcclient "lsmdb/pkg/rpc"
	"lsmdb/pkg/store"
	"lsmdb/pkg/wal"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func mustEnv(k string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		fmt.Printf("%s is not set\n", k)
		os.Exit(1)
	}
	return v
}

func mustRaftID() uint64 {
	raw := mustEnv("LSMDB_RAFT_ID")
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || id == 0 {
		fmt.Println("LSMDB_RAFT_ID invalid:", raw)
		os.Exit(1)
	}
	return id
}

// format: "1=http://node1:8080,2=http://node2:8080,3=http://node3:8080"
func mustRaftPeers() []pkgcfg.RaftPeerConfig {
	raw := mustEnv("LSMDB_RAFT_PEERS")
	parts := strings.Split(raw, ",")
	out := make([]pkgcfg.RaftPeerConfig, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			fmt.Println("LSMDB_RAFT_PEERS invalid part:", p)
			os.Exit(1)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(kv[0]), 10, 64)
		if err != nil || id == 0 {
			fmt.Println("LSMDB_RAFT_PEERS invalid id in:", p)
			os.Exit(1)
		}
		addr := strings.TrimSpace(kv[1])
		if addr == "" {
			fmt.Println("LSMDB_RAFT_PEERS empty addr in:", p)
			os.Exit(1)
		}
		out = append(out, pkgcfg.RaftPeerConfig{ID: id, Address: addr})
	}
	if len(out) == 0 {
		fmt.Println("LSMDB_RAFT_PEERS has no peers")
		os.Exit(1)
	}
	return out
}

// format: "1=http://localhost:8081,2=http://localhost:8082,3=http://localhost:8083"
func parsePeerMap(raw string) map[uint64]string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make(map[uint64]string, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			fmt.Println("LSMDB_PUBLIC_PEERS invalid part:", p)
			os.Exit(1)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(kv[0]), 10, 64)
		if err != nil || id == 0 {
			fmt.Println("LSMDB_PUBLIC_PEERS invalid id in:", p)
			os.Exit(1)
		}
		addr := strings.TrimSpace(kv[1])
		if addr == "" {
			fmt.Println("LSMDB_PUBLIC_PEERS empty addr in:", p)
			os.Exit(1)
		}
		out[id] = addr
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func contains(ss []string, x string) bool {
	for _, s := range ss {
		if s == x {
			return true
		}
	}
	return false
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg := config.Default()
	fmt.Printf("LSMDB starting. DataDir=%s\n", cfg.Storage.DataDir)

	localURL := mustEnv("LSMDB_ADVERTISE_URL") // например http://node1:8080
	raftID := mustRaftID()
	peers := mustRaftPeers()
	publicPeers := parsePeerMap(os.Getenv("LSMDB_PUBLIC_PEERS"))

	// --- WAL + store ---
	journal, err := wal.New(cfg.Storage.WALDir)
	if err != nil {
		fmt.Printf("Failed to init WAL: %v\n", err)
		os.Exit(1)
	}

	// ✅ ВАЖНО: дефолтный конфиг стора берём из pkg/config, а не pkg/store
	dbCfg := storecfg.Default()
	dbCfg.DB.Persistence.RootPath = cfg.Storage.DataDir

	db, err := store.New(&dbCfg, journal)
	if err != nil {
		fmt.Printf("Failed to init store: %v\n", err)
		os.Exit(1)
	}

	// --- Raft (одна группа на весь кластер) ---
	raftCfg := &pkgcfg.RaftConfig{
		ID:            raftID,
		ElectionTick:  10,
		HeartbeatTick: 1,
		Peers:         peers,
	}

	raftNode, err := raftadapter.NewNode(raftCfg, db)
	if err != nil {
		fmt.Printf("Failed to init raft node: %v\n", err)
		os.Exit(1)
	}

	// --- Ring по нодам (consistent hashing) ---
	vnodes := 100
	if raw := strings.TrimSpace(os.Getenv("LSMDB_VNODES")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			vnodes = n
		}
	}

	ring := cluster.NewHashRing(vnodes)
	for _, p := range peers {
		ring.AddNode(p.Address) // Должно совпадать форматом с localURL и rpc target (http://nodeX:8080)
	}
	fmt.Println("Ring nodes:", ring.ListNodes())

	// --- Replication factor ---
	rf := cfg.Replication.ReplicationFactor
	if raw := strings.TrimSpace(os.Getenv("LSMDB_RF")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			rf = n
		}
	}
	if rf <= 0 {
		rf = 1
	}

	// applyFilter: применять команду только на RF репликах по кольцу
	raftNode.SetApplyFilter(func(key string) bool {
		replicas, ok := ring.ReplicasForKey(key, rf)
		if !ok {
			return false
		}
		return contains(replicas, localURL)
	})

	// --- Router ---
	router := &cluster.Router{
		LocalAddr: localURL,
		Ring:      ring,
		RF:        rf,
		DB:        db,
		NewClient: func(target string) (cluster.Remote, error) {
			// ✅ ВАЖНО: используй тот конструктор, который у тебя реально есть в pkg/rpc:
			// Если ты оставила NewHTTPStore — оставь так:
			return rpcclient.NewHTTPStore(target), nil

			// Если у тебя функция называется NewRemoteStore — тогда замени строку выше на:
			// return rpcclient.NewRemoteStore(target), nil
		},
	}

	// --- HTTP server ---
	port := "8080"
	if u := strings.TrimSpace(os.Getenv("LSMDB_PORT")); u != "" {
		port = u
	}

	srv := httpserver.NewServer(
		raftNode,
		db,
		router,
		port,
		localURL,
		publicPeers,
	)

	// Raft run loop
	go func() {
		_ = raftNode.Run(ctx)
	}()

	if err := srv.Start(); err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("HTTP server is running on %s\n", localURL)

	<-ctx.Done()

	_ = srv.Stop()
	_ = raftNode.Stop()
	fmt.Println("LSMDB stopped")
	time.Sleep(200 * time.Millisecond)
}
