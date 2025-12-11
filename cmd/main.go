package main

import (
	"context"
	"fmt"
	"lsmdb/internal/config"
	"lsmdb/pkg/cluster"
	"lsmdb/pkg/rpc"
	"lsmdb/pkg/store"
	"lsmdb/pkg/wal"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, err := initConfig("./config/application.yml")
	if err != nil {
		panic("failed to init config: " + err.Error())
	}

<<<<<<< HEAD
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

	// первичная сборка кольца
	ring, err := membership.BuildRing(100)
	if err != nil {
		fmt.Printf("Failed to build ring from ZooKeeper: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Initial ring nodes:", ring.ListNodes())

	// --- локальное хранилище ---
	db, err := store.New(cfg.Storage.DataDir, &timeProvider{})
=======
	// Initialize global logger
	initLogger(&cfg)

	slog.Info("LSMDB starting", "rootPath", cfg.Persistence.RootPath)

	jr, err := wal.New(cfg.Persistence.RootPath)
	if err != nil {
		panic("failed to create WAL: " + err.Error())
	}

	// Create store
	db, err := store.New(&cfg, jr)
>>>>>>> origin/lab4
	if err != nil {
		slog.Error("failed to create db", "error", err)
		panic(err)
	}

	// --- Router с изначальным кольцом ---
	router := &cluster.Router{
		LocalAddr: localAddr,
		Ring:      ring,
		DB:        db,
		NewClient: func(target string) (cluster.Remote, error) {
			baseURL := "http://" + target
			return rpc.NewRemoteStore(baseURL), nil
		},
	}

	// запустить watcher, который будет обновлять ring при изменении /lsmdb/nodes
	membership.RunWatch(ctx, router, 100)

	// --- сервер поверх Router ---
	server := rpc.NewServer(router, "8080")
	if err := server.Start(); err != nil {
		slog.Error("Failed to start server", "error", err)
		return
	}

	fmt.Println("HTTP server is running on :8080 (with ZK-based sharding)")
	fmt.Println("Press Ctrl+C to stop...")

	// // Demo test operations
	// fmt.Println("Testing basic operations via Router (with sharding)...")

	// // Put some data
	// if err := router.PutString("user:1", "Alice"); err != nil {
	// 	fmt.Printf("Error putting user:1: %v\n", err)
	// }
	// if err := router.PutString("user:2", "Bob"); err != nil {
	// 	fmt.Printf("Error putting user:2: %v\n", err)
	// }
	// if err := router.PutString("config:timeout", "30s"); err != nil {
	// 	fmt.Printf("Error putting config:timeout: %v\n", err)
	// }

	// // Get data
	// if value, found, err := router.GetString("user:1"); err != nil {
	// 	fmt.Printf("Error getting user:1: %v\n", err)
	// } else if found {
	// 	fmt.Printf("user:1 = %s\n", value)
	// }

	// if value, found, err := router.GetString("user:2"); err != nil {
	// 	fmt.Printf("Error getting user:2: %v\n", err)
	// } else if found {
	// 	fmt.Printf("user:2 = %s\n", value)
	// }

	// // Update data
	// if err := router.PutString("user:1", "Alice Updated"); err != nil {
	// 	fmt.Printf("Error updating user:1: %v\n", err)
	// }

	// if value, found, err := router.GetString("user:1"); err != nil {
	// 	fmt.Printf("Error getting updated user:1: %v\n", err)
	// } else if found {
	// 	fmt.Printf("Updated user:1 = %s\n", value)
	// }

	// // Delete data
	// if err := router.Delete("user:2"); err != nil {
	// 	fmt.Printf("Error deleting user:2: %v\n", err)
	// }

	// if _, found, err := router.GetString("user:2"); err != nil {
	// 	fmt.Printf("Error checking deleted user:2: %v\n", err)
	// } else if found {
	// 	fmt.Println("ERROR: user:2 should be deleted but was found")
	// } else {
	// 	fmt.Println("user:2 successfully deleted")
	// }

	// fmt.Println("Demo via Router completed successfully!")

	<-ctx.Done()

	if err := server.Stop(); err != nil {
		slog.Error("Error stopping server", "error", err)
	}

	slog.Info("LSMDB terminated")
}

func demo(db *store.Store) {
	// Demo operations
	slog.Info("Testing basic operations")

	// Put some data
	if err := db.PutString("user:1", "Alice"); err != nil {
		slog.Error("Error putting user:1", "error", err)
	}

	if err := db.PutString("user:2", "Bob"); err != nil {
		slog.Error("Error putting user:2", "error", err)
	}

	if err := db.PutString("config:timeout", "30s"); err != nil {
		slog.Error("Error putting config:timeout", "error", err)
	}

	// Get data
	if value, found, err := db.GetString("user:1"); err != nil {
		slog.Error("Error getting user:1", "error", err)
	} else if found {
		slog.Info("user:1", "value", value)
	}

	if value, found, err := db.GetString("user:2"); err != nil {
		slog.Error("Error getting user:2", "error", err)
	} else if found {
		slog.Info("user:2", "value", value)
	}

	// Update data
	if err := db.PutString("user:1", "Alice Updated"); err != nil {
		slog.Error("Error updating user:1", "error", err)
	}

	// Verify update
	if value, found, err := db.GetString("user:1"); err != nil {
		slog.Error("Error getting updated user:1", "error", err)
	} else if found {
		slog.Info("Updated user:1", "value", value)
	}

	// Delete data
	if err := db.Delete("user:2"); err != nil {
		slog.Error("Error deleting user:2", "error", err)
	}

	// Verify deletion
	if _, found, err := db.GetString("user:2"); err != nil {
		slog.Error("Error checking deleted user:2", "error", err)
	} else if found {
		slog.Error("user:2 should be deleted but was found")
	} else {
		slog.Info("user:2 successfully deleted")
	}

	slog.Info("Demo completed successfully!")
}
