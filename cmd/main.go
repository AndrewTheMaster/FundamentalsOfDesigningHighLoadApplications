package main

import (
	"context"
	"fmt"
	"lsmdb/internal/config"
	"lsmdb/pkg/cluster"
	"lsmdb/pkg/rpc"
	"lsmdb/pkg/store"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// timeProvider implements iTimeProvider
type timeProvider struct{}

func (tp *timeProvider) Now() time.Time {
	return time.Now()
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg := config.Default()

	fmt.Printf("LSMDB starting (Lab 5 Sharding). DataDir=%s\n", cfg.Storage.DataDir)

	clusterCfg := cluster.FromEnv()
	fmt.Printf("Cluster config: local=%s, peers=%v\n", clusterCfg.Local, clusterCfg.Peers)

	ring := cluster.NewHashRing(100) // 100 виртуальных нод
	ring.AddNode(string(clusterCfg.Local))

	for _, p := range clusterCfg.Peers {
		ring.AddNode(string(p))
	}
	fmt.Println("Cluster ring initialized with nodes:", ring.ListNodes())

	// Create local store
	db, err := store.New(cfg.Storage.DataDir, &timeProvider{})
	if err != nil {
		panic(err)
	}

	// Create remote store
	router := &cluster.Router{
		LocalAddr: string(clusterCfg.Local), // тоже строка
		Ring:      ring,
		DB:        db,
		NewClient: func(target string) (cluster.Remote, error) {
			baseURL := "http://" + target
			return rpc.NewRemoteStore(baseURL), nil
		},
	}

	// Create gRPC server
	server := rpc.NewServer(router, "8080")

	// Start server
	if err := server.Start(); err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("gRPC/HTTP server running on port 8080")
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
		fmt.Printf("Error stopping server: %v\n", err)
	}

	fmt.Println("LSMDB stopped")
	os.Exit(0)
}
