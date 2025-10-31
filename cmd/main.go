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

	fmt.Printf("LSMDB starting (Lab 3 implementation). DataDir=%s\n", cfg.Storage.DataDir)

	ring := cluster.NewHashRing(100) // 100 виртуальных нод

	nodes := []string{
		"node1:8080",
		"node2:8080",
		"node3:8080",
	}
	for _, n := range nodes {
		ring.AddNode(n)
	}

	fmt.Println("Cluster ring initialized with nodes:", ring.ListNodes())

	// тест распределения ключей
	sampleKeys := []string{"user:1", "user:2", "config:timeout", "products", "order:42"}
	for _, k := range sampleKeys {
		fmt.Printf("Key %-15s → %s\n", k, ring.GetNode(k))
	}

	// Create store
	db, err := store.New(cfg.Storage.DataDir, &timeProvider{})
	if err != nil {
		panic(err)
	}

	// Create gRPC server
	server := rpc.NewServer(db, "8080")

	// Start server
	if err := server.Start(); err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		os.Exit(1)
	}

	// Demo operations
	fmt.Println("Testing basic operations...")

	// Put some data
	if err := db.PutString("user:1", "Alice"); err != nil {
		fmt.Printf("Error putting user:1: %v\n", err)
	}

	if err := db.PutString("user:2", "Bob"); err != nil {
		fmt.Printf("Error putting user:2: %v\n", err)
	}

	if err := db.PutString("config:timeout", "30s"); err != nil {
		fmt.Printf("Error putting config:timeout: %v\n", err)
	}

	// Get data
	if value, found, err := db.GetString("user:1"); err != nil {
		fmt.Printf("Error getting user:1: %v\n", err)
	} else if found {
		fmt.Printf("user:1 = %s\n", value)
	}

	if value, found, err := db.GetString("user:2"); err != nil {
		fmt.Printf("Error getting user:2: %v\n", err)
	} else if found {
		fmt.Printf("user:2 = %s\n", value)
	}

	// Update data
	if err := db.PutString("user:1", "Alice Updated"); err != nil {
		fmt.Printf("Error updating user:1: %v\n", err)
	}

	// Verify update
	if value, found, err := db.GetString("user:1"); err != nil {
		fmt.Printf("Error getting updated user:1: %v\n", err)
	} else if found {
		fmt.Printf("Updated user:1 = %s\n", value)
	}

	// Delete data
	if err := db.Delete("user:2"); err != nil {
		fmt.Printf("Error deleting user:2: %v\n", err)
	}

	// Verify deletion
	if _, found, err := db.GetString("user:2"); err != nil {
		fmt.Printf("Error checking deleted user:2: %v\n", err)
	} else if found {
		fmt.Println("ERROR: user:2 should be deleted but was found")
	} else {
		fmt.Println("user:2 successfully deleted")
	}

	fmt.Println("Demo completed successfully!")
	fmt.Println("gRPC server running on port 8080")
	fmt.Println("HTTP health check on port 8081")
	fmt.Println("Press Ctrl+C to stop...")

	<-ctx.Done()

	// Stop server
	if err := server.Stop(); err != nil {
		fmt.Printf("Error stopping server: %v\n", err)
	}

	fmt.Println("LSMDB stopped")
	os.Exit(0)
}
