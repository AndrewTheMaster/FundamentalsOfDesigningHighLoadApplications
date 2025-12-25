package main

import (
	"context"
	"fmt"
	"log/slog"
	"lsmdb/internal/http"
	"lsmdb/pkg/cluster"
	"lsmdb/pkg/config"
	"lsmdb/pkg/raftadapter"
	"lsmdb/pkg/store"
	"lsmdb/pkg/wal"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, err := initConfig("./config/application.yml")
	if err != nil {
		panic("failed to init config: " + err.Error())
	}

	// Initialize global logger
	initLogger(&cfg)

	slog.Info("LSMDB starting", "rootPath", cfg.Persistence.RootPath)

	jr, err := wal.New(cfg.Persistence.RootPath)
	if err != nil {
		panic("failed to create WAL: " + err.Error())
	}

	// Create store
	db, err := store.New(&cfg, jr)
	if err != nil {
		slog.Error("failed to create db", "error", err)
		panic(err)
	}

	node, err := raftadapter.NewNode(&cfg.Raft, db)
	if err != nil {
		slog.Error("failed to create raft node", "error", err)
		panic(err)
	}

	var server *http.Server

	if len(cfg.Cluster.Clusters) > 0 {
		slog.Info("Starting in sharded mode",
			"local_cluster", cfg.Cluster.LocalClusterID,
			"total_clusters", len(cfg.Cluster.Clusters))

		localAddr := getLocalAddress(&cfg)
		shardedDB := cluster.NewShardedRaftDB(
			localAddr,
			node,
			db,
			func(addr string) (cluster.Remote, error) {
				return cluster.NewHTTPClient(addr), nil
			},
		)

		for _, clusterInfo := range cfg.Cluster.Clusters {
			shardedDB.AddCluster(
				clusterInfo.ID,
				clusterInfo.Leader,
				clusterInfo.Replicas,
			)
		}

		go func() {
			if err := node.Run(context.Background()); err != nil {
				slog.Error("Raft node error", "error", err)
			}
		}()

		server = http.NewShardedServer(
			shardedDB,
			db,
			fmt.Sprintf("%d", cfg.Server.Port),
		)
	} else {
		slog.Info("Starting in single Raft cluster mode")

		// Обычный режим - один Raft кластер без шардирования
		server = http.NewServer(
			node,
			fmt.Sprintf("%d", cfg.Server.Port),
		)
		server.SetStore(db)
	}

	if err := server.Start(); err != nil {
		slog.Error("Failed to start server", "error", err)
		return
	}

	slog.Info("HTTP server running", "port", cfg.Server.Port)
	slog.Info("Press Ctrl+C to stop...")

	// Demo operations только в single mode
	if len(cfg.Cluster.Clusters) == 0 {
		demo(db)
	}

	<-ctx.Done()

	// Stop server
	if err := server.Stop(); err != nil {
		slog.Error("Error stopping server", "error", err)
	}

	slog.Info("LSMDB terminated")
}

func getLocalAddress(cfg *config.Config) string {
	for _, peer := range cfg.Raft.Peers {
		if peer.ID == cfg.Raft.ID {
			return peer.Address
		}
	}
	return fmt.Sprintf("http://localhost:%d", cfg.Server.Port)
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
