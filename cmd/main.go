package main

import (
	"context"
	"fmt"
	"log/slog"
	"lsmdb/internal/http"
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

	// Create gRPC server
	server := http.NewServer(
		db,
		fmt.Sprintf("%d", cfg.Server.Port),
	)
	if err := server.Start(); err != nil {
		slog.Error("Failed to start server", "error", err)
		return
	}

	slog.Info("gRPC server running", "port", cfg.Server.Port)
	slog.Info("Press Ctrl+C to stop...")

	demo(db)

	<-ctx.Done()

	// Stop server
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
