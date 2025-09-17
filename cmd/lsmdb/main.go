package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"lsmdb/internal/config"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg := config.Default()

	fmt.Printf("LSMDB starting (Lab 1 skeleton). DataDir=%s\n", cfg.Storage.DataDir)

	<-ctx.Done()

	fmt.Println("LSMDB stopped")
	os.Exit(0)
} 