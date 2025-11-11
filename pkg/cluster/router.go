package cluster

import (
	"context"
	"fmt"
	"lsmdb/pkg/store"
)

// минимальный интерфейс удалённого клиента
type Remote interface {
	Put(ctx context.Context, key, value string) error
	Get(ctx context.Context, key string) (string, bool, error)
	Delete(ctx context.Context, key string) error
	Close() error
}

// возвращает клиента для указанного target (host:port).
type ClientFactory func(target string) (Remote, error)

type Router struct {
	LocalAddr string // текущая нода
	Ring      *HashRing
	DB        *store.DB
	NewClient ClientFactory
}

func (r *Router) owner(key string) (string, error) {
	node, ok := r.Ring.GetNode(key)
	if !ok {
		return "", fmt.Errorf("ring is empty")
	}
	return node, nil
}

func (r *Router) Put(ctx context.Context, key, value string) error {
	target, err := r.owner(key)
	if err != nil {
		return err
	}
	if target == r.LocalAddr {
		return r.DB.PutString(key, value)
	}
	cl, err := r.NewClient(target)
	if err != nil {
		return fmt.Errorf("router: create client: %w", err)
	}
	defer cl.Close()
	return cl.Put(ctx, key, value)
}

func (r *Router) Get(ctx context.Context, key string) (string, bool, error) {
	target, err := r.owner(key)
	if err != nil {
		return "", false, err
	}
	if target == r.LocalAddr {
		return r.DB.GetString(key)
	}
	cl, err := r.NewClient(target)
	if err != nil {
		return "", false, fmt.Errorf("router: create client: %w", err)
	}
	defer cl.Close()
	return cl.Get(ctx, key)
}

func (r *Router) Delete(ctx context.Context, key string) error {
	target, err := r.owner(key)
	if err != nil {
		return err
	}
	if target == r.LocalAddr {
		return r.DB.Delete(key)
	}
	cl, err := r.NewClient(target)
	if err != nil {
		return fmt.Errorf("router: create client: %w", err)
	}
	defer cl.Close()
	return cl.Delete(ctx, key)
}
