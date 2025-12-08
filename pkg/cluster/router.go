package cluster

import (
	"fmt"
	"sync"
)

// удалённый клиент
type Remote interface {
	PutString(key, value string) error
	GetString(key string) (string, bool, error)
	Delete(key string) error
}

// фабрика удалённых клиентов
type ClientFactory func(target string) (Remote, error)

type Router struct {
	LocalAddr string // текущая нода
	Ring      *HashRing
	DB        KV
	NewClient ClientFactory

	mu sync.RWMutex
}

func (r *Router) owner(key string) (string, error) {
	r.mu.RLock()
	ring := r.Ring
	r.mu.RUnlock()

	if ring == nil {
		return "", fmt.Errorf("ring is not initialized")
	}

	node, ok := r.Ring.GetNode(key)
	if !ok {
		return "", fmt.Errorf("ring is empty")
	}
	return node, nil
}

func (r *Router) UpdateRing(newRing *HashRing) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Ring = newRing
	fmt.Println("[router] ring updated; nodes:", newRing.ListNodes())
}

func (r *Router) log(method, key, target string, local bool) {
	where := "remote"
	if local {
		where = "local"
	}

	fmt.Printf("[router] %-6s key=%s → %s (%s)\n", method, key, target, where)
}

func (r *Router) PutString(key, value string) error {
	target, err := r.owner(key)
	if err != nil {
		return err
	}

	local := target == r.LocalAddr
	r.log("PUT", key, target, local)

	if local {
		return r.DB.PutString(key, value)
	}

	cl, err := r.NewClient(target)
	if err != nil {
		return fmt.Errorf("router: create client: %w", err)
	}

	return cl.PutString(key, value)
}

func (r *Router) GetString(key string) (string, bool, error) {
	target, err := r.owner(key)
	if err != nil {
		return "", false, err
	}

	local := target == r.LocalAddr
	r.log("GET", key, target, local)

	if local {
		return r.DB.GetString(key)
	}

	cl, err := r.NewClient(target)
	if err != nil {
		return "", false, fmt.Errorf("router: create client: %w", err)
	}

	return cl.GetString(key)
}

func (r *Router) Delete(key string) error {
	target, err := r.owner(key)
	if err != nil {
		return err
	}

	local := target == r.LocalAddr
	r.log("DELETE", key, target, local)

	if local {
		return r.DB.Delete(key)
	}

	cl, err := r.NewClient(target)
	if err != nil {
		return fmt.Errorf("router: create client: %w", err)
	}

	return cl.Delete(key)
}
