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
	LocalAddr string
	Ring      *HashRing
	RF        int

	// локальный KV store для чтения (если нода — реплика)
	DB interface {
		PutString(key, value string) error
		GetString(key string) (string, bool, error)
		Delete(key string) error
	}

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

	node, ok := ring.GetNode(key)
	if !ok {
		return "", fmt.Errorf("ring is empty")
	}
	return node, nil
}

func (r *Router) UpdateRing(newRing *HashRing) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Ring = newRing
	if newRing == nil {
		fmt.Println("[router] ring updated; <nil>")
		return
	}
	fmt.Println("[router] ring updated; nodes:", newRing.ListNodes())
}

func (r *Router) log(method, key, target string, local bool) {
	where := "remote"
	if local {
		where = "local"
	}

	fmt.Printf("[router] %-6s key=%s → %s (%s)\n", method, key, target, where)
}

func (r *Router) LegacyPutString(key, value string) error {
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

func (r *Router) LegacyGetString(key string) (string, bool, error) {
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

func (r *Router) LegacyDelete(key string) error {
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

func (r *Router) replicasForKey(key string) ([]string, error) {
	r.mu.RLock()
	ring := r.Ring
	rf := r.RF
	r.mu.RUnlock()

	if ring == nil {
		return nil, fmt.Errorf("ring is not initialized")
	}
	if rf <= 0 {
		return nil, fmt.Errorf("RF must be > 0")
	}

	replicas, ok := ring.ReplicasForKey(key, rf)
	if !ok || len(replicas) == 0 {
		return nil, fmt.Errorf("ring is empty")
	}
	return replicas, nil
}

func contains(ss []string, x string) bool {
	for _, s := range ss {
		if s == x {
			return true
		}
	}
	return false
}

// Put: отправляем на "первую реплику" (или на себя, если мы в replica set).
// Сервер сам редиректит на raft-лидера и делает Execute через Raft. :contentReference[oaicite:9]{index=9}
func (r *Router) Put(key, value string) error {
	replicas, err := r.replicasForKey(key)
	if err != nil {
		return err
	}

	// предпочтение локали (если мы реплика) — меньше hops
	target := replicas[0]
	if contains(replicas, r.LocalAddr) {
		target = r.LocalAddr
	}

	if target == r.LocalAddr {
		// важно: локальный PUT должен идти через тот же HTTP API/raft,
		// иначе мы обойдём Raft. Поэтому локально мы тоже используем Remote,
		// либо вызываем внутренний handler напрямую (не советую).
	}

	cl, err := r.NewClient(target)
	if err != nil {
		return fmt.Errorf("router: create client: %w", err)
	}
	return cl.PutString(key, value)
}

func (r *Router) Delete(key string) error {
	replicas, err := r.replicasForKey(key)
	if err != nil {
		return err
	}

	target := replicas[0]
	if contains(replicas, r.LocalAddr) {
		target = r.LocalAddr
	}

	cl, err := r.NewClient(target)
	if err != nil {
		return fmt.Errorf("router: create client: %w", err)
	}
	return cl.Delete(key)
}

// Get: читаем с любой живой реплики, локально — если мы в replica set.
func (r *Router) Get(key string) (string, bool, error) {
	replicas, err := r.replicasForKey(key)
	if err != nil {
		return "", false, err
	}

	if contains(replicas, r.LocalAddr) {
		// локальное чтение из DB ок, потому что DB уже заполнится через applyFilter (это следующий этап)
		return r.DB.GetString(key)
	}

	var lastErr error
	for _, addr := range replicas {
		cl, err := r.NewClient(addr)
		if err != nil {
			lastErr = err
			continue
		}
		v, ok, err := cl.GetString(key)
		if err == nil {
			return v, ok, nil
		}
		lastErr = err
	}
	return "", false, fmt.Errorf("all replicas failed for key=%s: lastErr=%v", key, lastErr)
}

func (r *Router) IsLocalReplica(key string) bool {
	replicas, err := r.replicasForKey(key)
	if err != nil {
		return false
	}
	return contains(replicas, r.LocalAddr)
}