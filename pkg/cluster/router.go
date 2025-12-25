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
	mu        sync.RWMutex
	LocalAddr string
	Ring      *HashRing
	RF        int
	DB        interface {
		PutString(key, value string) error
		GetString(key string) (string, bool, error)
		Delete(key string) error
	}
	NewClient func(target string) (Remote, error)

	aliveMu sync.RWMutex
	alive   map[string]bool // addr -> alive
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

func (r *Router) SetAlive(addrs []string) {
	m := make(map[string]bool, len(addrs))
	for _, a := range addrs {
		m[a] = true
	}
	r.aliveMu.Lock()
	r.alive = m
	r.aliveMu.Unlock()
}

func (r *Router) isAlive(addr string) bool {
	r.aliveMu.RLock()
	defer r.aliveMu.RUnlock()
	// если alive ещё не настроен — считаем всех живыми (стартовое поведение)
	if r.alive == nil {
		return true
	}
	return r.alive[addr]
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

	// кандидаты — все ноды в порядке кольца (owner+successors), уникальные
	candidates, ok := ring.ReplicasForKey(key, max(rf, len(ring.ListNodes())))
	if !ok || len(candidates) == 0 {
		return nil, fmt.Errorf("empty replica candidates")
	}

	// фильтруем живых, пока не набрали rf
	out := make([]string, 0, rf)
	seen := map[string]bool{}
	for _, a := range candidates {
		if seen[a] {
			continue
		}
		seen[a] = true
		if r.isAlive(a) {
			out = append(out, a)
			if len(out) == rf {
				break
			}
		}
	}

	// если живых меньше rf — возвращаем сколько есть (это нормально для деградации)
	if len(out) == 0 {
		return nil, fmt.Errorf("no alive replicas for key=%s", key)
	}
	return out, nil
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
func (r *Router) Put(key, value string) error {
	replicas, err := r.replicasForKey(key)
	if err != nil {
		return err
	}

	// 1) порядок попыток: сначала локальная (если мы реплика), потом остальные
	targets := make([]string, 0, len(replicas))
	if contains(replicas, r.LocalAddr) {
		targets = append(targets, r.LocalAddr)
	}
	for _, a := range replicas {
		if a != r.LocalAddr {
			targets = append(targets, a)
		}
	}

	// 2) пробуем по очереди
	var lastErr error
	for _, target := range targets {
		local := target == r.LocalAddr
		r.log("PUT", key, target, local)

		cl, err := r.NewClient(target)
		if err != nil {
			lastErr = err
			continue
		}

		if err := cl.PutString(key, value); err == nil {
			return nil
		} else {
			lastErr = err
			continue
		}
	}

	return fmt.Errorf("PUT failed for key=%s: all replicas unreachable, lastErr=%v", key, lastErr)
}

// Delete: пытаемся удалить через любую живую реплику из replica-set.
// Дальше удаление также должно пройти через Raft.
func (r *Router) Delete(key string) error {
	replicas, err := r.replicasForKey(key)
	if err != nil {
		return err
	}

	targets := make([]string, 0, len(replicas))
	if contains(replicas, r.LocalAddr) {
		targets = append(targets, r.LocalAddr)
	}
	for _, a := range replicas {
		if a != r.LocalAddr {
			targets = append(targets, a)
		}
	}

	var lastErr error
	for _, target := range targets {
		local := target == r.LocalAddr
		r.log("DELETE", key, target, local)

		cl, err := r.NewClient(target)
		if err != nil {
			lastErr = err
			continue
		}

		if err := cl.Delete(key); err == nil {
			return nil
		} else {
			lastErr = err
			continue
		}
	}

	return fmt.Errorf("DELETE failed for key=%s: all replicas unreachable, lastErr=%v", key, lastErr)
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
