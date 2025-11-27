package cluster

import (
	"fmt"
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
	// ReplicationFactor определяет, сколько уникальных нод должны хранить ключ.
	// Если 0, используется значение по умолчанию (1).
	ReplicationFactor int
}

func (r *Router) owner(key string) (string, error) {
	node, ok := r.Ring.GetNode(key)
	if !ok {
		return "", fmt.Errorf("ring is empty")
	}
	return node, nil
}

func (r *Router) log(method, key, target string, local bool) {
	where := "remote"
	if local {
		where = "local"
	}

	fmt.Printf("[router] %-6s key=%s → %s (%s)\n", method, key, target, where)
}

func (r *Router) replicationFactor() int {
	if r.ReplicationFactor <= 0 {
		return 1
	}
	return r.ReplicationFactor
}

func (r *Router) targetsFor(key string) ([]string, error) {
	targets := r.Ring.Successors(key, r.replicationFactor())
	if len(targets) == 0 {
		node, err := r.owner(key)
		if err != nil {
			return nil, err
		}
		targets = []string{node}
	}

	// стараемся сначала обращаться к локальной ноде
	if r.LocalAddr != "" {
		for i, t := range targets {
			if t == r.LocalAddr && i != 0 {
				targets[0], targets[i] = targets[i], targets[0]
				break
			}
		}
	}

	return targets, nil
}

func (r *Router) PutString(key, value string) error {
	targets, err := r.targetsFor(key)
	if err != nil {
		return err
	}

	var firstErr error
	successes := 0

	for _, target := range targets {
		local := target == r.LocalAddr
		r.log("PUT", key, target, local)

		var putErr error
		if local {
			putErr = r.DB.PutString(key, value)
		} else {
			cl, err := r.NewClient(target)
			if err != nil {
				putErr = fmt.Errorf("router: create client: %w", err)
			} else {
				putErr = cl.PutString(key, value)
			}
		}

		if putErr != nil {
			if firstErr == nil {
				firstErr = putErr
			}
			continue
		}
		successes++
	}

	if successes == 0 && firstErr != nil {
		return firstErr
	}
	return nil
}

func (r *Router) GetString(key string) (string, bool, error) {
	targets, err := r.targetsFor(key)
	if err != nil {
		return "", false, err
	}

	var lastErr error
	for _, target := range targets {
		local := target == r.LocalAddr
		r.log("GET", key, target, local)

		var (
			value  string
			found  bool
			getErr error
		)

		if local {
			value, found, getErr = r.DB.GetString(key)
		} else {
			cl, err := r.NewClient(target)
			if err != nil {
				getErr = fmt.Errorf("router: create client: %w", err)
			} else {
				value, found, getErr = cl.GetString(key)
			}
		}

		if getErr != nil {
			lastErr = getErr
			continue
		}

		if found {
			return value, true, nil
		}
	}

	return "", false, lastErr
}

func (r *Router) Delete(key string) error {
	targets, err := r.targetsFor(key)
	if err != nil {
		return err
	}

	var firstErr error
	for _, target := range targets {
		local := target == r.LocalAddr
		r.log("DELETE", key, target, local)

		var delErr error
		if local {
			delErr = r.DB.Delete(key)
		} else {
			cl, err := r.NewClient(target)
			if err != nil {
				delErr = fmt.Errorf("router: create client: %w", err)
			} else {
				delErr = cl.Delete(key)
			}
		}

		if delErr != nil && firstErr == nil {
			firstErr = delErr
		}
	}

	return firstErr
}

// PutReplica сохраняет значение только на локальной ноде (без маршрутизации).
func (r *Router) PutReplica(key, value string) error {
	r.log("PUT*", key, r.LocalAddr, true)
	return r.DB.PutString(key, value)
}

// DeleteReplica удаляет значение только на локальной ноде (без маршрутизации).
func (r *Router) DeleteReplica(key string) error {
	r.log("DEL*", key, r.LocalAddr, true)
	return r.DB.Delete(key)
}
