package cluster

import (
	"fmt"
	"math"
	"testing"
)

// кольцо из N нод с заданным числом реплик
func makeRing(n, replicas int) *HashRing {
	r := NewHashRing(replicas)
	for i := 1; i <= n; i++ {
		r.AddNode(fmt.Sprintf("node%d:8080", i))
	}
	return r
}

// равномерность распределения ~ 1/N с допуском
func TestRing_DistributionUniformity(t *testing.T) {
	N := 3
	r := makeRing(N, 128)
	total := 60_000

	counts := map[string]int{}
	for i := 0; i < total; i++ {
		k := fmt.Sprintf("key-%d", i)
		n, ok := r.GetNode(k)
		if !ok {
			t.Fatalf("ring returned no owner for key %q", k)
		}
		counts[n]++
	}
	ideal := float64(total) / float64(N)
	tolerance := 0.15 * ideal // 15% коридор

	for node, c := range counts {
		diff := math.Abs(float64(c) - ideal)
		if diff > tolerance {
			t.Fatalf("node %s: count=%d ideal=%.0f diff=%.0f > tol=%.0f", node, c, ideal, diff, tolerance)
		}
	}
}

// минимальные перемещения при добавлении ноды (~1/(N+1))
func TestRing_MinimalMovementOnAdd(t *testing.T) {
	N := 3
	replicas := 128
	total := 100_000

	r := makeRing(N, replicas)
	before := make([]string, total)
	for i := 0; i < total; i++ {
		owner, ok := r.GetNode(fmt.Sprintf("k-%d", i))
		if !ok {
			t.Fatalf("no owner before add for i=%d", i)
		}
		before[i] = owner
	}

	r.AddNode("node4:8080")

	moved := 0
	for i := 0; i < total; i++ {
		now, ok := r.GetNode(fmt.Sprintf("k-%d", i))
		if !ok {
			t.Fatalf("no owner after add for i=%d", i)
		}
		if before[i] != now {
			moved++
		}
	}
	frac := float64(moved) / float64(total)
	if frac < 0.18 || frac > 0.32 { // ожидаемо около 0.25
		t.Fatalf("moved fraction %.3f out of expected range [0.18..0.32]", frac)
	}
}

func TestRing_Deterministic(t *testing.T) {
	a := makeRing(3, 128)
	b := makeRing(3, 128)
	for i := 0; i < 10_000; i++ {
		k := fmt.Sprintf("id-%d", i)
		oa, oka := a.GetNode(k)
		ob, okb := b.GetNode(k)
		if !oka || !okb || oa != ob {
			t.Fatalf("non-deterministic mapping for %s (oka=%v okb=%v oa=%q ob=%q)", k, oka, okb, oa, ob)
		}
	}
}

func TestRing_RemoveNode(t *testing.T) {
	r := makeRing(3, 128)
	owner, ok := r.GetNode("foo")
	if !ok {
		t.Fatal("no owner for key foo")
	}
	r.RemoveNode(owner)
	newOwner, ok := r.GetNode("foo")
	if !ok || newOwner == "" || newOwner == owner {
		t.Fatalf("remove failed: old=%s new=%s ok=%v", owner, newOwner, ok)
	}
}
