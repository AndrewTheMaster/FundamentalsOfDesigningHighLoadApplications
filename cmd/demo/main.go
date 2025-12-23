package main

import (
	"bufio"
	"fmt"
	"io"
	nethttp "net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"lsmdb/pkg/cluster"
	rpcclient "lsmdb/pkg/rpc"
)

func pause(msg string) {
	fmt.Println()
	fmt.Println(msg)
	fmt.Print("Нажми Enter, чтобы продолжить...")
	_, _ = bufio.NewReader(os.Stdin).ReadBytes('\n')
}

func contains(ss []string, x string) bool {
	for _, s := range ss {
		if s == x {
			return true
		}
	}
	return false
}

func main() {
	// base URL берём аргументом: go run ./cmd/demo http://localhost:8081
	if len(os.Args) < 2 {
		fmt.Println("usage: go run ./cmd/demo <base_url>")
		fmt.Println("example: go run ./cmd/demo http://localhost:8081")
		os.Exit(1)
	}
	base := strings.TrimRight(strings.TrimSpace(os.Args[1]), "/")
	if base == "" {
		fmt.Println("base_url is empty")
		os.Exit(1)
	}

	// nodes для кольца: DEMO_NODES="http://localhost:8081,http://localhost:8082,http://localhost:8083"
	rawNodes := strings.TrimSpace(os.Getenv("DEMO_NODES"))
	var nodes []string
	if rawNodes == "" {
		nodes = []string{base}
	} else {
		nodes = strings.Split(rawNodes, ",")
		for i := range nodes {
			nodes[i] = strings.TrimRight(strings.TrimSpace(nodes[i]), "/")
		}
	}

	// RF: DEMO_RF=2 или 3
	rf := 2
	if raw := strings.TrimSpace(os.Getenv("DEMO_RF")); raw != "" {
		if raw == "3" {
			rf = 3
		}
	}

	vnodes := 100
	fmt.Println("=== DEMO: one Raft group + consistent hashing + RF replication ===")
	fmt.Println("Base:", base)
	fmt.Println("Nodes:", nodes, "RF:", rf)

	ring := cluster.NewHashRing(vnodes)
	for _, n := range nodes {
		ring.AddNode(n)
	}

	// клиент ходит в /api/string и умеет работать с редиректами лидера
	client := rpcclient.NewHTTPStore(base)

	// 1) PUT 100 keys
	const totalKeys = 100
	fmt.Printf("\n[STEP 1] Put %d keys via %s\n", totalKeys, base)

	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("val-%d", i)
		if err := client.PutString(key, val); err != nil {
			fmt.Printf("PUT %s error: %v\n", key, err)
			os.Exit(1)
		}
	}

	// 2) Expected distribution по replica set (это теоретическое, по кольцу)
	fmt.Println("\n[STEP 2] Expected replica distribution (by ring, RF)")
	countByNode := map[string]int{}
	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		reps, _ := ring.ReplicasForKey(key, rf)
		for _, r := range reps {
			countByNode[r]++
		}
	}
	for _, n := range nodes {
		fmt.Printf("  %s holds ~%d replicas\n", n, countByNode[n])
	}

	pause(`[STEP 3] Сейчас урони одну ноду (например):
  docker compose stop node2

Потом нажми Enter — demo проверит чтение каждого key через replica-set (owner+successors).`)

	// 4) Проверить доступность ключей после падения ноды:
	fmt.Println("\n[STEP 4] Check keys after node failure (try replicas by ring)")
	var okCount, notFound, errCount int

	httpc := &nethttp.Client{
		Timeout: 2 * time.Second,
		CheckRedirect: func(req *nethttp.Request, via []*nethttp.Request) error {
			if len(via) >= 10 {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}

	const debugLimit = 10
	debugPrinted := 0

	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key-%d", i)

		reps, ok := ring.ReplicasForKey(key, rf)
		if !ok || len(reps) == 0 {
			errCount++
			if debugPrinted < debugLimit {
				fmt.Printf("[debug] key=%s ring returned empty replica-set\n", key)
				debugPrinted++
			}
			continue
		}

		got := false
		saw404 := false
		transportErr := false
		var lastErr error

		for _, addr := range reps {
			u := addr + "/api/string?key=" + url.QueryEscape(key)
			resp, err := httpc.Get(u)
			if err != nil {
				transportErr = true
				lastErr = err
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()

			switch resp.StatusCode {
			case nethttp.StatusOK:
				okCount++
				got = true

			case nethttp.StatusNotFound:
				saw404 = true
				lastErr = fmt.Errorf("404 %s", strings.TrimSpace(string(body)))

			case nethttp.StatusTemporaryRedirect, nethttp.StatusPermanentRedirect:
				lastErr = fmt.Errorf("%d redirect body=%s", resp.StatusCode, strings.TrimSpace(string(body)))

			default:
				lastErr = fmt.Errorf("status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
			}

			if got {
				break
			}
		}

		if !got {
			// чистый NotFound: все реплики ответили 404 и не было сетевых ошибок
			if saw404 && !transportErr && lastErr != nil && strings.HasPrefix(lastErr.Error(), "404 ") {
				notFound++
			} else {
				errCount++
			}

			if debugPrinted < debugLimit {
				fmt.Printf("[debug] key=%s reps=%v transportErr=%v lastErr=%v\n", key, reps, transportErr, lastErr)
				debugPrinted++
			}
		}
	}

	fmt.Println("\n=== RESULT ===")
	fmt.Printf("OK:        %d\n", okCount)
	fmt.Printf("NotFound:  %d\n", notFound)
	fmt.Printf("Errors:    %d\n", errCount)

	fmt.Println("\nОжидаемо при RF>=2: после остановки одной ноды большинство ключей должны остаться доступными.")
	_ = contains
}
