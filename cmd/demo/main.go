package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"

	"lsmdb/pkg/cluster"
)

func call(method, base, key, value string) {
	endpoint := base + "/api/" + method

	var resp *http.Response
	var err error

	switch method {
	case "put":
		fmt.Printf("[client] PUT    key=%s value=%s → %s\n", key, value, base)
		resp, err = http.PostForm(endpoint, url.Values{"key": {key}, "value": {value}})
	case "get":
		fmt.Printf("[client] GET    key=%s → %s\n", key, base)
		resp, err = http.Get(endpoint + "?key=" + url.QueryEscape(key))
	case "delete":
		fmt.Printf("[client] DELETE key=%s → %s\n", key, base)
		req, _ := http.NewRequest(http.MethodDelete, endpoint+"?key="+url.QueryEscape(key), nil)
		resp, err = http.DefaultClient.Do(req)
	}

	if err != nil {
		log.Println(method, "error:", err)
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("[client] RESPONSE: %s\n", body)
}

func pause(msg string) {
	fmt.Println(msg)
	fmt.Print("Нажми Enter, чтобы продолжить...")
	bufio.NewReader(os.Stdin).ReadBytes('\n')
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: demo http://node1:8080")
		os.Exit(1)
	}

	base := os.Args[1]

	call("put", base, "user:1", "Alice")
	call("put", base, "user:2", "Bob")
	call("put", base, "user:3", "Brioshe")
	call("put", base, "config:timeout", "30s")

	call("get", base, "user:1", "")
	call("get", base, "user:2", "")

	call("put", base, "user:1", "Alice Updated")
	call("get", base, "user:1", "")

	call("delete", base, "user:2", "")
	call("get", base, "user:2", "")

	// --- сценарий для теста падения ноды ---
	const totalKeys = 100

	fmt.Printf("\n=== [ШАГ 1] вставляем %d тестовых ключей ===\n", totalKeys)

	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("val-%d", i)
		call("put", base, key, val)
	}

	// --- распределение по кольцу до падения ноды ---
	fmt.Println("\n=== [ШАГ 2] распределение по кольцу при 3 нодах ===")

	ring3 := cluster.NewHashRing(100)
	nodes3 := []string{"node1:8080", "node2:8080", "node3:8080"}
	for _, n := range nodes3 {
		ring3.AddNode(n)
	}

	counts3 := make(map[string]int)
	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		node, ok := ring3.GetNode(key)
		if !ok {
			continue
		}
		counts3[node]++
	}

	for _, n := range ring3.ListNodes() {
		fmt.Printf("  %s → %d keys\n", n, counts3[n])
	}

	// --- убиваем одну ноду ---
	pause(` === [ШАГ 3] Останови одну из нод, например lsmdb-node3 ===
docker compose stop node3
После этого ZooKeeper уберёт node3:8080 из /lsmdb/nodes,
живые ноды перестроят кольцо без node3.`)

	// // --- распределение по кольцу после "падения" node2 ---
	// fmt.Println("\n=== [ШАГ 4] распределение по кольцу при 2 нодах (node2 убрана) ===")

	// ring2 := cluster.NewHashRing(100)
	// nodes2 := []string{"node1:8080", "node3:8080"}
	// for _, n := range nodes2 {
	// 	ring2.AddNode(n)
	// }

	// counts2 := make(map[string]int)
	// for i := 0; i < totalKeys; i++ {
	// 	key := fmt.Sprintf("key-%d", i)
	// 	node, ok := ring2.GetNode(key)
	// 	if !ok {
	// 		continue
	// 	}
	// 	counts2[node]++
	// }

	// for _, n := range ring2.ListNodes() {
	// 	fmt.Printf("  %s → %d keys\n", n, counts2[n])
	// }

	fmt.Println("\n=== [ШАГ 4] проверяем доступность ключей после падения ноды ===")

	call("get", base, "user:1", "")

	var okCount, notFoundCount, errCount int

	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		resp, err := http.Get(base + "/api/get?key=" + url.QueryEscape(key))
		if err != nil {
			fmt.Printf("[check] key=%s ERROR: %v\n", key, err)
			errCount++
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusOK:
			okCount++
		case http.StatusNotFound:
			notFoundCount++
		default:
			errCount++
			fmt.Printf("[check] key=%s status=%d body=%s\n", key, resp.StatusCode, body)
		}
	}

	fmt.Printf("\n=== РЕЗЮМЕ ПОСЛЕ ПАДЕНИЯ НОДЫ ===\n")
	fmt.Printf("  OK (ключ найден):      %d\n", okCount)
	fmt.Printf("  NOT FOUND (потерян):   %d\n", notFoundCount)
	fmt.Printf("  ERR (другая ошибка):   %d\n", errCount)
}
