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
	default:
		log.Printf("unsupported method: %s\n", method)
		return
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
	fmt.Println()
	fmt.Println(msg)
	fmt.Print("Нажми Enter, чтобы продолжить...")
	_, _ = bufio.NewReader(os.Stdin).ReadBytes('\n')
}

// статическое размещение шардов по нодам для демонстрации:
// 3 ноды, RF=2
func shardOwners(shardID int) []string {
	nodes := []string{"node1:8080", "node2:8080", "node3:8080"}
	const replicas = 2

	res := make([]string, 0, replicas)
	for i := 0; i < replicas; i++ {
		idx := (shardID + i) % len(nodes)
		res = append(res, nodes[idx])
	}
	return res
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: demo http://node1:8080")
		os.Exit(1)
	}

	base := os.Args[1]

	fmt.Println("=== БАЗОВАЯ ПРОВЕРКА API (без шардирования) ===")
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

	// --- шардирование + репликация ---
	const totalKeys = 100
	const totalShards = 4 // должно совпадать с конфигом кластера

	fmt.Printf("\n=== [ШАГ 1] вставляем %d тестовых ключей (для проверки шардирования/репликации) ===\n", totalKeys)

	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("val-%d", i)
		call("put", base, key, val)
	}

	// --- Consistent Hashing по ШАРДАМ ---
	fmt.Println("\n=== [ШАГ 2] Consistent Hashing по логическим shardID ===")
	shardRing := cluster.NewHashRing(100)
	for shard := 0; shard < totalShards; shard++ {
		shardName := fmt.Sprintf("shard-%d", shard)
		shardRing.AddNode(shardName)
	}

	shardCounts := make(map[int]int)

	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key-%d", i)

		shardID, err := cluster.ShardFromRing(shardRing, key)
		if err != nil {
			fmt.Printf("  key=%s: ShardFromRing error: %v\n", key, err)
			continue
		}

		shardCounts[int(shardID)]++
	}

	for shard := 0; shard < totalShards; shard++ {
		owners := shardOwners(shard)
		fmt.Printf("  shard-%d → %d keys; реплики: %v\n", shard, shardCounts[shard], owners)
	}

	pause(`=== [ШАГ 3] ТЕСТ РЕПЛИКАЦИИ ===
Сейчас каждый логический shard реплицируется на несколько нод.
1) Останови ОДНУ из нод, например:
   docker compose stop node3
2) Подожди, пока ZooKeeper удалит node3 из /lsmdb/nodes,
   а живые ноды перестроят кольцо и/или выберут нового лидера Raft-группы.
После этого проверим, что данные всё ещё доступны благодаря репликам.`)

	fmt.Println("\n=== [ШАГ 4] проверяем доступность ключей после падения ноды ===")

	// маленькая sanity-проверка
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

	fmt.Printf("\n=== РЕЗЮМЕ ПОСЛЕ ПАДЕНИЯ НОДЫ (шардирование + репликация) ===\n")
	fmt.Printf("  OK (ключ найден):      %d\n", okCount)
	fmt.Printf("  NOT FOUND (потерян):   %d\n", notFoundCount)
	fmt.Printf("  ERR (другая ошибка):   %d\n", errCount)
	fmt.Println("Если репликация и Raft/placement настроены корректно, NOT FOUND должно быть 0 💚")
}
