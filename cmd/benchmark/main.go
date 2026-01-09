package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type BenchmarkResult struct {
	TotalOps      int
	SuccessfulOps int
	FailedOps     int
	Duration      time.Duration
	OpsPerSec     float64
	AvgLatency    time.Duration
	MinLatency    time.Duration
	MaxLatency    time.Duration
}

func main() {
	baseURL := "http://localhost:8081"
	if len(os.Args) > 1 {
		baseURL = os.Args[1]
	}

	fmt.Println("=== LSMDB Benchmark Test ===")
	fmt.Printf("Target: %s\n", baseURL)
	fmt.Println()

	// Проверка доступности
	if !checkHealth(baseURL) {
		fmt.Printf("ERROR: Node %s is not available\n", baseURL)
		return
	}

	// Тест 1: Последовательные записи
	fmt.Println("Test 1: Sequential Writes (100 operations)")
	writeResult := benchmarkWrites(baseURL, 100, 1)
	printResult("Writes", writeResult)

	// Тест 2: Последовательные чтения
	fmt.Println("\nTest 2: Sequential Reads (100 operations)")
	readResult := benchmarkReads(baseURL, 100, 1)
	printResult("Reads", readResult)

	// Тест 3: Параллельные записи
	fmt.Println("\nTest 3: Concurrent Writes (100 operations, 10 goroutines)")
	concurrentWriteResult := benchmarkWrites(baseURL, 100, 10)
	printResult("Concurrent Writes", concurrentWriteResult)

	// Тест 4: Параллельные чтения
	fmt.Println("\nTest 4: Concurrent Reads (100 operations, 10 goroutines)")
	concurrentReadResult := benchmarkReads(baseURL, 100, 10)
	printResult("Concurrent Reads", concurrentReadResult)

	fmt.Println("\n=== Benchmark Complete ===")
}

func checkHealth(baseURL string) bool {
	resp, err := http.Get(baseURL + "/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func benchmarkWrites(baseURL string, totalOps, concurrency int) BenchmarkResult {
	start := time.Now()
	var wg sync.WaitGroup
	var mu sync.Mutex
	
	successful := 0
	failed := 0
	latencies := make([]time.Duration, 0, totalOps)

	opsPerGoroutine := totalOps / concurrency
	remainder := totalOps % concurrency

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			
			ops := opsPerGoroutine
			if goroutineID < remainder {
				ops++
			}

			for j := 0; j < ops; j++ {
				key := fmt.Sprintf("bench_key_%d_%d", goroutineID, j)
				value := fmt.Sprintf("bench_value_%d_%d_%d", goroutineID, j, time.Now().UnixNano())
				
				opStart := time.Now()
				err := putKey(baseURL, key, value)
				latency := time.Since(opStart)

				mu.Lock()
				if err == nil {
					successful++
				} else {
					failed++
				}
				latencies = append(latencies, latency)
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	// Вычисление статистики латентности
	var min, max, sum time.Duration
	if len(latencies) > 0 {
		min = latencies[0]
		max = latencies[0]
		for _, lat := range latencies {
			if lat < min {
				min = lat
			}
			if lat > max {
				max = lat
			}
			sum += lat
		}
	}
	avgLatency := sum / time.Duration(len(latencies))

	opsPerSec := float64(successful) / duration.Seconds()

	return BenchmarkResult{
		TotalOps:      totalOps,
		SuccessfulOps: successful,
		FailedOps:     failed,
		Duration:      duration,
		OpsPerSec:     opsPerSec,
		AvgLatency:    avgLatency,
		MinLatency:    min,
		MaxLatency:    max,
	}
}

func benchmarkReads(baseURL string, totalOps, concurrency int) BenchmarkResult {
	start := time.Now()
	var wg sync.WaitGroup
	var mu sync.Mutex
	
	successful := 0
	failed := 0
	latencies := make([]time.Duration, 0, totalOps)

	// Сначала создаём ключи для чтения
	for i := 0; i < totalOps; i++ {
		key := fmt.Sprintf("read_test_%d", i)
		putKey(baseURL, key, fmt.Sprintf("value_%d", i))
	}
	time.Sleep(500 * time.Millisecond) // Даём время на репликацию

	opsPerGoroutine := totalOps / concurrency
	remainder := totalOps % concurrency

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			
			ops := opsPerGoroutine
			if goroutineID < remainder {
				ops++
			}

			for j := 0; j < ops; j++ {
				keyIndex := goroutineID*opsPerGoroutine + j
				if goroutineID < remainder {
					keyIndex = goroutineID*(opsPerGoroutine+1) + j
				}
				key := fmt.Sprintf("read_test_%d", keyIndex)
				
				opStart := time.Now()
				_, found, err := getKey(baseURL, key)
				latency := time.Since(opStart)

				mu.Lock()
				if err == nil && found {
					successful++
				} else {
					failed++
				}
				latencies = append(latencies, latency)
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	// Вычисление статистики латентности
	var min, max, sum time.Duration
	if len(latencies) > 0 {
		min = latencies[0]
		max = latencies[0]
		for _, lat := range latencies {
			if lat < min {
				min = lat
			}
			if lat > max {
				max = lat
			}
			sum += lat
		}
	}
	avgLatency := sum / time.Duration(len(latencies))

	opsPerSec := float64(successful) / duration.Seconds()

	return BenchmarkResult{
		TotalOps:      totalOps,
		SuccessfulOps: successful,
		FailedOps:     failed,
		Duration:      duration,
		OpsPerSec:     opsPerSec,
		AvgLatency:    avgLatency,
		MinLatency:    min,
		MaxLatency:    max,
	}
}

func putKey(baseURL, key, value string) error {
	data := url.Values{}
	data.Set("key", key)
	data.Set("value", value)

	client := &http.Client{
		Timeout: 5 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// Следуем редиректам автоматически
			return nil
		},
	}

	req, err := http.NewRequest("PUT", baseURL+"/api/string", strings.NewReader(data.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Читаем тело ответа для очистки
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode == http.StatusOK {
		return nil
	}
	// Также принимаем редиректы как успех (клиент следует автоматически)
	if resp.StatusCode == http.StatusTemporaryRedirect {
		return nil
	}
	return fmt.Errorf("unexpected status: %d", resp.StatusCode)
}

func getKey(baseURL, key string) (string, bool, error) {
	encodedKey := url.QueryEscape(key)
	resp, err := http.Get(baseURL + "/api/string?key=" + encodedKey)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", false, nil
	}

	if resp.StatusCode != http.StatusOK {
		return "", false, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	var result struct {
		Status string `json:"status"`
		Value  string `json:"value"`
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", false, err
	}

	if err := json.Unmarshal(body, &result); err != nil {
		return "", false, err
	}

	return result.Value, true, nil
}

func printResult(testName string, result BenchmarkResult) {
	fmt.Printf("  Total Operations: %d\n", result.TotalOps)
	fmt.Printf("  Successful: %d\n", result.SuccessfulOps)
	fmt.Printf("  Failed: %d\n", result.FailedOps)
	fmt.Printf("  Duration: %v\n", result.Duration)
	fmt.Printf("  Operations/sec: %.2f\n", result.OpsPerSec)
	fmt.Printf("  Avg Latency: %v\n", result.AvgLatency)
	fmt.Printf("  Min Latency: %v\n", result.MinLatency)
	fmt.Printf("  Max Latency: %v\n", result.MaxLatency)
}

