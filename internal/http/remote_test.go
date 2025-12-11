package http

import (
	"testing"
)

func TestRemoteStore_PutString_GetString(t *testing.T) {
	store := NewRemoteStore("http://localhost:8081")

	// Test PutString
	err := store.PutString("key1", "value1")
	if err != nil {
		t.Fatalf("PutString failed: %v", err)
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"lsmdb/pkg/config"
	"lsmdb/pkg/store"
	"lsmdb/pkg/wal"
)

func TestRemoteAPI(t *testing.T) {
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "lsmdb-remote-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create db
	cfg := config.Default()
	cfg.Persistence.RootPath = tempDir
	journal, err := wal.New(cfg.Persistence.RootPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer journal.Close()
	db, err := store.New(&cfg, journal)
	if err != nil {
		t.Fatalf("Failed to create db: %v", err)
	}
	node := &fakeRaftNode{store: db}

	// Test GetString
	value, found, err := store.GetString("key1")
	if err != nil {
		t.Fatalf("GetString failed: %v", err)
	}
	if !found {
		t.Fatal("Expected to find key1")
	}
	if value != "value1" {
		t.Fatalf("Expected 'value1', got '%s'", value)
	}
	// Create server
	server := NewServer(node, "8081")
	// attach DB to server before starting to avoid races (handler may be called immediately)
	server.store = db
	if err = server.Start(); err != nil {
		t.Fatal(err)
	}
	//nolint:errcheck
	defer server.Stop()

	// wait for server start
	time.Sleep(100 * time.Millisecond)

	// Test data
	testKey := "remote_test_key"
	testValue := "remote_test_value"

	t.Run("PUT operation", func(t *testing.T) {
		// Prepare form data
		formData := fmt.Sprintf("key=%s&value=%s", testKey, testValue)

		// Make PUT request
		req, err := http.NewRequest(
			http.MethodPut,
			server.URL+"/api/string",
			bytes.NewBufferString(formData),
		)
		if err != nil {
			t.Fatalf("PUT request failed: %v", err)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("PUT request failed: %v", err)
		}

		defer resp.Body.Close()

		// Check response
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("PUT failed with status %d: %s", resp.StatusCode, string(body))
		}

		// Check response body
		var result map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if result["status"] != "success" {
			t.Fatalf("Expected success status, got: %s", result["status"])
		}
	})

	t.Run("GET operation", func(t *testing.T) {
		// Make GET request
		resp, err := http.Get(server.URL + "/api/string?key=" + testKey)
		if err != nil {
			t.Fatalf("GET request failed: %v", err)
		}
		defer resp.Body.Close()

		// Check response
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("GET failed with status %d: %s", resp.StatusCode, string(body))
		}

		// Check response body
		var result map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if result["value"] != testValue {
			t.Fatalf("Expected value '%s', got: '%s'", testValue, result["value"])
		}
	})

	t.Run("DELETE operation", func(t *testing.T) {
		// Make DELETE request
		req, err := http.NewRequest("DELETE", server.URL+"/api?key="+testKey, nil)
		if err != nil {
			t.Fatalf("Failed to create DELETE request: %v", err)
		}

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("DELETE request failed: %v", err)
		}
		defer resp.Body.Close()

		// Check response
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("DELETE failed with status %d: %s", resp.StatusCode, string(body))
		}

		// Check response body
		var result map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if result["status"] != "success" {
			t.Fatalf("Expected success status, got: %s", result["status"])
		}
	})

	t.Run("GET after DELETE", func(t *testing.T) {
		// Try to get deleted key
		resp, err := http.Get(server.URL + "/api/string?key=" + testKey)
		if err != nil {
			t.Fatalf("GET request failed: %v", err)
		}
		defer resp.Body.Close()

		// Should return 404
		if resp.StatusCode != http.StatusNotFound {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected 404 after delete, got status %d: %s", resp.StatusCode, string(body))
		}
	})

	t.Run("Health check", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/health")
		if err != nil {
			t.Fatalf("Health check failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Health check failed with status %d", resp.StatusCode)
		}

		// Expect JSON response: {"status":"OK"}
		var result map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("Failed to decode health response JSON: %v", err)
		}
		if result["status"] != string(StatusOK) {
			t.Fatalf("Expected status %s, got: %s", StatusOK, result["status"])
		}
	})
}

func TestRemoteStore_DeleteString(t *testing.T) {
	store := NewRemoteStore("http://localhost:8081")

<<<<<<< HEAD:pkg/rpc/remote_test.go
	// Put a value
	err := store.PutString("key1", "value1")
=======
	// Create store
	cfg := config.Default()
	cfg.Persistence.RootPath = tempDir
	// Ensure Raft config is initialized for tests
	cfg.Raft.ID = 1
	cfg.Raft.Peers = []config.RaftPeerConfig{{ID: 1, Address: "http://localhost:8081"}}
	journal, err := wal.New(cfg.Persistence.RootPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer journal.Close()
	db, err := store.New(&cfg, journal)
>>>>>>> origin/lab4:internal/http/remote_test.go
	if err != nil {
		t.Fatalf("PutString failed: %v", err)
	}
	node := &fakeRaftNode{store: db}

<<<<<<< HEAD:pkg/rpc/remote_test.go
	// Verify it exists
	value, found, err := store.GetString("key1")
	if err != nil {
		t.Fatalf("GetString failed: %v", err)
	}
	if !found || value != "value1" {
		t.Fatal("Expected to find key1 with value1")
	}

	// Delete it
	err = store.Delete("key1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it's deleted
	value, found, err = store.GetString("key1")
	if err != nil {
		t.Fatalf("GetString after delete failed: %v", err)
	}
	if found {
		t.Fatalf("Expected key1 to be deleted, but found value: %s", value)
	}
}

func TestRemoteStore_Overwrite(t *testing.T) {
	store := NewRemoteStore("http://localhost:8081")

	// Put initial value
	err := store.PutString("key1", "value1")
	if err != nil {
		t.Fatalf("PutString failed: %v", err)
	}

	// Overwrite with new value
	err = store.PutString("key1", "value2")
	if err != nil {
		t.Fatalf("PutString overwrite failed: %v", err)
	}

	// Verify new value
	value, found, err := store.GetString("key1")
	if err != nil {
		t.Fatalf("GetString failed: %v", err)
	}
	if !found {
		t.Fatal("Expected to find key1")
	}
	if value != "value2" {
		t.Fatalf("Expected 'value2', got '%s'", value)
	}
}

func TestRemoteStore_MultipleKeys(t *testing.T) {
	store := NewRemoteStore("http://localhost:8081")

	// Put multiple keys
	keys := []string{"key1", "key2", "key3"}
	values := []string{"value1", "value2", "value3"}

	for i, key := range keys {
		err := store.PutString(key, values[i])
		if err != nil {
			t.Fatalf("PutString failed for %s: %v", key, err)
		}
	}

	// Verify all keys
	for i, key := range keys {
		value, found, err := store.GetString(key)
		if err != nil {
			t.Fatalf("GetString failed for %s: %v", key, err)
		}
		if !found {
			t.Fatalf("Expected to find %s", key)
		}
		if value != values[i] {
			t.Fatalf("Expected '%s' for %s, got '%s'", values[i], key, value)
		}
	}
}

func TestRemoteStore_NonExistentKey(t *testing.T) {
	store := NewRemoteStore("http://localhost:8081")

	// Try to Get non-existent key
	_, found, err := store.GetString("nonexistent")
	if err != nil {
		t.Fatalf("GetString failed: %v", err)
	}
	if found {
		t.Fatal("Expected key to not exist")
	}
=======
	// Create server
	server := NewServer(node, "8081")
	if err = server.Start(); err != nil {
		t.Fatal(err)
	}
	//nolint:errcheck
	defer server.Stop()

	t.Run("PUT without key", func(t *testing.T) {
		formData := "value=test"
		// Make PUT request
		req, err := http.NewRequest(
			http.MethodPut,
			server.URL+"/api/string",
			bytes.NewBufferString(formData),
		)
		if err != nil {
			t.Fatalf("PUT request failed: %v", err)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("PUT request failed: %v", err)
		}

		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("Expected 400 for missing key, got: %d", resp.StatusCode)
		}
	})

	t.Run("GET without key", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/api/string")
		if err != nil {
			t.Fatalf("GET request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("Expected 400 for missing key, got: %d", resp.StatusCode)
		}
	})

	t.Run("GET non-existent key", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/api/get?key=nonexistent")
		if err != nil {
			t.Fatalf("GET request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("Expected 404 for non-existent key, got: %d", resp.StatusCode)
		}
	})
>>>>>>> origin/lab4:internal/http/remote_test.go
}
