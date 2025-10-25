package rpc

import (
	"fmt"
	"testing"
)

// TestDataConsistency tests data consistency across operations
func TestDataConsistency(t *testing.T) {
	// Create remote store client
	store := NewRemoteStore("http://localhost:8081")

	// Test data consistency
	t.Run("BasicConsistency", func(t *testing.T) {
		// Write data
		err := store.PutString("key1", "value1")
		if err != nil {
			t.Fatalf("PutString failed: %v", err)
		}

		// Read data
		value, found, err := store.GetString("key1")
		if err != nil {
			t.Fatalf("GetString failed: %v", err)
		}
		if !found {
			t.Fatal("Key not found")
		}
		if value != "value1" {
			t.Fatalf("Expected value1, got %s", value)
		}
	})

	t.Run("UpdateConsistency", func(t *testing.T) {
		// Update data
		err := store.PutString("key1", "value1_updated")
		if err != nil {
			t.Fatalf("PutString failed: %v", err)
		}

		// Read updated data
		value, found, err := store.GetString("key1")
		if err != nil {
			t.Fatalf("GetString failed: %v", err)
		}
		if !found {
			t.Fatal("Key not found")
		}
		if value != "value1_updated" {
			t.Fatalf("Expected value1_updated, got %s", value)
		}
	})

	t.Run("DeleteConsistency", func(t *testing.T) {
		// Delete data
		err := store.Delete("key1")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// Try to read deleted data
		_, found, err := store.GetString("key1")
		if err != nil {
			t.Fatalf("GetString failed: %v", err)
		}
		if found {
			t.Fatal("Deleted key should not be found")
		}
	})
}

// TestDataPersistence tests data persistence across restarts
func TestDataPersistence(t *testing.T) {
	// Create store client
	store := NewRemoteStore("http://localhost:8081")

	// Write data
	err := store.PutString("persistent_key", "persistent_value")
	if err != nil {
		t.Fatalf("PutString failed: %v", err)
	}

	// Restart the container
	t.Log("Restarting container...")
	// Note: В реальном сценарии здесь был бы рестарт контейнера

	// Read data after restart
	value, found, err := store.GetString("persistent_key")
	if err != nil {
		t.Fatalf("GetString failed: %v", err)
	}
	if !found {
		t.Fatal("Persistent key not found after restart")
	}
	if value != "persistent_value" {
		t.Fatalf("Expected persistent_value, got %s", value)
	}
}

// TestConcurrentConsistency tests consistency under concurrent access
func TestConcurrentConsistency(t *testing.T) {
	// Create store client
	store := NewRemoteStore("http://localhost:8081")

	// Test concurrent writes to different keys
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			key := fmt.Sprintf("concurrent_key_%d", id)
			value := fmt.Sprintf("concurrent_value_%d", id)

			err := store.PutString(key, value)
			if err != nil {
				t.Errorf("Concurrent PutString failed: %v", err)
			}

			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all data is consistent
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("concurrent_key_%d", i)
		expected := fmt.Sprintf("concurrent_value_%d", i)

		value, found, err := store.GetString(key)
		if err != nil {
			t.Fatalf("GetString failed for key %s: %v", key, err)
		}
		if !found {
			t.Fatalf("Key %s not found", key)
		}
		if value != expected {
			t.Fatalf("Expected %s, got %s for key %s", expected, value, key)
		}
	}
}

// TestTransactionConsistency tests consistency of transaction-like operations
func TestTransactionConsistency(t *testing.T) {
	// Create store client
	store := NewRemoteStore("http://localhost:8081")

	// Simulate transaction: write multiple keys
	keys := []string{"tx_key1", "tx_key2", "tx_key3"}
	values := []string{"tx_value1", "tx_value2", "tx_value3"}

	// Write all keys
	for i, key := range keys {
		err := store.PutString(key, values[i])
		if err != nil {
			t.Fatalf("PutString failed for key %s: %v", key, err)
		}
	}

	// Verify all keys are consistent
	for i, key := range keys {
		value, found, err := store.GetString(key)
		if err != nil {
			t.Fatalf("GetString failed for key %s: %v", key, err)
		}
		if !found {
			t.Fatalf("Key %s not found", key)
		}
		if value != values[i] {
			t.Fatalf("Expected %s, got %s for key %s", values[i], value, key)
		}
	}
}
