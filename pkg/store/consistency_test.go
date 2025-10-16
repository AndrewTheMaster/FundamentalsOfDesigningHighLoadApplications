package store

import (
	"fmt"
	"testing"
	"time"
)

// TestDataConsistency tests data consistency across operations
func TestDataConsistency(t *testing.T) {
	tempDir := t.TempDir()

	// Create store
	timeProvider := &mockTimeProvider{now: time.Now()}
	store := New(tempDir, timeProvider)

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
		err := store.DeleteString("key1")
		if err != nil {
			t.Fatalf("DeleteString failed: %v", err)
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
	tempDir := t.TempDir()

	// Create first store instance
	timeProvider1 := &mockTimeProvider{now: time.Now()}
	store1 := New(tempDir, timeProvider1)

	// Write data
	err := store1.PutString("persistent_key", "persistent_value")
	if err != nil {
		t.Fatalf("PutString failed: %v", err)
	}

	// Close first store
	// (In real implementation, we would close the store properly)

	// Create second store instance (simulating restart)
	timeProvider2 := &mockTimeProvider{now: time.Now()}
	store2 := New(tempDir, timeProvider2)

	// Read data from second store
	value, found, err := store2.GetString("persistent_key")
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
	tempDir := t.TempDir()

	// Create store
	timeProvider := &mockTimeProvider{now: time.Now()}
	store := New(tempDir, timeProvider)

	// Test concurrent writes to different keys
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			key := fmt.Sprintf("concurrent_key_%d", id)
			value := fmt.Sprintf("concurrent_value_%d", id)

			err := store.PutString(key, value)
			if err != nil {
				t.Logf("Concurrent PutString failed: %v", err)
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
	tempDir := t.TempDir()

	// Create store
	timeProvider := &mockTimeProvider{now: time.Now()}
	store := New(tempDir, timeProvider)

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
