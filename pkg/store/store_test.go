package store

import (
	"testing"
	"time"
)

// mockTimeProvider implements iTimeProvider for testing
type mockTimeProvider struct {
	now time.Time
}

func (m *mockTimeProvider) Now() time.Time {
	return m.now
}

func TestStore_PutString_GetString(t *testing.T) {
	// Create store
	timeProvider := &mockTimeProvider{now: time.Now()}
	store := New("/tmp/test", timeProvider)

	// Test PutString
	err := store.PutString("key1", "value1")
	if err != nil {
		t.Fatalf("PutString failed: %v", err)
	}

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
}

func TestStore_DeleteString(t *testing.T) {
	// Create store
	timeProvider := &mockTimeProvider{now: time.Now()}
	store := New("/tmp/test", timeProvider)

	// Put a value
	err := store.PutString("key1", "value1")
	if err != nil {
		t.Fatalf("PutString failed: %v", err)
	}

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

func TestStore_Overwrite(t *testing.T) {
	// Create store
	timeProvider := &mockTimeProvider{now: time.Now()}
	store := New("/tmp/test", timeProvider)

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

func TestStore_MultipleKeys(t *testing.T) {
	// Create store
	timeProvider := &mockTimeProvider{now: time.Now()}
	store := New("/tmp/test", timeProvider)

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

func TestStore_NonExistentKey(t *testing.T) {
	// Create store
	timeProvider := &mockTimeProvider{now: time.Now()}
	store := New("/tmp/test", timeProvider)

	// Try to Get non-existent key
	_, found, err := store.GetString("nonexistent")
	if err != nil {
		t.Fatalf("GetString failed: %v", err)
	}
	if found {
		t.Fatal("Expected key to not exist")
	}
}
