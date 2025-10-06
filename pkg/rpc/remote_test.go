package rpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"lsmdb/pkg/store"
)

// mockTimeProvider for testing
type mockTimeProvider struct {
	now time.Time
}

func (m *mockTimeProvider) Now() time.Time {
	return m.now
}

func TestRemoteAPI(t *testing.T) {
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "lsmdb-remote-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create store
	timeProvider := &mockTimeProvider{now: time.Now()}
	store := store.New(tempDir, timeProvider)

	// Create server
	server := NewServer(store, "8081")

	// Test HTTP server
	httpServer := httptest.NewServer(server.createHTTPHandler())
	defer httpServer.Close()

	// Test data
	testKey := "remote_test_key"
	testValue := "remote_test_value"

	t.Run("PUT operation", func(t *testing.T) {
		// Prepare form data
		formData := fmt.Sprintf("key=%s&value=%s", testKey, testValue)

		// Make PUT request
		resp, err := http.Post(httpServer.URL+"/api/put", "application/x-www-form-urlencoded", bytes.NewBufferString(formData))
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
		resp, err := http.Get(httpServer.URL + "/api/get?key=" + testKey)
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
		req, err := http.NewRequest("DELETE", httpServer.URL+"/api/delete?key="+testKey, nil)
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
		resp, err := http.Get(httpServer.URL + "/api/get?key=" + testKey)
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
		resp, err := http.Get(httpServer.URL + "/health")
		if err != nil {
			t.Fatalf("Health check failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Health check failed with status %d", resp.StatusCode)
		}

		body, _ := io.ReadAll(resp.Body)
		if string(body) != "OK" {
			t.Fatalf("Expected 'OK', got: %s", string(body))
		}
	})
}

func TestRemoteAPIErrorHandling(t *testing.T) {
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "lsmdb-remote-test-errors")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create store
	timeProvider := &mockTimeProvider{now: time.Now()}
	store := store.New(tempDir, timeProvider)

	// Create server
	server := NewServer(store, "8081")

	// Test HTTP server
	httpServer := httptest.NewServer(server.createHTTPHandler())
	defer httpServer.Close()

	t.Run("PUT without key", func(t *testing.T) {
		formData := "value=test"
		resp, err := http.Post(httpServer.URL+"/api/put", "application/x-www-form-urlencoded", bytes.NewBufferString(formData))
		if err != nil {
			t.Fatalf("PUT request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("Expected 400 for missing key, got: %d", resp.StatusCode)
		}
	})

	t.Run("GET without key", func(t *testing.T) {
		resp, err := http.Get(httpServer.URL + "/api/get")
		if err != nil {
			t.Fatalf("GET request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("Expected 400 for missing key, got: %d", resp.StatusCode)
		}
	})

	t.Run("GET non-existent key", func(t *testing.T) {
		resp, err := http.Get(httpServer.URL + "/api/get?key=nonexistent")
		if err != nil {
			t.Fatalf("GET request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("Expected 404 for non-existent key, got: %d", resp.StatusCode)
		}
	})
}
