package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// simple in-memory fake store implementing iStoreAPI
type fakeStore struct {
	mu sync.RWMutex
	m  map[string]string
}

func newFakeStore() *fakeStore {
	return &fakeStore{m: make(map[string]string)}
}

func (f *fakeStore) PutString(key, value string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[key] = value
	return nil
}

func (f *fakeStore) GetString(key string) (string, bool, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	v, ok := f.m[key]
	return v, ok, nil
}

func (f *fakeStore) Delete(key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.m, key)
	return nil
}

func decodeResp(t *testing.T, rr *httptest.ResponseRecorder) Response {
	t.Helper()
	var resp Response
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response JSON: %v, body=%s", err, rr.Body.String())
	}
	return resp
}

func TestHealthHandler(t *testing.T) {
	s := NewServer(newFakeStore(), "")
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()

	s.createRouter().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	resp := decodeResp(t, rr)
	if resp.Status != StatusOK {
		t.Fatalf("expected status %s, got %s", StatusOK, resp.Status)
	}
}

func TestPutGetDeleteFlow(t *testing.T) {
	store := newFakeStore()
	s := NewServer(store, "")

	// PUT
	form := url.Values{}
	form.Set("key", "foo")
	form.Set("value", "bar")
	req := httptest.NewRequest(http.MethodPut, "/api/string", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	s.createRouter().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("put: expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if resp := decodeResp(t, rr); resp.Status != StatusSuccess {
		t.Fatalf("put: expected status %s, got %s", StatusSuccess, resp.Status)
	}

	// GET
	req = httptest.NewRequest(http.MethodGet, "/api/string?key=foo", nil)
	rr = httptest.NewRecorder()
	s.createRouter().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("get: expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	resp := decodeResp(t, rr)
	if resp.Value != "bar" {
		t.Fatalf("get: expected value 'bar', got '%s'", resp.Value)
	}

	// DELETE
	req = httptest.NewRequest(http.MethodDelete, "/api?key=foo", nil)
	rr = httptest.NewRecorder()
	s.createRouter().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("delete: expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if resp = decodeResp(t, rr); resp.Status != StatusSuccess {
		t.Fatalf("delete: expected status %s, got %s", StatusSuccess, resp.Status)
	}

	// GET after delete -> 404
	req = httptest.NewRequest(http.MethodGet, "/api/string?key=foo", nil)
	rr = httptest.NewRecorder()
	s.createRouter().ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("get-after-delete: expected 404, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestMissingParamsAndMethodNotAllowed(t *testing.T) {
	s := NewServer(newFakeStore(), "")

	// PUT missing params
	req := httptest.NewRequest(http.MethodPut, "/api/string", strings.NewReader(""))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	s.createRouter().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("put-missing: expected 400, got %d body=%s", rr.Code, rr.Body.String())
	}

	// GET missing key
	req = httptest.NewRequest(http.MethodGet, "/api/string", nil)
	rr = httptest.NewRecorder()
	s.createRouter().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("get-missing: expected 400, got %d body=%s", rr.Code, rr.Body.String())
	}

	// DELETE missing key
	req = httptest.NewRequest(http.MethodDelete, "/api", nil)
	rr = httptest.NewRecorder()
	s.createRouter().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("delete-missing: expected 400, got %d body=%s", rr.Code, rr.Body.String())
	}

	// Method not allowed: POST to /health
	req = httptest.NewRequest(http.MethodPost, "/health", nil)
	rr = httptest.NewRecorder()
	s.createRouter().ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("method-not-allowed: expected 405, got %d body=%s", rr.Code, rr.Body.String())
	}
}
