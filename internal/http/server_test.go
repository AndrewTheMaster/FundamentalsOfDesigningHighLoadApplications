//nolint:hugeParam // test only
package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"lsmdb/pkg/raftadapter"
	"lsmdb/pkg/store"

	"go.etcd.io/etcd/raft/v3/raftpb"
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

type iTestStoreAPI interface {
	PutString(key, value string) error
	GetString(key string) (string, bool, error)
	Delete(key string) error
}

// fakeRaftNode implements iRaftNode minimally for tests and can apply to a provided store
type fakeRaftNode struct {
	store iTestStoreAPI
}

func (n *fakeRaftNode) IsLeader() bool     { return true }
func (n *fakeRaftNode) LeaderAddr() string { return "" }
func (n *fakeRaftNode) Execute(ctx context.Context, cmd raftadapter.Cmd) error {
	switch cmd.Op {
	case store.InsertOp:
		if n.store != nil {
			return n.store.PutString(string(cmd.Key), string(cmd.Value))
		}
	case store.DeleteOp:
		if n.store != nil {
			return n.store.Delete(string(cmd.Key))
		}
	}
	return nil
}
func (n *fakeRaftNode) Handle(ctx context.Context, message raftpb.Message) error { return nil }
func (n *fakeRaftNode) Run(ctx context.Context) error                            { return nil }
func (n *fakeRaftNode) Stop() error                                              { return nil }

func decodeResp(t *testing.T, rr *httptest.ResponseRecorder) Response {
	t.Helper()
	var resp Response
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response JSON: %v, body=%s", err, rr.Body.String())
	}
	return resp
}

func TestHealthHandler(t *testing.T) {
	node := &fakeRaftNode{}
	s := NewServer(node, "")
	// provide store for GET handler
	s.store = newFakeStore()
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
	node := &fakeRaftNode{store: store}
	s := NewServer(node, "")
	// attach store to server so GET works
	s.store = store

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
	node := &fakeRaftNode{}
	s := NewServer(node, "")
	// attach store for GET
	s.store = newFakeStore()

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
