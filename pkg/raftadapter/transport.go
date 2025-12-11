package raftadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

const (
	raftEndpoint     = "/api/internal/raft"
	transportTimeout = 3 * time.Second
	maxRetries       = 3
	retryDelay       = 100 * time.Millisecond
)

type Transport struct {
	peersMu    sync.RWMutex
	peers      map[uint64]string
	httpClient *http.Client
}

func NewTransport(peers map[uint64]string) *Transport {
	return &Transport{
		peers: peers,
		httpClient: &http.Client{
			Timeout: transportTimeout,
		},
	}
}

func (t *Transport) AddPeer(nodeID uint64, addr string) {
	t.peersMu.Lock()
	defer t.peersMu.Unlock()
	t.peers[nodeID] = addr
}

func (t *Transport) RemovePeer(nodeID uint64) {
	t.peersMu.Lock()
	defer t.peersMu.Unlock()
	delete(t.peers, nodeID)
}

func (t *Transport) UpdatePeer(nodeID uint64, addr string) {
	t.peersMu.Lock()
	defer t.peersMu.Unlock()
	t.peers[nodeID] = addr
}

func (t *Transport) Send(msg raftpb.Message) error {
	t.peersMu.RLock()
	targetAddr, ok := t.peers[msg.To]
	t.peersMu.RUnlock()
	if !ok {
		return fmt.Errorf("unknown peer node: %d", msg.To)
	}

	url := targetAddr + raftEndpoint

	// Сериализация сообщения
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	// Retry логика для сетевых ошибок
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := t.sendHTTP(url, body); err != nil {
			lastErr = err
			slog.Warn("failed to send raft message, retrying",
				"attempt", attempt+1,
				"to", msg.To,
				"type", msg.Type,
				"error", err)
			time.Sleep(retryDelay * time.Duration(attempt+1))
			continue
		}
		return nil
	}

	return fmt.Errorf("failed to send after %d retries: %w", maxRetries, lastErr)
}

func (t *Transport) sendHTTP(url string, body []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), transportTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}
