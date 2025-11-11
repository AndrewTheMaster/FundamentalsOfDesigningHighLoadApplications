package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type HTTPRemote struct {
	baseURL string
	client  *http.Client
}

func NewHTTPRemote(baseURL string) *HTTPRemote {
	return &HTTPRemote{
		baseURL: baseURL,
		client:  http.DefaultClient,
	}
}

type getResp struct {
	Value string `json:"value"`
}

func (s *HTTPRemote) Put(ctx context.Context, key, value string) error {
	form := url.Values{}
	form.Set("key", key)
	form.Set("value", value)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.baseURL+"/api/put", io.NopCloser(strings.NewReader(form.Encode())))
	if err != nil {
		return fmt.Errorf("create PUT request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("PUT do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("PUT failed: %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

func (s *HTTPRemote) Get(ctx context.Context, key string) (string, bool, error) {
	u := fmt.Sprintf("%s/api/get?key=%s", s.baseURL, url.QueryEscape(key))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", false, fmt.Errorf("create GET request: %w", err)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return "", false, fmt.Errorf("GET do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", false, nil
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return "", false, fmt.Errorf("GET failed: %d: %s", resp.StatusCode, string(b))
	}

	var gr getResp
	if err := json.NewDecoder(resp.Body).Decode(&gr); err != nil {
		return "", false, fmt.Errorf("decode GET body: %w", err)
	}
	return gr.Value, true, nil
}

func (s *HTTPRemote) Delete(ctx context.Context, key string) error {
	u := fmt.Sprintf("%s/api/delete?key=%s", s.baseURL, url.QueryEscape(key))
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return fmt.Errorf("create DELETE request: %w", err)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("DELETE do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("DELETE failed: %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

func (s *HTTPRemote) Close() error { return nil }
