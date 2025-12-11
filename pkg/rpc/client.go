package rpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// RemoteStore — простой HTTP-клиент к REST API другой ноды.
type RemoteStore struct {
	baseURL string
	client  *http.Client
}

// NewRemoteStore создаёт новый клиент для базового URL, например "http://node1:8080".
func NewRemoteStore(baseURL string) *RemoteStore {
	baseURL = strings.TrimRight(baseURL, "/")
	return &RemoteStore{
		baseURL: baseURL,
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (r *RemoteStore) PutString(key, value string) error {
	form := url.Values{}
	form.Set("key", key)
	form.Set("value", value)

	resp, err := r.client.Post(
		r.baseURL+"/api/put",
		"application/x-www-form-urlencoded",
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		return fmt.Errorf("remote put: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("remote put: status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (r *RemoteStore) GetString(key string) (string, bool, error) {
	req, err := http.NewRequest(http.MethodGet, r.baseURL+"/api/get?key="+url.QueryEscape(key), nil)
	if err != nil {
		return "", false, fmt.Errorf("remote get: %w", err)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return "", false, fmt.Errorf("remote get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", false, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", false, fmt.Errorf("remote get: status %d: %s", resp.StatusCode, string(body))
	}

	var payload struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		body, _ := io.ReadAll(resp.Body)
		return "", false, fmt.Errorf("remote get: decode: %w (body=%q)", err, string(body))
	}

	return payload.Value, true, nil
}

func (r *RemoteStore) Delete(key string) error {
	req, err := http.NewRequest(
		http.MethodDelete,
		r.baseURL+"/api/delete?key="+url.QueryEscape(key),
		bytes.NewReader(nil),
	)
	if err != nil {
		return fmt.Errorf("remote delete: %w", err)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("remote delete: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("remote delete: status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}
