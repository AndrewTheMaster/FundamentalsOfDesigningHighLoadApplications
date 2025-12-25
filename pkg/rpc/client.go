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

type HTTPStore struct {
	baseURL string
	client  *http.Client
}

type ValueResponse struct {
	Value string `json:"value"`
}

func NewHTTPStore(baseURL string) *HTTPStore {
	return &HTTPStore{
		baseURL: strings.TrimRight(baseURL, "/"),
		client: &http.Client{
			Timeout: 3 * time.Second,
			// важно: разрешаем редиректы (307/302) — default ок,
			// но делаем явным, чтобы можно было менять позже.
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return nil
			},
		},
	}
}

func (s *HTTPStore) PutString(key, value string) error {
	form := url.Values{}
	form.Set("key", key)
	form.Set("value", value)

	req, err := http.NewRequest(http.MethodPut, s.baseURL+"/api/string", bytes.NewBufferString(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("PUT failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("PUT status=%d body=%s", resp.StatusCode, string(b))
	}
	return nil
}

func (s *HTTPStore) GetString(key string) (string, bool, error) {
	u := s.baseURL + "/api/string?key=" + url.QueryEscape(key)
	resp, err := s.client.Get(u)
	if err != nil {
		return "", false, fmt.Errorf("GET failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", false, nil
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return "", false, fmt.Errorf("GET status=%d body=%s", resp.StatusCode, string(b))
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", false, err
	}

	var vr ValueResponse
	if err := json.Unmarshal(b, &vr); err != nil {
		return "", false, fmt.Errorf("decode: %w body=%s", err, string(b))
	}
	return vr.Value, true, nil
}

func (s *HTTPStore) Delete(key string) error {
	u := s.baseURL + "/api?key=" + url.QueryEscape(key)
	req, err := http.NewRequest(http.MethodDelete, u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("DELETE failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("DELETE status=%d body=%s", resp.StatusCode, string(b))
	}
	return nil
}
