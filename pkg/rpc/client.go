package rpc

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// RemoteStore представляет клиент для работы с удаленным хранилищем через HTTP
type RemoteStore struct {
	baseURL string
}

var httpClient = &http.Client{
	Timeout: 2 * time.Second,
}

// Response представляет ответ от сервера
type Response struct {
	Value string `json:"value"`
}

// NewRemoteStore создает новый клиент для работы с удаленным хранилищем
func NewRemoteStore(baseURL string) *RemoteStore {
	return &RemoteStore{baseURL: baseURL}
}

// PutString сохраняет строковое значение по ключу
func (s *RemoteStore) PutString(key, value string) error {
	data := url.Values{}
	data.Set("key", key)
	data.Set("value", value)

	req, err := http.NewRequest(http.MethodPost, s.baseURL+"/api/put", strings.NewReader(data.Encode()))
	if err != nil {
		return fmt.Errorf("failed to create PUT request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-LSMDB-Replica", "1")

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to PUT: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("PUT failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// GetString получает строковое значение по ключу
func (s *RemoteStore) GetString(key string) (string, bool, error) {
	resp, err := httpClient.Get(fmt.Sprintf("%s/api/get?key=%s", s.baseURL, url.QueryEscape(key)))
	if err != nil {
		return "", false, fmt.Errorf("failed to GET: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", false, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return "", false, fmt.Errorf("GET failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Читаем тело ответа
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", false, fmt.Errorf("failed to read response: %w", err)
	}

	// Парсим JSON
	var response Response
	if err := json.Unmarshal(body, &response); err != nil {
		return "", false, fmt.Errorf("failed to parse response: %w", err)
	}

	return response.Value, true, nil
}

// Delete удаляет значение по ключу
func (s *RemoteStore) Delete(key string) error {
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/api/delete?key=%s", s.baseURL, url.QueryEscape(key)), nil)
	if err != nil {
		return fmt.Errorf("failed to create DELETE request: %w", err)
	}
	req.Header.Set("X-LSMDB-Replica", "1")

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to DELETE: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("DELETE failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}
