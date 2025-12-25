package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// HTTPClient реализует Remote интерфейс для HTTP запросов к другим нодам
type HTTPClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewHTTPClient создает новый HTTP клиент для удаленной ноды
func NewHTTPClient(baseURL string) *HTTPClient {
	return &HTTPClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (c *HTTPClient) PutString(key, value string) error {
	formData := fmt.Sprintf("key=%s&value=%s", url.QueryEscape(key), url.QueryEscape(value))

	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPut,
		c.baseURL+"/api/string",
		bytes.NewBufferString(formData),
	)
	if err != nil {
		return fmt.Errorf("create PUT request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute PUT request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("PUT failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (c *HTTPClient) GetString(key string) (string, bool, error) {
	reqURL := fmt.Sprintf("%s/api/string?key=%s", c.baseURL, url.QueryEscape(key))

	resp, err := c.httpClient.Get(reqURL)
	if err != nil {
		return "", false, fmt.Errorf("execute GET request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", false, nil
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", false, fmt.Errorf("GET failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", false, fmt.Errorf("decode response: %w", err)
	}

	if val, ok := result["value"].(string); ok {
		return val, true, nil
	}

	return "", false, nil
}

func (c *HTTPClient) Delete(key string) error {
	reqURL := fmt.Sprintf("%s/api?key=%s", c.baseURL, url.QueryEscape(key))

	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodDelete,
		reqURL,
		nil,
	)
	if err != nil {
		return fmt.Errorf("create DELETE request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute DELETE request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("DELETE failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}
