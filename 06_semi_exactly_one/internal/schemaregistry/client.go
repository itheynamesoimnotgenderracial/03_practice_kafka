package schemaregistry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// Client is a minimal Confluent Schema Registry HTTP client.
// It caches schema IDs and schema strings to avoid repeated round-trips.
type Client struct {
	baseURL string
	http    *http.Client
	mu      sync.RWMutex
	bySubj  map[string]int    // subject -> schemaID
	byID    map[int]string    // schemaID -> avsc JSON
}

type registerRequest struct {
	Schema string `json:"schema"`
}

type registerResponse struct {
	ID int `json:"id"`
}

type schemaResponse struct {
	Schema string `json:"schema"`
}

// New returns a Client pointed at the given Confluent Schema Registry base URL
// (e.g. "http://schema-registry:8081").
func New(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: 10 * time.Second},
		bySubj:  make(map[string]int),
		byID:    make(map[int]string),
	}
}

// EnsureSchema registers avscJSON under subject (POST /subjects/{subject}/versions).
// If the exact schema already exists the registry returns its existing ID.
// The ID is cached so subsequent calls are free.
func (c *Client) EnsureSchema(subject, avscJSON string) (int, error) {
	c.mu.RLock()
	if id, ok := c.bySubj[subject]; ok {
		c.mu.RUnlock()
		return id, nil
	}
	c.mu.RUnlock()

	body, _ := json.Marshal(registerRequest{Schema: avscJSON})
	url := fmt.Sprintf("%s/subjects/%s/versions", c.baseURL, subject)

	resp, err := c.http.Post(url, "application/vnd.schemaregistry.v1+json", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("schema registry post %s: %w", subject, err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("schema registry %d for %s: %s", resp.StatusCode, subject, raw)
	}

	var sr registerResponse
	if err := json.Unmarshal(raw, &sr); err != nil {
		return 0, fmt.Errorf("parse schema registry response: %w", err)
	}

	c.mu.Lock()
	c.bySubj[subject] = sr.ID
	c.byID[sr.ID] = avscJSON
	c.mu.Unlock()
	return sr.ID, nil
}

// SchemaByID fetches the avsc JSON for the given schema ID (GET /schemas/ids/{id}).
// Result is cached.
func (c *Client) SchemaByID(id int) (string, error) {
	c.mu.RLock()
	if sc, ok := c.byID[id]; ok {
		c.mu.RUnlock()
		return sc, nil
	}
	c.mu.RUnlock()

	url := fmt.Sprintf("%s/schemas/ids/%d", c.baseURL, id)
	resp, err := c.http.Get(url)
	if err != nil {
		return "", fmt.Errorf("schema registry get %d: %w", id, err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("schema registry %d for id %d: %s", resp.StatusCode, id, raw)
	}

	var sr schemaResponse
	if err := json.Unmarshal(raw, &sr); err != nil {
		return "", fmt.Errorf("parse schema: %w", err)
	}

	c.mu.Lock()
	c.byID[id] = sr.Schema
	c.mu.Unlock()
	return sr.Schema, nil
}
