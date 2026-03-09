package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// SkyflowConfig holds environment-driven configuration for the Skyflow v2 API.
type SkyflowConfig struct {
	DataPlaneURL string
	APIKey       string
	VaultID      string
}

// SkyflowClient makes calls to the Skyflow v2 API.
type SkyflowClient struct {
	cfg    SkyflowConfig
	client *http.Client
}

// loadSkyflowConfig reads Skyflow configuration from environment variables.
func loadSkyflowConfig() *SkyflowConfig {
	url := os.Getenv("SKYFLOW_DATA_PLANE_URL")
	apiKey := os.Getenv("SKYFLOW_API_KEY")
	vaultID := os.Getenv("SKYFLOW_VAULT_ID")

	if url == "" || apiKey == "" || vaultID == "" {
		return nil
	}

	return &SkyflowConfig{
		DataPlaneURL: url,
		APIKey:       apiKey,
		VaultID:      vaultID,
	}
}

// NewSkyflowClient creates a client with HTTP connection pooling.
func NewSkyflowClient(cfg SkyflowConfig) *SkyflowClient {
	return &SkyflowClient{
		cfg: cfg,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// --- Detokenize ---

type detokenizeRequest struct {
	VaultID string   `json:"vaultID"`
	Tokens  []string `json:"tokens"`
}

type detokenizeResponse struct {
	Response []detokenizeEntry `json:"response"`
}

type detokenizeEntry struct {
	Token string `json:"token"`
	Value string `json:"value"`
}

// Detokenize sends tokens to Skyflow for detokenization with deduplication.
func (sc *SkyflowClient) Detokenize(ctx context.Context, rows [][]interface{}) ([][]interface{}, error) {
	result := make([][]interface{}, len(rows))

	// Build dedup map: token → list of (origIdx, rowIndex)
	type rowRef struct {
		origIdx  int
		rowIndex interface{}
	}
	tokenMap := make(map[string][]rowRef)
	var orderedTokens []string

	for i, row := range rows {
		if len(row) < 2 {
			result[i] = []interface{}{row[0], "ERROR: missing value"}
			continue
		}
		token := fmt.Sprintf("%v", row[1])
		refs := tokenMap[token]
		if len(refs) == 0 {
			orderedTokens = append(orderedTokens, token)
		}
		tokenMap[token] = append(refs, rowRef{origIdx: i, rowIndex: row[0]})
	}

	// Call Skyflow API with all unique tokens in a single request
	values, err := sc.detokenizeBatch(ctx, orderedTokens)
	if err != nil {
		return nil, err
	}

	// Create token → value map
	valueMap := make(map[string]string, len(orderedTokens))
	for i, token := range orderedTokens {
		valueMap[token] = values[i]
	}

	// Fan results back to all original row indexes
	for token, refs := range tokenMap {
		val := valueMap[token]
		for _, ref := range refs {
			result[ref.origIdx] = []interface{}{ref.rowIndex, val}
		}
	}

	return result, nil
}

func (sc *SkyflowClient) detokenizeBatch(ctx context.Context, tokens []string) ([]string, error) {
	body := detokenizeRequest{
		VaultID: sc.cfg.VaultID,
		Tokens:  tokens,
	}

	respBody, err := sc.doPost(ctx, sc.cfg.DataPlaneURL+"/v2/tokens/detokenize", body)
	if err != nil {
		return nil, err
	}

	var resp detokenizeResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return nil, fmt.Errorf("detokenize: unmarshal response: %w", err)
	}

	if len(resp.Response) != len(tokens) {
		return nil, fmt.Errorf("detokenize: expected %d entries, got %d", len(tokens), len(resp.Response))
	}

	values := make([]string, len(tokens))
	for i, entry := range resp.Response {
		values[i] = entry.Value
	}

	return values, nil
}

// --- HTTP helpers ---

func (sc *SkyflowClient) doPost(ctx context.Context, url string, body interface{}) ([]byte, error) {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+sc.cfg.APIKey)

	resp, err := sc.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("skyflow request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("skyflow API returned %d: %s", resp.StatusCode, truncate(string(respBody), 200))
	}

	return respBody, nil
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
