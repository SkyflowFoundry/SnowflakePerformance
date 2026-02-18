package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SkyflowConfig holds environment-driven configuration for the Skyflow v2 API.
type SkyflowConfig struct {
	DataPlaneURL   string
	AccountID      string
	APIKey         string
	VaultID        string
	TableName      string
	ColumnName     string
	BatchSize      int
	MaxConcurrency int
}

// SkyflowMetrics captures per-invocation metrics across all three layers.
type SkyflowMetrics struct {
	TotalRows    int   // rows received from Snowflake
	UniqueTokens int   // unique tokens after dedup (= TotalRows for tokenize)
	DedupPct     float64 // percent reduction from dedup
	SkyflowCalls int   // number of Skyflow API sub-batch calls
	SkyflowWallMs int64 // wall clock ms for all Skyflow work (concurrent)
	CallMinMs    int64  // fastest individual API call
	CallMaxMs    int64  // slowest individual API call
	CallAvgMs    int64  // average individual API call
	Errors       int   // API errors/retries
}

// SkyflowClient makes batched, concurrent calls to the Skyflow v2 API.
type SkyflowClient struct {
	cfg    SkyflowConfig
	client *http.Client
}

// loadSkyflowConfigs reads Skyflow configuration from environment variables.
// Returns nil if SKYFLOW_DATA_PLANE_URL is not set (mock mode).
// Supports per-entity vault IDs via SKYFLOW_VAULT_ID_{ENTITY} env vars.
// Falls back to single SKYFLOW_VAULT_ID for backward compatibility.
func loadSkyflowConfigs() map[string]*SkyflowConfig {
	url := os.Getenv("SKYFLOW_DATA_PLANE_URL")
	if url == "" {
		return nil
	}

	apiKey := os.Getenv("SKYFLOW_API_KEY")
	accountID := os.Getenv("SKYFLOW_ACCOUNT_ID")
	batchSize := envIntOrDefault("SKYFLOW_BATCH_SIZE", 25)
	maxConcurrency := envIntOrDefault("SKYFLOW_MAX_CONCURRENCY", 10)

	if apiKey == "" {
		log.Printf("WARN: SKYFLOW_DATA_PLANE_URL set but SKYFLOW_API_KEY missing — Skyflow calls will fail")
	}

	entities := []string{"NAME", "ID", "SSN", "DOB", "EMAIL"}
	configs := make(map[string]*SkyflowConfig)

	// Try per-entity vault IDs first
	for _, entity := range entities {
		vaultID := os.Getenv("SKYFLOW_VAULT_ID_" + entity)
		if vaultID == "" {
			continue
		}
		configs[entity] = &SkyflowConfig{
			DataPlaneURL:   url,
			AccountID:      accountID,
			APIKey:         apiKey,
			VaultID:        vaultID,
			TableName:      "table1",
			ColumnName:     strings.ToLower(entity),
			BatchSize:      batchSize,
			MaxConcurrency: maxConcurrency,
		}
	}

	// Backward compat: fall back to single SKYFLOW_VAULT_ID if no per-entity vars found
	if len(configs) == 0 {
		vaultID := os.Getenv("SKYFLOW_VAULT_ID")
		if vaultID == "" {
			log.Printf("WARN: SKYFLOW_DATA_PLANE_URL set but no SKYFLOW_VAULT_ID or per-entity vault IDs found")
			return nil
		}
		configs["NAME"] = &SkyflowConfig{
			DataPlaneURL:   url,
			AccountID:      accountID,
			APIKey:         apiKey,
			VaultID:        vaultID,
			TableName:      envOrDefault("SKYFLOW_TABLE_NAME", "table1"),
			ColumnName:     envOrDefault("SKYFLOW_COLUMN_NAME", "name"),
			BatchSize:      batchSize,
			MaxConcurrency: maxConcurrency,
		}
	}

	return configs
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envIntOrDefault(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

// NewSkyflowClient creates a client with connection pooling.
func NewSkyflowClient(cfg SkyflowConfig) *SkyflowClient {
	return &SkyflowClient{
		cfg: cfg,
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 50,
				MaxIdleConns:        100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// --- Tokenize ---

type tokenizeRequest struct {
	VaultID   string              `json:"vaultID"`
	TableName string              `json:"tableName"`
	Records   []tokenizeRecordReq `json:"records"`
}

type tokenizeRecordReq struct {
	Data map[string]string `json:"data"`
}

type tokenizeResponse struct {
	Records []tokenizeRecordResp `json:"records"`
}

type tokenizeRecordResp struct {
	Tokens map[string][]tokenEntry `json:"tokens"`
}

type tokenEntry struct {
	Token string `json:"token"`
}

// Tokenize sends values to Skyflow for tokenization.
func (sc *SkyflowClient) Tokenize(ctx context.Context, rows [][]interface{}) ([][]interface{}, *SkyflowMetrics, error) {
	result := make([][]interface{}, len(rows))
	metrics := &SkyflowMetrics{TotalRows: len(rows)}

	// Extract row indices and values
	items := make([]indexedValue, 0, len(rows))
	for i, row := range rows {
		if len(row) < 2 {
			result[i] = []interface{}{i, "ERROR: missing value"}
			continue
		}
		items = append(items, indexedValue{
			origIdx:  i,
			rowIndex: row[0],
			value:    fmt.Sprintf("%v", row[1]),
		})
	}

	metrics.UniqueTokens = len(items) // no dedup for tokenize
	metrics.DedupPct = 0

	// Split into sub-batches
	batches := splitIndexedValues(items, sc.cfg.BatchSize)
	metrics.SkyflowCalls = len(batches)

	// Process concurrently, collecting per-call latencies
	sem := make(chan struct{}, sc.cfg.MaxConcurrency)
	var mu sync.Mutex
	var wg sync.WaitGroup
	callLatencies := make([]int64, 0, len(batches))

	skyflowStart := time.Now()

	for _, batch := range batches {
		wg.Add(1)
		go func(batch []indexedValue) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			callStart := time.Now()
			tokens, err := sc.tokenizeBatch(ctx, batch)
			callMs := time.Since(callStart).Milliseconds()

			mu.Lock()
			defer mu.Unlock()
			callLatencies = append(callLatencies, callMs)
			if err != nil {
				metrics.Errors++
				for _, item := range batch {
					result[item.origIdx] = []interface{}{item.rowIndex, fmt.Sprintf("ERROR: %v", err)}
				}
				return
			}
			for j, item := range batch {
				result[item.origIdx] = []interface{}{item.rowIndex, tokens[j]}
			}
		}(batch)
	}
	wg.Wait()

	metrics.SkyflowWallMs = time.Since(skyflowStart).Milliseconds()
	computeLatencyStats(metrics, callLatencies)

	return result, metrics, nil
}

func (sc *SkyflowClient) tokenizeBatch(ctx context.Context, items []indexedValue) ([]string, error) {
	records := make([]tokenizeRecordReq, len(items))
	for i, item := range items {
		records[i] = tokenizeRecordReq{
			Data: map[string]string{sc.cfg.ColumnName: item.value},
		}
	}

	body := tokenizeRequest{
		VaultID:   sc.cfg.VaultID,
		TableName: sc.cfg.TableName,
		Records:   records,
	}

	respBody, err := sc.doWithRetry(ctx, sc.cfg.DataPlaneURL+"/v2/records/insert", body)
	if err != nil {
		return nil, err
	}

	var resp tokenizeResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return nil, fmt.Errorf("tokenize: unmarshal response: %w", err)
	}

	if len(resp.Records) != len(items) {
		return nil, fmt.Errorf("tokenize: expected %d records, got %d", len(items), len(resp.Records))
	}

	tokens := make([]string, len(items))
	for i, rec := range resp.Records {
		entries, ok := rec.Tokens[sc.cfg.ColumnName]
		if !ok || len(entries) == 0 {
			return nil, fmt.Errorf("tokenize: no token for column %q in record %d", sc.cfg.ColumnName, i)
		}
		tokens[i] = entries[0].Token
	}

	return tokens, nil
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
func (sc *SkyflowClient) Detokenize(ctx context.Context, rows [][]interface{}) ([][]interface{}, *SkyflowMetrics, error) {
	result := make([][]interface{}, len(rows))
	metrics := &SkyflowMetrics{TotalRows: len(rows)}

	// Build dedup map: token → list of (origIdx, rowIndex)
	type rowRef struct {
		origIdx  int
		rowIndex interface{}
	}
	tokenMap := make(map[string][]rowRef)
	var orderedTokens []string

	for i, row := range rows {
		if len(row) < 2 {
			result[i] = []interface{}{i, "ERROR: missing value"}
			continue
		}
		token := fmt.Sprintf("%v", row[1])
		refs := tokenMap[token]
		if len(refs) == 0 {
			orderedTokens = append(orderedTokens, token)
		}
		tokenMap[token] = append(refs, rowRef{origIdx: i, rowIndex: row[0]})
	}

	metrics.UniqueTokens = len(orderedTokens)
	if len(rows) > 0 {
		metrics.DedupPct = 100.0 * (1.0 - float64(len(orderedTokens))/float64(len(rows)))
	}

	// Split unique tokens into sub-batches
	batches := splitStrings(orderedTokens, sc.cfg.BatchSize)
	metrics.SkyflowCalls = len(batches)

	// Process concurrently, collecting per-call latencies
	sem := make(chan struct{}, sc.cfg.MaxConcurrency)
	var mu sync.Mutex
	var wg sync.WaitGroup
	valueMap := make(map[string]string, len(orderedTokens))
	callLatencies := make([]int64, 0, len(batches))

	skyflowStart := time.Now()

	for _, batch := range batches {
		wg.Add(1)
		go func(batch []string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			callStart := time.Now()
			values, err := sc.detokenizeBatch(ctx, batch)
			callMs := time.Since(callStart).Milliseconds()

			mu.Lock()
			defer mu.Unlock()
			callLatencies = append(callLatencies, callMs)
			if err != nil {
				metrics.Errors++
				for _, tok := range batch {
					valueMap[tok] = fmt.Sprintf("ERROR: %v", err)
				}
				return
			}
			for i, tok := range batch {
				valueMap[tok] = values[i]
			}
		}(batch)
	}
	wg.Wait()

	metrics.SkyflowWallMs = time.Since(skyflowStart).Milliseconds()
	computeLatencyStats(metrics, callLatencies)

	// Fan results back to all original row indexes
	for token, refs := range tokenMap {
		val := valueMap[token]
		for _, ref := range refs {
			result[ref.origIdx] = []interface{}{ref.rowIndex, val}
		}
	}

	return result, metrics, nil
}

func (sc *SkyflowClient) detokenizeBatch(ctx context.Context, tokens []string) ([]string, error) {
	body := detokenizeRequest{
		VaultID: sc.cfg.VaultID,
		Tokens:  tokens,
	}

	respBody, err := sc.doWithRetry(ctx, sc.cfg.DataPlaneURL+"/v2/tokens/detokenize", body)
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

func (sc *SkyflowClient) doWithRetry(ctx context.Context, url string, body interface{}) ([]byte, error) {
	respBody, statusCode, err := sc.doPost(ctx, url, body)
	if err != nil {
		return nil, err
	}

	if statusCode >= 500 || statusCode == 429 {
		log.Printf("WARN: Skyflow returned %d, retrying after 500ms...", statusCode)
		time.Sleep(500 * time.Millisecond)
		respBody, statusCode, err = sc.doPost(ctx, url, body)
		if err != nil {
			return nil, err
		}
	}

	if statusCode < 200 || statusCode >= 300 {
		return nil, fmt.Errorf("skyflow API returned %d: %s", statusCode, truncate(string(respBody), 200))
	}

	return respBody, nil
}

func (sc *SkyflowClient) doPost(ctx context.Context, url string, body interface{}) ([]byte, int, error) {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+sc.cfg.APIKey)
	if sc.cfg.AccountID != "" {
		req.Header.Set("X-Skyflow-Account-Id", sc.cfg.AccountID)
	}

	resp, err := sc.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("skyflow request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("read response: %w", err)
	}

	return respBody, resp.StatusCode, nil
}

// --- Utility ---

func computeLatencyStats(m *SkyflowMetrics, latencies []int64) {
	if len(latencies) == 0 {
		return
	}
	var sum int64
	m.CallMinMs = latencies[0]
	m.CallMaxMs = latencies[0]
	for _, l := range latencies {
		sum += l
		if l < m.CallMinMs {
			m.CallMinMs = l
		}
		if l > m.CallMaxMs {
			m.CallMaxMs = l
		}
	}
	m.CallAvgMs = sum / int64(len(latencies))
}

type indexedValue struct {
	origIdx  int
	rowIndex interface{}
	value    string
}

func splitIndexedValues(items []indexedValue, size int) [][]indexedValue {
	var batches [][]indexedValue
	for i := 0; i < len(items); i += size {
		end := i + size
		if end > len(items) {
			end = len(items)
		}
		batches = append(batches, items[i:end])
	}
	return batches
}

func splitStrings(items []string, size int) [][]string {
	var batches [][]string
	for i := 0; i < len(items); i += size {
		end := i + size
		if end > len(items) {
			end = len(items)
		}
		batches = append(batches, items[i:end])
	}
	return batches
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
