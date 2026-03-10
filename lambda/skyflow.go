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
	"sync"
	"time"

	"crypto/tls"

	api "github.com/skyflowapi/common/api/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

// SkyflowConfig holds environment-driven configuration for the Skyflow v2 API.
type SkyflowConfig struct {
	DataPlaneURL     string
	GRPCEndpoint     string
	AccountID        string
	APIKey           string
	VaultID          string
	TableName        string
	ColumnName       string
	BatchSize        int
	MaxConcurrency   int
	GRPCTimeoutMs    int  // per-call timeout in ms (default 100)
	GRPCRetries      int  // max retries on timeout (default 2)
	GRPCHardDeadline int  // absolute deadline in ms (default 2000)
	UniformBatches   bool // distribute tokens uniformly across batches (default true)
}

// SkyflowMetrics captures per-invocation metrics across all three layers.
type SkyflowMetrics struct {
	TotalRows     int     // rows received from Snowflake
	UniqueTokens  int     // unique tokens after dedup (= TotalRows for tokenize)
	DedupPct      float64 // percent reduction from dedup
	SkyflowCalls  int     // number of Skyflow API sub-batch calls
	SkyflowWallMs int64   // wall clock ms for all Skyflow work (concurrent)
	CallMinMs     int64   // fastest individual API call
	CallMaxMs     int64   // slowest individual API call
	CallAvgMs     int64   // average individual API call
	Errors        int     // API errors (final failures)
	Retries       int     // total gRPC retries across all sub-batches
}

// SkyflowClient makes batched, concurrent calls to the Skyflow v2 API.
type SkyflowClient struct {
	cfg        SkyflowConfig
	client     *http.Client
	flowClient api.FlowServiceClient
}

// loadSkyflowConfig reads Skyflow configuration from environment variables.
// Returns nil if SKYFLOW_DATA_PLANE_URL is not set (mock mode).
func loadSkyflowConfig() *SkyflowConfig {
	url := os.Getenv("SKYFLOW_DATA_PLANE_URL")
	if url == "" {
		return nil
	}

	cfg := &SkyflowConfig{
		DataPlaneURL:   url,
		GRPCEndpoint:   os.Getenv("SKYFLOW_GRPC_ENDPOINT"),
		AccountID:      os.Getenv("SKYFLOW_ACCOUNT_ID"),
		APIKey:         os.Getenv("SKYFLOW_API_KEY"),
		VaultID:        os.Getenv("SKYFLOW_VAULT_ID"),
		TableName:      envOrDefault("SKYFLOW_TABLE_NAME", "table1"),
		ColumnName:     envOrDefault("SKYFLOW_COLUMN_NAME", "name"),
		BatchSize:        envIntOrDefault("SKYFLOW_BATCH_SIZE", 25),
		MaxConcurrency:   envIntOrDefault("SKYFLOW_MAX_CONCURRENCY", 10),
		GRPCTimeoutMs:    envIntOrDefault("SKYFLOW_GRPC_TIMEOUT_MS", 100),
		GRPCRetries:      envIntOrDefault("SKYFLOW_GRPC_RETRIES", 2),
		GRPCHardDeadline: envIntOrDefault("SKYFLOW_GRPC_HARD_DEADLINE_MS", 2000),
		UniformBatches:   envBoolOrDefault("SKYFLOW_UNIFORM_BATCHES", true),
	}

	if cfg.APIKey == "" {
		log.Printf("WARN: SKYFLOW_DATA_PLANE_URL set but SKYFLOW_API_KEY missing — Skyflow calls will fail")
	}
	if cfg.VaultID == "" {
		log.Printf("WARN: SKYFLOW_DATA_PLANE_URL set but SKYFLOW_VAULT_ID missing — Skyflow calls will fail")
	}

	return cfg
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

func envBoolOrDefault(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		b, err := strconv.ParseBool(v)
		if err == nil {
			return b
		}
	}
	return fallback
}

// basicAuthCreds implements grpc.PerRPCCredentials for Bearer token auth.
type basicAuthCreds struct {
	token string
}

func (b basicAuthCreds) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Bearer " + b.token,
	}, nil
}

func (b basicAuthCreds) RequireTransportSecurity() bool {
	return false
}

// NewSkyflowClient creates a client with connection pooling and optional gRPC.
func NewSkyflowClient(cfg SkyflowConfig) *SkyflowClient {
	sc := &SkyflowClient{
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

	// Initialize gRPC client if endpoint is configured
	if cfg.GRPCEndpoint != "" {
		// NLB terminates TLS without ALPN h2 negotiation.
		// grpc-go >= 1.67 enforces ALPN by default — requires GRPC_ENFORCE_ALPN_ENABLED=false
		// in Lambda environment variables (must be set before grpc package init).
		conn, err := grpc.NewClient(
			cfg.GRPCEndpoint,
			grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})),
			grpc.WithPerRPCCredentials(basicAuthCreds{token: cfg.APIKey}),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(64*1024*1024),
				grpc.MaxCallSendMsgSize(64*1024*1024),
			),
		)
		if err != nil {
			log.Printf("WARN: gRPC connection to %s failed: %v — falling back to REST", cfg.GRPCEndpoint, err)
		} else {
			// Force eager TLS handshake during init, not during first request.
			// This moves cold-start latency (500-2000ms) out of the request path.
			conn.Connect()
			sc.flowClient = api.NewFlowServiceClient(conn)
			log.Printf("INFO: gRPC client initialized (endpoint=%s)", cfg.GRPCEndpoint)
		}
	}

	return sc
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
		val, ok := row[1].(string)
		if !ok {
			val = fmt.Sprintf("%v", row[1])
		}
		items = append(items, indexedValue{
			origIdx:  i,
			rowIndex: row[0],
			value:    val,
		})
	}

	metrics.UniqueTokens = len(items) // no dedup for tokenize
	metrics.DedupPct = 0

	// Split into sub-batches
	batches := splitIndexedValues(items, sc.cfg.BatchSize)
	metrics.SkyflowCalls = len(batches)

	callLatencies := make([]int64, 0, len(batches))

	skyflowStart := time.Now()

	if sc.cfg.MaxConcurrency <= 1 || len(batches) <= 1 {
		for _, batch := range batches {
			callStart := time.Now()
			tokens, err := sc.tokenizeBatch(ctx, batch)
			callLatencies = append(callLatencies, time.Since(callStart).Milliseconds())
			if err != nil {
				metrics.Errors++
				errMsg := "ERROR: " + err.Error()
				for _, item := range batch {
					result[item.origIdx] = []interface{}{item.rowIndex, errMsg}
				}
				continue
			}
			for j, item := range batch {
				result[item.origIdx] = []interface{}{item.rowIndex, tokens[j]}
			}
		}
	} else {
		var mu sync.Mutex
		var wg sync.WaitGroup
		useSemaphore := len(batches) > sc.cfg.MaxConcurrency
		var sem chan struct{}
		if useSemaphore {
			sem = make(chan struct{}, sc.cfg.MaxConcurrency)
		}

		for _, batch := range batches {
			wg.Add(1)
			go func(batch []indexedValue) {
				defer wg.Done()
				if useSemaphore {
					sem <- struct{}{}
					defer func() { <-sem }()
				}

				callStart := time.Now()
				tokens, err := sc.tokenizeBatch(ctx, batch)
				callMs := time.Since(callStart).Milliseconds()

				mu.Lock()
				callLatencies = append(callLatencies, callMs)
				if err != nil {
					metrics.Errors++
					errMsg := "ERROR: " + err.Error()
					for _, item := range batch {
						result[item.origIdx] = []interface{}{item.rowIndex, errMsg}
					}
					mu.Unlock()
					return
				}
				for j, item := range batch {
					result[item.origIdx] = []interface{}{item.rowIndex, tokens[j]}
				}
				mu.Unlock()
			}(batch)
		}
		wg.Wait()
	}

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
func (sc *SkyflowClient) Detokenize(ctx context.Context, rows []SfRow) ([]SfRow, *SkyflowMetrics, error) {
	result := make([]SfRow, len(rows))
	metrics := &SkyflowMetrics{TotalRows: len(rows)}

	// Build dedup map: token → list of original indices
	type rowRef struct {
		origIdx int
		rowNum  json.RawMessage
	}
	tokenMap := make(map[string][]rowRef, len(rows))
	orderedTokens := make([]string, 0, len(rows))

	for i, row := range rows {
		token := row.Value
		if token == "" {
			result[i] = SfRow{RowNum: row.RowNum, Value: "ERROR: missing value"}
			continue
		}
		refs := tokenMap[token]
		if len(refs) == 0 {
			orderedTokens = append(orderedTokens, token)
		}
		tokenMap[token] = append(refs, rowRef{origIdx: i, rowNum: row.RowNum})
	}

	metrics.UniqueTokens = len(orderedTokens)
	if len(rows) > 0 {
		metrics.DedupPct = 100.0 * (1.0 - float64(len(orderedTokens))/float64(len(rows)))
	}

	// Split unique tokens into sub-batches
	var batches [][]string
	if sc.cfg.UniformBatches {
		// Uniform distribution: calculate number of batches, then divide equally
		// so all batches finish at roughly the same time when dispatched concurrently.
		numBatches := (len(orderedTokens) + sc.cfg.BatchSize - 1) / sc.cfg.BatchSize
		batches = splitStringsUniform(orderedTokens, numBatches)
	} else {
		batches = splitStrings(orderedTokens, sc.cfg.BatchSize)
	}
	metrics.SkyflowCalls = len(batches)

	valueMap := make(map[string]string, len(orderedTokens))
	callLatencies := make([]int64, 0, len(batches))

	skyflowStart := time.Now()

	if sc.cfg.MaxConcurrency <= 1 || len(batches) <= 1 {
		// Sequential path — no goroutine overhead
		for _, batch := range batches {
			callStart := time.Now()
			values, retries, err := sc.detokenizeBatch(ctx, batch)
			callLatencies = append(callLatencies, time.Since(callStart).Milliseconds())
			metrics.Retries += retries
			if err != nil {
				metrics.Errors++
				errMsg := "ERROR: " + err.Error()
				for _, tok := range batch {
					valueMap[tok] = errMsg
				}
				continue
			}
			for i, tok := range batch {
				valueMap[tok] = values[i]
			}
		}
	} else {
		// Concurrent path
		var mu sync.Mutex
		var wg sync.WaitGroup
		useSemaphore := len(batches) > sc.cfg.MaxConcurrency
		var sem chan struct{}
		if useSemaphore {
			sem = make(chan struct{}, sc.cfg.MaxConcurrency)
		}

		for _, batch := range batches {
			wg.Add(1)
			go func(batch []string) {
				defer wg.Done()
				if useSemaphore {
					sem <- struct{}{}
					defer func() { <-sem }()
				}

				callStart := time.Now()
				values, retries, err := sc.detokenizeBatch(ctx, batch)
				callMs := time.Since(callStart).Milliseconds()

				mu.Lock()
				callLatencies = append(callLatencies, callMs)
				metrics.Retries += retries
				if err != nil {
					metrics.Errors++
					errMsg := "ERROR: " + err.Error()
					for _, tok := range batch {
						valueMap[tok] = errMsg
					}
					mu.Unlock()
					return
				}
				for i, tok := range batch {
					valueMap[tok] = values[i]
				}
				mu.Unlock()
			}(batch)
		}
		wg.Wait()
	}

	metrics.SkyflowWallMs = time.Since(skyflowStart).Milliseconds()
	computeLatencyStats(metrics, callLatencies)

	// Fan results back to all original row indexes
	for token, refs := range tokenMap {
		val := valueMap[token]
		for _, ref := range refs {
			result[ref.origIdx] = SfRow{RowNum: ref.rowNum, Value: val}
		}
	}

	return result, metrics, nil
}

// detokenizeBatch returns (values, retries, error).
func (sc *SkyflowClient) detokenizeBatch(ctx context.Context, tokens []string) ([]string, int, error) {
	// Use gRPC if available
	if sc.flowClient != nil {
		return sc.detokenizeBatchGrpc(ctx, tokens)
	}
	vals, err := sc.detokenizeBatchREST(ctx, tokens)
	return vals, 0, err
}

// detokenizeBatchGrpc returns (values, retries, error).
func (sc *SkyflowClient) detokenizeBatchGrpc(ctx context.Context, tokens []string) ([]string, int, error) {
	hardCtx, hardCancel := context.WithTimeout(ctx, time.Duration(sc.cfg.GRPCHardDeadline)*time.Millisecond)
	defer hardCancel()

	md := metadata.Pairs("X-SKYFLOW-ACCOUNT-ID", sc.cfg.AccountID)

	req := &api.FlowDetokenizeRequest{
		VaultID: sc.cfg.VaultID,
		Tokens:  tokens,
	}

	var lastErr error
	retries := 0
	for attempt := 0; attempt <= sc.cfg.GRPCRetries; attempt++ {
		callCtx, callCancel := context.WithTimeout(hardCtx, time.Duration(sc.cfg.GRPCTimeoutMs)*time.Millisecond)
		grpcCtx := metadata.NewOutgoingContext(callCtx, md)

		resp, err := sc.flowClient.Detokenize(grpcCtx, req)
		callCancel()

		if err != nil {
			lastErr = err
			retries++
			// Only retry on deadline exceeded (slow call), not on other errors
			if hardCtx.Err() != nil {
				break // hard deadline hit, stop retrying
			}
			if ctx.Err() != nil {
				break // parent context cancelled
			}
			continue // timeout — retry immediately on likely different pod
		}

		if len(resp.Response) != len(tokens) {
			return nil, retries, fmt.Errorf("detokenize: expected %d entries, got %d", len(tokens), len(resp.Response))
		}

		values := make([]string, len(tokens))
		for i, entry := range resp.Response {
			if entry.Value != nil {
				values[i] = entry.Value.GetStringValue()
			} else if entry.Error != nil {
				values[i] = "ERROR: " + entry.Error.GetValue()
			} else {
				values[i] = "ERROR: no value"
			}
		}
		return values, retries, nil
	}

	return nil, retries, fmt.Errorf("detokenize: grpc error after %d attempts: %w", sc.cfg.GRPCRetries+1, lastErr)
}

func (sc *SkyflowClient) detokenizeBatchREST(ctx context.Context, tokens []string) ([]string, error) {
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

// splitStringsUniform divides items into n batches of roughly equal size.
// E.g. 1810 items into 4 batches → [453, 453, 452, 452]
func splitStringsUniform(items []string, n int) [][]string {
	if n <= 0 || len(items) == 0 {
		return nil
	}
	if n > len(items) {
		n = len(items)
	}
	batches := make([][]string, n)
	base := len(items) / n
	extra := len(items) % n
	offset := 0
	for i := 0; i < n; i++ {
		size := base
		if i < extra {
			size++
		}
		batches[i] = items[offset : offset+size]
		offset += size
	}
	return batches
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
