package main

import (
	"context"
	"fmt"
	"os"
	"testing"
)

// Run with real Skyflow credentials:
//
//	SKYFLOW_DATA_PLANE_URL=https://lgrkinpzqtda.skyvault.skyflowapis.com \
//	SKYFLOW_VAULT_ID_NAME=b937a5d3be514ee6b0ca5236708be247 \
//	SKYFLOW_ACCOUNT_ID=s941db958e8646729eebfe63b1da4198 \
//	SKYFLOW_API_KEY='<your-jwt>' \
//	go test -v -run TestSkyflowRoundTrip ./...
func TestSkyflowRoundTrip(t *testing.T) {
	if os.Getenv("SKYFLOW_DATA_PLANE_URL") == "" {
		t.Skip("SKYFLOW_DATA_PLANE_URL not set — skipping live test")
	}

	configs := loadSkyflowConfigs()
	if configs == nil {
		t.Fatal("loadSkyflowConfigs returned nil")
	}
	cfg, ok := configs["NAME"]
	if !ok {
		t.Fatal("no NAME config found in loadSkyflowConfigs result")
	}
	client := NewSkyflowClient(*cfg)
	ctx := context.Background()

	// Tokenize — Snowflake row format: [[idx, value], ...]
	tokenizeInput := [][]interface{}{
		{0, "Alice"},
		{1, "Bob"},
		{2, "Alice"}, // duplicate value
	}

	t.Log("Tokenizing 3 rows...")
	tokenized, _, err := client.Tokenize(ctx, tokenizeInput)
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}

	for i, row := range tokenized {
		t.Logf("  row %d: idx=%v token=%v", i, row[0], row[1])
		if str, ok := row[1].(string); ok && len(str) > 6 && str[:6] == "ERROR:" {
			t.Fatalf("Tokenize row %d returned error: %s", i, str)
		}
	}

	// Detokenize — use the tokens we just got
	detokenizeInput := make([][]interface{}, len(tokenized))
	for i, row := range tokenized {
		detokenizeInput[i] = []interface{}{row[0], row[1]}
	}

	t.Log("Detokenizing 3 tokens...")
	detokenized, _, err := client.Detokenize(ctx, detokenizeInput)
	if err != nil {
		t.Fatalf("Detokenize failed: %v", err)
	}

	// Verify round-trip
	for i, row := range detokenized {
		t.Logf("  row %d: idx=%v value=%v", i, row[0], row[1])
		original := fmt.Sprintf("%v", tokenizeInput[i][1])
		got := fmt.Sprintf("%v", row[1])
		if got != original {
			t.Errorf("Round-trip mismatch row %d: want %q, got %q", i, original, got)
		}
	}

	t.Log("Round-trip verified!")
}
