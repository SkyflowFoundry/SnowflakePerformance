package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

var (
	simulatedDelay  time.Duration
	invocationCount atomic.Int64
	skyflowClient   *SkyflowClient
)

type sfRequest struct {
	Data [][]interface{} `json:"data"`
}

type sfResponse struct {
	Data [][]interface{} `json:"data"`
}

var lambdaInstanceID string

func init() {
	lambdaInstanceID = fmt.Sprintf("%d", time.Now().UnixNano())

	// Initialize Skyflow client (nil if SKYFLOW_DATA_PLANE_URL not set → mock mode)
	skyflowCfg := loadSkyflowConfig()
	if skyflowCfg != nil {
		skyflowClient = NewSkyflowClient(*skyflowCfg)
		log.Printf("INFO: Skyflow mode enabled (url=%s, vault=%s, batch=%d, concurrency=%d)",
			skyflowCfg.DataPlaneURL, skyflowCfg.VaultID, skyflowCfg.BatchSize, skyflowCfg.MaxConcurrency)
	} else {
		log.Printf("INFO: Mock mode (SKYFLOW_DATA_PLANE_URL not set)")
	}
}

func handler(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	receiveTs := time.Now().UnixNano()
	invNum := invocationCount.Add(1)

	// Normalize headers to lowercase (HTTP headers are case-insensitive,
	// but Go maps are case-sensitive. Snowflake/API Gateway may vary casing.)
	lowerHeaders := make(map[string]string, len(req.Headers))
	for k, v := range req.Headers {
		lowerHeaders[strings.ToLower(k)] = v
	}

	// Extract Snowflake headers (Snowflake prepends "sf-custom-" to custom headers)
	queryID := lowerHeaders["sf-external-function-current-query-id"]
	batchID := lowerHeaders["sf-external-function-query-batch-id"]
	benchConfig := lowerHeaders["sf-benchmark-config"]
	operation := lowerHeaders["sf-custom-x-operation"]
	if operation == "" {
		operation = "detokenize" // backward compatible
	}
	operation = strings.ToLower(operation)

	if queryID == "" {
		queryID = "unknown"
	}
	if batchID == "" {
		batchID = "unknown"
	}
	if benchConfig == "" {
		benchConfig = "unknown"
	}

	// Parse request
	var sfReq sfRequest
	if err := json.Unmarshal([]byte(req.Body), &sfReq); err != nil {
		log.Printf("ERROR: failed to parse request body: %v", err)
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       fmt.Sprintf(`{"error": "invalid request body: %v"}`, err),
		}, nil
	}

	batchSize := len(sfReq.Data)

	// Determine mode and build response
	mode := "mock"
	var resp sfResponse
	var skyflowM *SkyflowMetrics
	if skyflowClient != nil {
		mode = "skyflow"
		var respData [][]interface{}
		var skyflowErr error
		switch operation {
		case "tokenize":
			respData, skyflowM, skyflowErr = skyflowClient.Tokenize(ctx, sfReq.Data)
		case "detokenize":
			respData, skyflowM, skyflowErr = skyflowClient.Detokenize(ctx, sfReq.Data)
		default:
			return events.APIGatewayProxyResponse{
				StatusCode: 400,
				Body:       fmt.Sprintf(`{"error": "unknown operation: %s"}`, operation),
			}, nil
		}
		if skyflowErr != nil {
			log.Printf("ERROR: Skyflow %s failed: %v", operation, skyflowErr)
			return events.APIGatewayProxyResponse{
				StatusCode: 500,
				Body:       fmt.Sprintf(`{"error": "skyflow %s failed: %v"}`, operation, skyflowErr),
			}, nil
		}
		resp = sfResponse{Data: respData}
	} else {
		// Mock mode: simulated delay + DETOK_ prefix
		if simulatedDelay > 0 {
			time.Sleep(simulatedDelay)
		}
		resp = sfResponse{Data: make([][]interface{}, batchSize)}
		for i, row := range sfReq.Data {
			if len(row) < 2 {
				resp.Data[i] = []interface{}{i, "DETOK_ERROR_MISSING_VALUE"}
				continue
			}
			rowNum := row[0]
			tokenVal := fmt.Sprintf("%v", row[1])
			resp.Data[i] = []interface{}{rowNum, "DETOK_" + tokenVal}
		}
	}

	processingDur := time.Now().UnixNano() - receiveTs

	// Log to CloudWatch
	if skyflowM != nil {
		lambdaOverheadMs := processingDur/1e6 - skyflowM.SkyflowWallMs
		log.Printf("METRIC query_id=%s batch_id=%s batch_size=%d operation=%s mode=%s duration_ms=%d "+
			"unique_tokens=%d dedup_pct=%.1f skyflow_calls=%d skyflow_wall_ms=%d "+
			"call_min_ms=%d call_avg_ms=%d call_max_ms=%d lambda_overhead_ms=%d errors=%d "+
			"invocation=%d instance=%s config=%s",
			queryID, batchID, batchSize, operation, mode, processingDur/1e6,
			skyflowM.UniqueTokens, skyflowM.DedupPct, skyflowM.SkyflowCalls, skyflowM.SkyflowWallMs,
			skyflowM.CallMinMs, skyflowM.CallAvgMs, skyflowM.CallMaxMs, lambdaOverheadMs, skyflowM.Errors,
			invNum, lambdaInstanceID, benchConfig)
	} else {
		log.Printf("METRIC query_id=%s batch_id=%s batch_size=%d operation=%s mode=%s duration_ms=%d invocation=%d instance=%s config=%s",
			queryID, batchID, batchSize, operation, mode, processingDur/1e6, invNum, lambdaInstanceID, benchConfig)
	}

	respBody, err := json.Marshal(resp)
	if err != nil {
		return events.APIGatewayProxyResponse{StatusCode: 500, Body: `{"error":"marshal failure"}`}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       string(respBody),
	}, nil
}

func main() {
	lambda.Start(handler)
}
