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

// SfRow is a strongly-typed Snowflake external function row [rowNum, value].
// Custom JSON avoids [][]interface{} allocations and type assertions.
type SfRow struct {
	RowNum json.RawMessage // preserve original row number exactly as Snowflake sent it
	Value  string
}

func (r *SfRow) UnmarshalJSON(data []byte) error {
	// Expect JSON array: [rowNum, "value"]
	if len(data) < 2 || data[0] != '[' {
		return fmt.Errorf("expected JSON array, got %c", data[0])
	}
	depth := 0
	commaIdx := -1
	for i := 1; i < len(data)-1; i++ {
		switch data[i] {
		case '[', '{':
			depth++
		case ']', '}':
			depth--
		case ',':
			if depth == 0 {
				commaIdx = i
				break
			}
		}
		if commaIdx >= 0 {
			break
		}
	}
	if commaIdx < 0 {
		return fmt.Errorf("expected [rowNum, value], no comma found")
	}
	r.RowNum = json.RawMessage(data[1:commaIdx])
	return json.Unmarshal(data[commaIdx+1:len(data)-1], &r.Value)
}

func (r SfRow) MarshalJSON() ([]byte, error) {
	valBytes, err := json.Marshal(r.Value)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 1+len(r.RowNum)+1+len(valBytes)+1)
	buf = append(buf, '[')
	buf = append(buf, r.RowNum...)
	buf = append(buf, ',')
	buf = append(buf, valBytes...)
	buf = append(buf, ']')
	return buf, nil
}

type sfRequest struct {
	Data []SfRow `json:"data"`
}

type sfResponse struct {
	Data []SfRow `json:"data"`
}

var lambdaInstanceID string

func init() {
	lambdaInstanceID = fmt.Sprintf("%d", time.Now().UnixNano())

	// Initialize Skyflow client (nil if SKYFLOW_DATA_PLANE_URL not set → mock mode)
	skyflowCfg := loadSkyflowConfig()
	if skyflowCfg != nil {
		skyflowClient = NewSkyflowClient(*skyflowCfg)
		log.Printf("INFO: Skyflow mode enabled (url=%s, grpc=%s, vault=%s, batch=%d, concurrency=%d)",
			skyflowCfg.DataPlaneURL, skyflowCfg.GRPCEndpoint, skyflowCfg.VaultID, skyflowCfg.BatchSize, skyflowCfg.MaxConcurrency)
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
		var skyflowErr error
		switch operation {
		case "detokenize":
			var respData []SfRow
			respData, skyflowM, skyflowErr = skyflowClient.Detokenize(ctx, sfReq.Data)
			if skyflowErr != nil {
				log.Printf("ERROR: Skyflow %s failed: %v", operation, skyflowErr)
				return events.APIGatewayProxyResponse{
					StatusCode: 500,
					Body:       fmt.Sprintf(`{"error": "skyflow %s failed: %v"}`, operation, skyflowErr),
				}, nil
			}
			resp = sfResponse{Data: respData}
		default:
			return events.APIGatewayProxyResponse{
				StatusCode: 400,
				Body:       fmt.Sprintf(`{"error": "unknown operation: %s"}`, operation),
			}, nil
		}
	} else {
		// Mock mode: echo rows back with DETOK_ prefix
		seen := make(map[string]struct{}, batchSize)
		resp = sfResponse{Data: make([]SfRow, batchSize)}
		for i, row := range sfReq.Data {
			seen[row.Value] = struct{}{}
			resp.Data[i] = SfRow{RowNum: row.RowNum, Value: "DETOK_" + row.Value}
		}
		uniqueTokens := len(seen)
		dedupPct := 0.0
		if batchSize > 0 {
			dedupPct = (1 - float64(uniqueTokens)/float64(batchSize)) * 100
		}
		skyflowM = &SkyflowMetrics{
			UniqueTokens: uniqueTokens,
			DedupPct:     dedupPct,
		}
	}

	processingDur := time.Now().UnixNano() - receiveTs

	// Log to CloudWatch (skyflowM is always set — both Skyflow and mock modes populate it)
	lambdaOverheadMs := processingDur/1e6 - skyflowM.SkyflowWallMs
	log.Printf("METRIC query_id=%s batch_id=%s batch_size=%d operation=%s mode=%s duration_ms=%d "+
		"unique_tokens=%d dedup_pct=%.1f skyflow_calls=%d skyflow_wall_ms=%d "+
		"call_min_ms=%d call_avg_ms=%d call_max_ms=%d lambda_overhead_ms=%d errors=%d "+
		"invocation=%d instance=%s config=%s",
		queryID, batchID, batchSize, operation, mode, processingDur/1e6,
		skyflowM.UniqueTokens, skyflowM.DedupPct, skyflowM.SkyflowCalls, skyflowM.SkyflowWallMs,
		skyflowM.CallMinMs, skyflowM.CallAvgMs, skyflowM.CallMaxMs, lambdaOverheadMs, skyflowM.Errors,
		invNum, lambdaInstanceID, benchConfig)

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
