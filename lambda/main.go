package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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

func init() {
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
	// Parse request
	var sfReq sfRequest
	if err := json.Unmarshal([]byte(req.Body), &sfReq); err != nil {
		log.Printf("ERROR: failed to parse request body: %v", err)
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       fmt.Sprintf(`{"error": "invalid request body: %v"}`, err),
		}, nil
	}

	// batchSize := len(sfReq.Data)

	var resp sfResponse
	var respData [][]interface{}
	respData, skyflowErr := skyflowClient.Detokenize(ctx, sfReq.Data)
	if skyflowErr != nil {
		log.Printf("ERROR: Skyflow %s failed: %v", "detokenize", skyflowErr)
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       fmt.Sprintf(`{"error": "skyflow %s failed: %v"}`, "detokenize", skyflowErr),
		}, nil
	}
	resp = sfResponse{Data: respData}

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
