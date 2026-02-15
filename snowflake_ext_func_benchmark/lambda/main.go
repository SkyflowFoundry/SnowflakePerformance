package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

var (
	ddbClient       *dynamodb.Client
	tableName       string
	simulatedDelay  time.Duration
	invocationCount atomic.Int64
)

type sfRequest struct {
	Data [][]interface{} `json:"data"`
}

type sfResponse struct {
	Data [][]interface{} `json:"data"`
}

type metricRecord struct {
	QueryID            string `dynamodbav:"query_id"`
	SortKey            string `dynamodbav:"sk"`
	BatchID            string `dynamodbav:"batch_id"`
	BatchSize          int    `dynamodbav:"batch_size"`
	ReceiveTimestampNs int64  `dynamodbav:"receive_timestamp_ns"`
	ProcessingDurNs    int64  `dynamodbav:"processing_duration_ns"`
	BenchmarkConfig    string `dynamodbav:"benchmark_config"`
	InvocationNum      int64  `dynamodbav:"invocation_num"`
	LambdaInstanceID   string `dynamodbav:"lambda_instance_id"`
}

var lambdaInstanceID string

func init() {
	lambdaInstanceID = fmt.Sprintf("%d", time.Now().UnixNano())

	tableName = os.Getenv("DYNAMODB_TABLE")
	if tableName == "" {
		tableName = "ext_func_benchmark_metrics"
	}

	delayStr := os.Getenv("SIMULATED_DELAY_MS")
	if delayStr != "" {
		ms, err := strconv.Atoi(delayStr)
		if err == nil && ms > 0 {
			simulatedDelay = time.Duration(ms) * time.Millisecond
		}
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Printf("WARN: failed to load AWS config for DynamoDB: %v", err)
		return
	}
	ddbClient = dynamodb.NewFromConfig(cfg)
}

func handler(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	receiveTs := time.Now().UnixNano()
	invNum := invocationCount.Add(1)

	// Extract Snowflake headers
	queryID := req.Headers["sf-external-function-current-query-id"]
	batchID := req.Headers["sf-external-function-query-batch-id"]
	benchConfig := req.Headers["sf-benchmark-config"]

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

	// Simulate API latency
	if simulatedDelay > 0 {
		time.Sleep(simulatedDelay)
	}

	// Build response: prepend DETOK_ to each token value
	resp := sfResponse{Data: make([][]interface{}, batchSize)}
	for i, row := range sfReq.Data {
		if len(row) < 2 {
			resp.Data[i] = []interface{}{i, "DETOK_ERROR_MISSING_VALUE"}
			continue
		}
		rowNum := row[0]
		tokenVal := fmt.Sprintf("%v", row[1])
		resp.Data[i] = []interface{}{rowNum, "DETOK_" + tokenVal}
	}

	processingDur := time.Now().UnixNano() - receiveTs

	// Log to CloudWatch (synchronous, cheap)
	log.Printf("METRIC query_id=%s batch_id=%s batch_size=%d config=%s duration_ns=%d invocation=%d instance=%s",
		queryID, batchID, batchSize, benchConfig, processingDur, invNum, lambdaInstanceID)

	// Synchronous DynamoDB write — ensures metrics land before Lambda freezes.
	// Adds ~5ms per invocation (acceptable for benchmark accuracy).
	if ddbClient != nil {
		record := metricRecord{
			QueryID:            queryID,
			SortKey:            fmt.Sprintf("%s#%d", batchID, receiveTs),
			BatchID:            batchID,
			BatchSize:          batchSize,
			ReceiveTimestampNs: receiveTs,
			ProcessingDurNs:    processingDur,
			BenchmarkConfig:    benchConfig,
			InvocationNum:      invNum,
			LambdaInstanceID:   lambdaInstanceID,
		}
		item, err := attributevalue.MarshalMap(record)
		if err != nil {
			log.Printf("WARN: failed to marshal DynamoDB item: %v", err)
		} else {
			tbl := tableName
			_, err = ddbClient.PutItem(context.Background(), &dynamodb.PutItemInput{
				TableName: &tbl,
				Item:      item,
			})
			if err != nil {
				log.Printf("WARN: failed to write to DynamoDB: %v", err)
			}
		}
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
