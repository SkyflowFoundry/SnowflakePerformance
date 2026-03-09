package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

var skyflowClient *SkyflowClient

func init() {
	skyflowCfg := loadSkyflowConfig()
	if skyflowCfg != nil {
		skyflowClient = NewSkyflowClient(*skyflowCfg)
	}
}

func handler(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var requestData [][]interface{}
	if err := json.Unmarshal([]byte(req.Body), &map[string]interface{}{"data": &requestData}); err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       fmt.Sprintf(`{"error": "invalid request: %v"}`, err),
		}, nil
	}

	if skyflowClient == nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       `{"error": "skyflow client not configured"}`,
		}, nil
	}

	result, err := skyflowClient.Detokenize(ctx, requestData)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       fmt.Sprintf(`{"error": "detokenize failed: %v"}`, err),
		}, nil
	}

	respBody, err := json.Marshal(map[string]interface{}{"data": result})
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       `{"error": "response encoding failed"}`,
		}, nil
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
