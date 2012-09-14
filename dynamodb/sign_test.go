package dynamodb_test

import (
	"goamz/aws"
	"goamz/dynamodb"
	"testing"
)

var testAuth = aws.Auth{"AccessKeyId", "secret-access-key-UkQjTld9"}

var server = dynamodb.Server{testAuth, aws.USEast}

func TestSign(t *testing.T) {
	params := map[string][]string{	}

	headers := map[string][]string{		
		"x-amz-date":   {"Mon, 16 Jan 2012 17:49:52 GMT"},
		"x-amz-target": {"DynamoDB_20111205.CreateTable"},
		"content-type": {"application/x-amz-json-1.0"},
	}
	
	actual   := server.Sign("dynamodb.us-east-1.amazonaws.com", params, headers)
	expected := "AWS4-HMAC-SHA256 Credential=AccessKeyId/20120116/us-east-1/dynamodb/aws4_request,SignedHeaders=host;x-amz-date;x-amz-target," + 
		"Signature=CCBOCnTVN10+r9D8HI7WpaYjyaDUQewTFDbSWyU7dRY="

	if actual == nil || actual[0] != expected {
		t.Errorf("Unexpected Authorization %s", actual)
	}

}

func BenchmarkSign(b *testing.B) {
	params := map[string][]string{
		"x-amz-date":   {"Mon, 16 Jan 2012 17:49:52 GMT"},
		"x-amz-target": {"DynamoDB_20111205.CreateTable"},
	}

	headers := map[string][]string{
		"content-type": {"application/x-amz-json-1.0"},
	}

	for i := 0; i < b.N; i++ {
		server.Sign("dynamodb.us-east-1.amazonaws.com", params, headers)
	}

}
