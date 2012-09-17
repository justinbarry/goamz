package dynamodb_test

import (
	"goamz/aws"
	"goamz/dynamodb"
	"net/http"
	"testing"
)

func TestError(t *testing.T) {
	r, _ := http.NewRequest("POST", "http://example.com", nil)

	auth := &aws.Auth{"", ""}

	sv := &dynamodb.Service{"dynamodb", aws.USEast.Name}
	err := sv.Sign(auth, r)

	if err != dynamodb.ErrNoDate {
		t.Log(err.Error())
		t.FailNow()
	}
}

