package dynamodb_test

import (
	"goamz/aws"
	"goamz/dynamodb"
	"net/http"
	"testing"
	"time"
	"fmt"
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

func TestDerivedKey(t *testing.T){
	auth := &aws.Auth{"", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"}
	ti, _ := time.Parse("20060102", "20120215")
	sv   := &dynamodb.Service{"iam", "us-east-1"}	
	k    := sv.DerivedKey(auth, ti)
	actualKey   := fmt.Sprintf("%x", k)	
	expectedKey := "f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d"

	if actualKey != expectedKey {
		t.Log("Derived key does not match")
		t.Logf("Expected: %s", expectedKey)
		t.Logf("Actual:   %s", actualKey)
		t.FailNow()
	}
	
}

func TestSign(t *testing.T){
	r, _ := http.NewRequest("GET", "http://host.foo.com/", nil)

	r.Header.Set("Date", "Mon, 09 Sep 2011 23:36:00 GMT")

	auth := &aws.Auth{"AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"}

	sv := &dynamodb.Service{"host", "us-east-1"}

	err := sv.Sign(auth, r)
	
	if err != nil {
		t.Logf("Unexpected Error %s", err.Error())
		t.FailNow()
	}
	authHeader := r.Header.Get("Authorization")
	
	expectedAuth := "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, SignedHeaders=date;host, Signature=9a702034bd5925037e98565f48ec0de29d88a4d35526611798d21393cf04a3e3"
	
	if authHeader != expectedAuth {
		t.Logf("Authorization Does Not Match")
		t.Logf("Expected: %s", expectedAuth)
		t.Logf("Actual:   %s" , authHeader)
		t.FailNow()
	}
		
}

