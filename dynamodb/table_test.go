package dynamodb_test

import (
	"flag"
	"goamz/aws"
	"goamz/dynamodb"
	"testing"
	"fmt"
	"time"
	)

var amazon = flag.Bool("amazon", false, "Enable tests against amazon server")

func TestListTables(t *testing.T){
	if !*amazon {
		t.Log("Amazon tests not enabled")
		return
	}

	auth, err := aws.EnvAuth()

	if err != nil {
		t.Log(err)
		t.FailNow()
	}

	server := dynamodb.Server{auth, aws.USEast}
	query  := &dynamodb.Query{""}
	tables, err := server.ListTables(query)

	now := time.Now()
	isoDate := now.Format(time.RFC3339)

	fmt.Printf("\nDate is: %s", isoDate)

	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	fmt.Printf("tables %x", tables)
	
}
