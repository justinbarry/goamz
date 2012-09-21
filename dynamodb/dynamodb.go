package dynamodb

import (
	"fmt"
	"goamz/aws"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

const (
	TYPE_STRING = "S"
	TYPE_NUMBER = "N"
	TYPE_BIN    = "B"
	)

type Server struct {
	Auth   aws.Auth
	Region aws.Region
}

type Query struct {
	Query string
}

func NewQuery(queryParts []string) *Query {
	return &Query{
		"{" + strings.Join(queryParts, ",") +"}",
	}
}

type PrimaryKey struct {
	KeyAttribute   *Attribute
	RangeAttribute *Attribute
}

type Attribute struct {
	Type  string
	Name string
	Value string
}

func NewStringAttribute(name string, value string) *Attribute {
	return &Attribute{ TYPE_STRING,
		name,
		value,
	}
}

func NewNumericAttribute(name string, value string) *Attribute {
	return &Attribute{ TYPE_NUMBER,
		name,
		value,
	}
}

func NewBinaryAttribute(name string, value string) *Attribute {
	return &Attribute{ TYPE_BIN,
		name,
		value,
	}
}

func (s *Server) queryServer(target string, query *Query) ([]byte, error) {
	data := strings.NewReader(query.Query)
	hreq, err := http.NewRequest("POST", s.Region.DynamoDBEndpoint +"/", data)

	if err != nil {
		return nil, err
	}
	
	hreq.Header.Set("Date", requestDate())
	hreq.Header.Set("Content-Type", "application/x-amz-json-1.0")
	hreq.Header.Set("X-Amz-Target", target)

	service := Service{
		"dynamodb",
		s.Region.Name,
	}

	err = service.Sign(&s.Auth, hreq)

	if err == nil {

		resp, err := http.DefaultClient.Do(hreq)

		if err != nil {
			fmt.Printf("Error calling Amazon")
			return nil, err
		}

		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)

		if err != nil {
			fmt.Printf("Could not read response body")
			return nil, err
		}

		return body, nil

	}

	return nil, err

}

func requestDate() string {
	now := time.Now().UTC()
	return now.Format(http.TimeFormat)
}
