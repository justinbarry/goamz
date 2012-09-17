package dynamodb

import (
	"goamz/aws"
	"net/http"
	"time"
	"io/ioutil"
	"net/url"
	"strings"
	"fmt"
	)

type Server struct {
	Auth   aws.Auth
	Region aws.Region
}

type Query struct {
	Query string
}

type PrimaryKey struct {
	Key string
	RangeAttribute ValueType
}

type ValueType struct {
	Value string
}

type Item struct {
	Key PrimaryKey
	Attributes []Attribute
}

type Attribute struct {
	Name string
	Value ValueType
}

func (s *Server) queryServer(target string, params url.Values, query *Query) ([]byte, error) {
	url, err := url.Parse(s.Region.DynamoDBEndpoint)

	headers := map[string][]string{
		"content-type": {"application/x-amz-json-1.0"},
		"Date": {requestDate()},
		"x-amz-target": {target},
	}

	if err != nil {
		return nil, err
	}

	hreq := http.Request {
	URL: url,
    	Method: "POST",
	ProtoMajor: 1,
	ProtoMinor: 1,
        Close: true,
	Header: headers,
	}

	reader    := strings.NewReader(query.Query)
	hreq.Body = ioutil.NopCloser(reader)

	service := Service{
		"dynamodb",
		s.Region.Name,
	}

	err = service.Sign(&s.Auth, &hreq)

	if err == nil {
		resp, err := http.DefaultClient.Do(&hreq)

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
