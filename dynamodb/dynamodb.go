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
		"contnt-type": {"application/x-amz-json-1.0"},
		"x-amz-date": {requestDate()},
		"x-amz-target": {target},
	}

	if err != nil {
		return nil, err
	}
	
	headers["Authorization"] = s.Sign(url.Host, params, headers)

	hreq := http.Request{
	        URL: url,
    	        Method: "POST",
	        ProtoMajor: 1,
	        ProtoMinor: 1,
         	Close: true,
	        Header: headers,
	}

	if query.Query != "" {
		reader    := strings.NewReader(query.Query)
		hreq.Body = ioutil.NopCloser(reader)
	}
	
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

func requestDate() string {	
	now := time.Now()
	return now.Format(time.RFC3339)
}
