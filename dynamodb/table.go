package dynamodb

import simplejson "github.com/bitly/go-simplejson"
import (
	"errors"
	"fmt"
	)

type Table struct {
	Name string
	Key PrimaryKey
	ReadCapacity int
	WriteCapacity int
}

func New(name string, key PrimaryKey, readCapacity int, writeCapacity int) *Table {
	return &Table{name, key, readCapacity, writeCapacity}
}

func (s *Server) ListTables(query *Query) ([]string, error) {
	var tables []string
	var params map[string][]string

	jsonResponse, err := s.queryServer(target("ListTables"), params, query)

	if err != nil {
		return nil, err
	}

	json, err := simplejson.NewJson(jsonResponse)

	if err != nil {
		return nil, err
	}

	response, err := json.Get("TableNames").Array()

	if err != nil {
		message := fmt.Sprintf("Unexpected response %s", jsonResponse)
		return nil, errors.New(message)
	}

	for _, value := range response {
		if t, ok := (value).(string); ok {
			tables = append(tables, t)
		}
	}

	return tables, nil
}

func target(name string) string {
	return "DynamoDB_20111205." + name
}


