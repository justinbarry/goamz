package dynamodb

import simplejson "github.com/bitly/go-simplejson"
import (
	"errors"
	"fmt"
)

type Table struct {
	Server *Server
	Name   string
	Key    PrimaryKey
}

func (s *Server) NewTable(name string, key PrimaryKey) *Table {
	return &Table{s, name, key}
}

func (s *Server) ListTables() ([]string, error) {
	var tables []string

	query := &Query{"{}"}

	jsonResponse, err := s.queryServer(target("ListTables"), query)

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

func tableParam(t *Table) string {
	return keyValue("TableName", t.Name)
}

func keyParam(k *PrimaryKey, hashKey string, rangeKey string) string {
	primaryKey := "{" + keyValue(k.KeyAttribute.Type, hashKey) + "}"
	value := "{\"HashKeyElement\":" + primaryKey

	if k.RangeAttribute != nil {
		value = fmt.Sprintf("%s,\"RangeKeyElement\":{%s}", value, keyValue(k.RangeAttribute.Type, rangeKey))

	}

	return "\"Key\":" + value + "}"
}

func keyValue(key string, value string) string {
	ret := fmt.Sprintf("\"%s\":\"%s\"", key, value)
	
	return ret
}



