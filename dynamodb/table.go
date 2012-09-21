package dynamodb

import simplejson "github.com/bitly/go-simplejson"
import (
	"errors"
	"fmt"
)

type Table struct {
	Server        *Server
	Name          string
	Key           PrimaryKey
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

func (t *Table) GetItem(hashKey string, rangeKey string) (map[string] *Attribute, error) {
	queryParts := []string{
		tableParam(t),
		keyParam(&t.Key, hashKey, rangeKey),
	}

	q := NewQuery(queryParts)
	
	jsonResponse, err := t.Server.queryServer(target("GetItem"), q)

	if err != nil {
		return nil, err
	}

	json, err := simplejson.NewJson(jsonResponse)

	if err != nil {
		return nil, err
	}

	item, err := json.Get("Item").Map()

	if err != nil {
		message := fmt.Sprintf("Unexpected response %s", jsonResponse)
		return nil, errors.New(message)
	}

	return parseAttributes(item), nil

}

func tableParam(t *Table) string {
	return keyValue("TableName", t.Name)
}

func keyParam(k *PrimaryKey, hashKey string, rangeKey string) string {
	primaryKey := "{" + keyValue(k.KeyAttribute.Type, hashKey) + "}"
	value := "{\"HashKeyElement\":" + primaryKey 

	if k.RangeAttribute != nil {
		value = value + "," + "\"RangeKeyElement\": {" + keyValue(k.RangeAttribute.Type, rangeKey) + "}"
		
	}
	
	return "\"Key\":" + value + "}"
}
	
func consistentRead(consistent bool) string {
	value := "true"
	if !consistent {
		value = "false"
	}
	return keyValue("ConsistentRead", value)
}

func keyValue(key string, value string) string {
	return "\"" + key + "\":\"" + value + "\""
}

func target(name string) string {
	return "DynamoDB_20111205." + name
}

func parseAttributes(s map[string] interface{}) map[string] *Attribute {
	results := map[string] *Attribute {}

	for key, value := range s {		
		if v, ok := value.(map[string] interface{}); ok {
			if val, ok  := v[TYPE_STRING].(string); ok  {
				attr := &Attribute{
					TYPE_STRING,
					key,
					val,
				}
				results[key] = attr
			}
		} else {
			fmt.Printf("type assertion to map[string]string failed for : %s\n ", value)
		}

	}

	return results
}
