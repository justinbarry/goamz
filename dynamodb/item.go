package dynamodb

import simplejson "github.com/bitly/go-simplejson"
import (
	"errors"
	"fmt"
)

func (t *Table) GetItem(hashKey string, rangeKey string) (map[string]*Attribute, error) {
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

func (t *Table) PutItem(hashKey string, rangeKey string, attributes []Attribute) (bool, error) {

	if len(attributes) == 0 {
		return false, errors.New("At least one attribute is required.")
	}

	queryParts := []string{
		tableParam(t),
		itemParam(&t.Key, hashKey, rangeKey, attributes),
	}

	fmt.Printf("query : %s", queryParts)
	q := NewQuery(queryParts)

	jsonResponse, err := t.Server.queryServer(target("PutItem"), q)

	if err != nil {
		return false, err
	}

	json, err := simplejson.NewJson(jsonResponse)

	if err != nil {
		return false, err
	}

	_, err = json.Get("ConsumedCapacityUnits").Map()

	if err != nil {
		message := fmt.Sprintf("Unexpected response %s", jsonResponse)
		return false, errors.New(message)
	}

	return true, nil
}

// Example Request Json
//    "Item":{
//        "AttributeName1":{"S":"AttributeValue1"},
//        "AttributeName2":{"N":"AttributeValue2"},
//        "AttributeName5":{"B":"dmFsdWU="}
//    },
//    "Expected":{"AttributeName3":{"Value": {"S":"AttributeValue"}, "Exists":Boolean}},
//    "ReturnValues":"ReturnValuesConstant"}

func itemParam(k *PrimaryKey, hashKey string, rangeKey string, attributes []Attribute) string {

	result := "\"Item\":{" +
		k.KeyAttribute.Name +
		"{" +
		keyValue(k.KeyAttribute.Type, hashKey) +
		"}"

	if k.RangeAttribute != nil {
		result = result + "," +
			k.RangeAttribute.Name +
			": {" +
			keyValue(k.RangeAttribute.Type, rangeKey) +
			"}"
	}

	for _, attribute := range attributes {
		result = result + "," +
			attribute.Name + ": {" +
			keyValue(attribute.Type, attribute.Value) + "}"
	}

	return result + "}"
}

func parseAttributes(s map[string]interface{}) map[string]*Attribute {
	results := map[string]*Attribute{}

	for key, value := range s {
		if v, ok := value.(map[string]interface{}); ok {
			if val, ok := v[TYPE_STRING].(string); ok {
				attr := &Attribute{
					TYPE_STRING,
					key,
					val,
				}
				results[key] = attr
			}
		} else {
			fmt.Printf("type assertion to map[string] interface{} failed for : %s\n ", value)
		}

	}

	return results
}
