// Package data holds custom types and functions for passing objects
// between ratchet stages. All output channels in ratchet starters and
// and stages are of type Data.
package data

import (
	"encoding/json"
	"fmt"
	"runtime/debug"

	"github.com/DailyBurn/ratchet/logger"
)

// JSON is the generic type that is passed along data channels.
// Under the covers, Data is simply a []byte containing JSON data.
// The Data primitive is kept intentionally vague so in future updates
// marshallable types other than JSON can be used as well.
type JSON []byte

// NewJSON is a simple wrapper for json.Marshal.
func NewJSON(v interface{}) (JSON, error) {
	d, err := json.Marshal(v)
	if err != nil {
		logger.Error(fmt.Sprintf("data: failure to marshal JSON %+v - error is \"%v\"", v, err.Error()))
		logger.Debug(string(debug.Stack()))
	}
	return d, err
}

// ParseJSON is a simple wrapper for json.Unmarshal
func ParseJSON(d JSON, v interface{}) error {
	err := json.Unmarshal(d, v)
	if err != nil {
		logger.Error(fmt.Sprintf("data: failure to unmarshal JSON into %+v - error is \"%v\"", v, err.Error()))
		logger.Debug(fmt.Sprintf("	Failed Data: %+v", string(d)))
		logger.Debug(string(debug.Stack()))
	}
	return err
}

// ObjectsFromJSON is a helper for parsing a JSON into a slice of
// generic maps/objects. The use-case is when a stage is expecting
// to receive either a JSON object or an array of JSON objects, and
// want to deal with it in a generic fashion.
func ObjectsFromJSON(d JSON) ([]map[string]interface{}, error) {
	var v interface{}
	err := ParseJSON(d, &v)
	if err != nil {
		return nil, err
	}

	var objects []map[string]interface{}
	// check if we have a single object or a slice of objects
	switch vv := v.(type) {
	case []interface{}:
		for _, o := range vv {
			objects = append(objects, o.(map[string]interface{}))
		}
	case map[string]interface{}:
		objects = []map[string]interface{}{vv}
	case []map[string]interface{}:
		objects = vv
	default:
		return nil, fmt.Errorf("ObjectsFromJSON: unsupported data type: %T", vv)
	}

	return objects, nil
}

// JSONFromHeaderAndRows takes the given header and rows of values, and
// turns it into a JSON array of objects.
func JSONFromHeaderAndRows(header []string, rows [][]interface{}) JSON {
	// There may be a better way to do this?
	jsonStr := "["
	for i, row := range rows {
		if i > 0 {
			jsonStr += ","
		}
		jsonStr += "{"
		for j, v := range row {
			if j > 0 {
				jsonStr += ","
			}
			// type check for string to see if it needs quotes
			switch vv := v.(type) {
			case string:
				jsonStr += fmt.Sprintf("\"%v\":\"%v\"", header[j], vv)
			default:
				jsonStr += fmt.Sprintf("\"%v\":%v", header[j], vv)
			}
		}
		jsonStr += "}"
	}
	jsonStr += "]"

	return JSON(jsonStr)
}
