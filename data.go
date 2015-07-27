package ratchet

import (
	"encoding/json"
	"fmt"
	"runtime/debug"
)

// Data is the generic type that is passed along data channels.
// Under the covers, Data is simply a []byte containing JSON data.
// The Data primitive is kept intentionally vague so in future updates
// marshallable types other than JSON can be used as well.
type Data []byte

// NewData is a simple wrapper for json.Marshal.
func NewData(v interface{}) (Data, error) {
	data, err := json.Marshal(v)
	if err != nil {
		LogError(fmt.Sprintf("Data: failure to marshal data %+v - error is \"%v\"", v, err.Error()))
		LogDebug(string(debug.Stack()))
	}
	return data, err
}

// ParseData is a simple wrapper for json.Unmarshal
func ParseData(data Data, v interface{}) error {
	err := json.Unmarshal(data, v)
	if err != nil {
		LogError(fmt.Sprintf("Data: failure to unmarshal data into %+v - error is \"%v\"", v, err.Error()))
		LogDebug(fmt.Sprintf("	Failed Data: %+v", string(data)))
		LogDebug(string(debug.Stack()))
	}
	return err
}

// ObjectsFromData is a helper for parsing a Data into a slice of
// generic maps/objects. The use-case is when a stage is expecting
// to receive either a JSON object or an array of JSON objects, and
// want to deal with it in a generic fashion.
func ObjectsFromData(data Data) ([]map[string]interface{}, error) {
	var v interface{}
	err := ParseData(data, &v)
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
		return nil, fmt.Errorf("ObjectsFromData: unsupported data type: %T", vv)
	}

	return objects, nil
}
