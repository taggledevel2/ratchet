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

// NewData is a simple wrapper for json.Marshal. Use NewDataFromStruct
// if you are wanting to serialize a custom type.
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
