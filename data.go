package ratchet

import (
	"encoding/json"
)

// Data is the generic type that is passed along data channels.
// Under the covers, Data is simply a []byte containing JSON data.
// The Data primitive is kept intentionally vague so in future updates
// marshallable types other than JSON can be used as well.
type Data []byte

// NewDataFromStruct is a simple wrapper for json.Marshal
func NewDataFromStruct(strct interface{}) (Data, error) {
	data, err := json.Marshal(strct)
	if err != nil {
		LogError("Data: failure to marshal data:", err.Error())
	}
	return data, err
}

// ParseDataIntoStructPtr is a simple wrapper for json.Unmarshal
func ParseDataIntoStructPtr(data Data, strctPtr interface{}) error {
	err := json.Unmarshal(data, strctPtr)
	if err != nil {
		LogError("Data: failure to unmarshal data:", err.Error())
	}
	return err
}
