package util

import (
	"fmt"
)

func CSVString(v interface{}) string {
	switch v.(type) {
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}
