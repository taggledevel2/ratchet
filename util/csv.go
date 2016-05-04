package util

import (
	"bufio"
	"bytes"
	"fmt"
	"sort"

	"github.com/dailyburn/ratchet/data"
)

func CSVString(v interface{}) string {
	switch v.(type) {
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}

type CSVParameters struct {
	Writer        *CSVWriter
	WriteHeader   bool
	HeaderWritten bool
	Header        []string
	SendUpstream  bool
	QuoteEscape   string
}

func CSVProcess(params *CSVParameters, d data.JSON, outputChan chan data.JSON, killChan chan error) {
	objects, err := data.ObjectsFromJSON(d)
	KillPipelineIfErr(err, killChan)

	if params.Header == nil {
		for k := range objects[0] {
			params.Header = append(params.Header, k)
		}
		sort.Strings(params.Header)
	}

	rows := [][]string{}

	if params.WriteHeader && !params.HeaderWritten {
		header_row := []string{}
		for _, k := range params.Header {
			header_row = append(header_row, CSVString(k))
		}
		rows = append(rows, header_row)
		params.HeaderWritten = true
	}

	for _, object := range objects {
		row := []string{}
		for i := range params.Header {
			v := object[params.Header[i]]
			row = append(row, CSVString(v))
		}
		rows = append(rows, row)
	}

	if params.SendUpstream {
		var b bytes.Buffer
		params.Writer.SetWriter(bufio.NewWriter(&b))

		err = params.Writer.WriteAll(rows)
		KillPipelineIfErr(err, killChan)

		outputChan <- []byte(b.String())
	} else {
		err = params.Writer.WriteAll(rows)
		KillPipelineIfErr(err, killChan)
	}
}
