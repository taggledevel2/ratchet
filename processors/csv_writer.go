package processors

import (
	"io"
	"sort"

	"github.com/dailyburn/ratchet/data"
	"github.com/dailyburn/ratchet/util"
)

// CSVWriter is handles converting data.JSON objects into CSV format,
// and writing them to the given io.Writer. The Data
// must be a valid JSON object or a slice of valid JSON objects.
// If you already have Data formatted as a CSV string you can
// use an IoWriter instead.
type CSVWriter struct {
	Writer        *util.CSVWriter
	WriteHeader   bool
	headerWritten bool
	Header        []string
}

// NewCSVWriter returns a new CSVWriter wrapping the given io.Writer object
func NewCSVWriter(w io.Writer) *CSVWriter {
	return &CSVWriter{Writer: util.NewCSVWriter(w), WriteHeader: true, headerWritten: false}
}

func (w *CSVWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	// use util helper to convert Data into []map[string]interface{}
	objects, err := data.ObjectsFromJSON(d)
	util.KillPipelineIfErr(err, killChan)

	if w.Header == nil {
		for k := range objects[0] {
			w.Header = append(w.Header, k)
		}
		sort.Strings(w.Header)
	}

	rows := [][]string{}

	if w.WriteHeader && !w.headerWritten {
		header_row := []string{}
		for _, k := range w.Header {
			header_row = append(header_row, util.CSVString(k))
		}
		rows = append(rows, header_row)
		w.headerWritten = true
	}

	for _, object := range objects {
		row := []string{}
		for i := range w.Header {
			v := object[w.Header[i]]
			row = append(row, util.CSVString(v))
		}
		rows = append(rows, row)
	}

	err = w.Writer.WriteAll(rows)
	util.KillPipelineIfErr(err, killChan)
}

func (w *CSVWriter) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (w *CSVWriter) String() string {
	return "CSVWriter"
}
