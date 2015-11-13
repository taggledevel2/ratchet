package processors

import (
	"io"

	"github.com/dailyburn/ratchet/data"
	"github.com/dailyburn/ratchet/util"
)

// CSVWriter is handles converting data.JSON objects into CSV format,
// and writing them to the given io.Writer. The Data
// must be a valid JSON object or a slice of valid JSON objects.
// If you already have Data formatted as a CSV string you can
// use an IoWriter instead.
type CSVWriter struct {
	writer        *util.CSVWriter
	WriteHeader   bool
	headerWritten bool
	header        []string
}

// NewCSVWriter returns a new CSVWriter wrapping the given io.Writer object
func NewCSVWriter(w io.Writer) *CSVWriter {
	return &CSVWriter{writer: util.NewCSVWriter(w), WriteHeader: true, headerWritten: false}
}

func (w *CSVWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	// use util helper to convert Data into []map[string]interface{}
	objects, err := data.ObjectsFromJSON(d)
	util.KillPipelineIfErr(err, killChan)

	header_row := []string{}
	if w.header == nil {
		for k := range objects[0] {
			w.header = append(w.header, k)
			header_row = append(header_row, util.CSVString(k))
		}
	}

	rows := [][]string{}
	if w.WriteHeader && !w.headerWritten {
		rows = append(rows, header_row)
		w.headerWritten = true
	}

	for _, object := range objects {
		row := []string{}
		for i := range w.header {
			v := object[w.header[i]]
			row = append(row, util.CSVString(v))
		}
		rows = append(rows, row)
	}

	err = w.writer.WriteAll(rows)
	util.KillPipelineIfErr(err, killChan)
}

func (w *CSVWriter) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (w *CSVWriter) String() string {
	return "CSVWriter"
}
