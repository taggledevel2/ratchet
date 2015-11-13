package processors

import (
	"bufio"
	"bytes"

	"github.com/dailyburn/ratchet/data"
	"github.com/dailyburn/ratchet/util"
)

// CSVTransformer converts data.JSON objects into a CSV string object
// and sends it on to the next stage. In use-cases where
// you simply want to write to a CSV file, use CSVWriter instead.
//
// CSVTransformer is for more complex use-cases where you need to
// generate CSV data and perhaps send it to multiple output stages.
type CSVTransformer struct {
	WriteHeader   bool
	headerWritten bool
}

// NewCSVTransformer returns a new CSVTransformer wrapping the given io.Writer object
func NewCSVTransformer() *CSVTransformer {
	return &CSVTransformer{WriteHeader: true, headerWritten: false}
}

func (w *CSVTransformer) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	// use util helper to convert Data into []map[string]interface{}
	objects, err := data.ObjectsFromJSON(d)
	util.KillPipelineIfErr(err, killChan)

	rows := [][]string{}
	if w.WriteHeader && !w.headerWritten {
		header_row := []string{}
		header := []string{}
		for k := range objects[0] {
			header = append(header, k)
			header_row = append(header_row, util.CSVString(k))
		}
		rows = append(rows, header_row)
		w.headerWritten = true
	}

	for _, object := range objects {
		row := []string{}
		for _, v := range object {
			row = append(row, util.CSVString(v))
		}
		rows = append(rows, row)
	}

	var b bytes.Buffer
	writer := util.NewCSVWriter(bufio.NewWriter(&b))

	err = writer.WriteAll(rows)
	util.KillPipelineIfErr(err, killChan)

	outputChan <- []byte(b.String())
}

func (w *CSVTransformer) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (w *CSVTransformer) String() string {
	return "CSVTransformer"
}
