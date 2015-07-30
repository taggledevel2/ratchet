package stages

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"

	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/util"
)

// CSVTransformer converts data.JSON objects into a CSV string object
// and sends it on to the next PipelineStage. In use-cases where
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
		header := []string{}
		for k := range objects[0] {
			header = append(header, k)
		}
		rows = append(rows, header)
		w.headerWritten = true
	}

	for _, object := range objects {
		row := []string{}
		for _, v := range object {
			row = append(row, fmt.Sprintf("%v", v))
		}
		rows = append(rows, row)
	}

	var b bytes.Buffer
	writer := csv.NewWriter(bufio.NewWriter(&b))

	err = writer.WriteAll(rows)
	util.KillPipelineIfErr(err, killChan)

	outputChan <- []byte(b.String())
}

func (w *CSVTransformer) Finish(outputChan chan data.JSON, killChan chan error) {
	if outputChan != nil {
		close(outputChan)
	}
}

func (w *CSVTransformer) String() string {
	return "CSVTransformer"
}
