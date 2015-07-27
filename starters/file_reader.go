// Package starters holds PipelineStarter implementations that
// are generic and potentially useful across any ETL project.
package starters

import (
	"io/ioutil"

	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/util"
)

// FileReader is a simple stage that wraps an io.Reader.
// For reading files, it's better to use the FileReader.
type FileReader struct {
	filename string
}

// NewFileReader returns a new FileReader that will read the entire contents
// of the given file path and send it at once. For buffered reading try using
// IoReader.
func NewFileReader(filename string) *FileReader {
	return &FileReader{filename: filename}
}

// Start - see interface in stages.go for documentation.
func (r *FileReader) Start(outputChan chan data.JSON, killChan chan error) {
	d, err := ioutil.ReadFile(r.filename)
	util.KillPipelineIfErr(err, killChan)
	outputChan <- d
	close(outputChan)
}

func (r *FileReader) String() string {
	return "FileReader"
}
