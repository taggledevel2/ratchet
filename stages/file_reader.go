package stages

import (
	"io/ioutil"

	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/util"
)

// FileReader is an initial PipelineStage that reads the given file.
type FileReader struct {
	filename string
}

// NewFileReader returns a new FileReader that will read the entire contents
// of the given file path and send it at once. For buffered reading try using
// IoReader.
func NewFileReader(filename string) *FileReader {
	return &FileReader{filename: filename}
}

// ProcessData - see interface for documentation.
func (r *FileReader) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	d, err := ioutil.ReadFile(r.filename)
	util.KillPipelineIfErr(err, killChan)
	outputChan <- d
}

// Finish - see interface for documentation.
func (r *FileReader) Finish(outputChan chan data.JSON, killChan chan error) {
	close(outputChan)
}

func (r *FileReader) String() string {
	return "FileReader"
}
