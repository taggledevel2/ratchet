package starters

import (
	"io/ioutil"

	"github.com/DailyBurn/ratchet"
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
func (r *FileReader) Start(outputChan chan ratchet.Data, killChan chan error) {
	data, err := ioutil.ReadFile(r.filename)
	ratchet.KillPipelineIfErr(err, killChan)
	outputChan <- data
	close(outputChan)
}

func (r *FileReader) String() string {
	return "FileReader"
}
