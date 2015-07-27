package stages

import (
	"io"

	"github.com/DailyBurn/ratchet"
)

// IoWriter is a stage that wraps any io.Writer objects.
// It can be used to write data out to a File, os.Stdout, or
// any other task that can be supported via io.Writer.
type IoWriter struct {
	Writer io.Writer
}

// NewIoWriter returns a new IoWriter wrapping the given io.Writer object
func NewIoWriter(writer io.Writer) *IoWriter {
	return &IoWriter{Writer: writer}
}

// HandleData - see interface for documentation.
func (w *IoWriter) HandleData(data ratchet.Data, outputChan chan ratchet.Data, killChan chan error) {
	bytesWritten, err := w.Writer.Write(data)
	ratchet.KillPipelineIfErr(err, killChan)
	ratchet.LogDebug("IoWriter:", bytesWritten, "bytes written")
}

// Finish - see interface for documentation.
func (w *IoWriter) Finish(outputChan chan ratchet.Data, killChan chan error) {
	if outputChan != nil {
		close(outputChan)
	}
}

func (w *IoWriter) String() string {
	return "IoWriter"
}
