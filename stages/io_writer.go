package stages

import (
	"io"

	"github.com/DailyBurn/ratchet"
)

// IoWriter is a simple stage that wraps an io.Writer.
// It can be used to write data out to a file, or any other
// task that can be supported via io.Writer.
type IoWriter struct {
	Writer io.Writer
}

// NewIoWriter returns a new IoWriter wrapping the given io.Writer object
func NewIoWriter(writer io.Writer) *IoWriter {
	return &IoWriter{Writer: writer}
}

// HandleData - see interface in stages.go for documentation.
func (w *IoWriter) HandleData(data ratchet.Data, outputChan chan ratchet.Data, killChan chan error) {
	bytesWritten, err := w.Writer.Write(data)
	if err != nil {
		ratchet.LogError("IoWriter:", err.Error())
		killChan <- err
	} else {
		ratchet.LogDebug("IoWriter:", bytesWritten, "bytes written")
	}
}

// Finish - see interface in stages.go for documentation.
func (w *IoWriter) Finish(outputChan chan ratchet.Data, killChan chan error) {
	if outputChan != nil {
		close(outputChan)
	}
}

func (w *IoWriter) String() string {
	return "IoWriter"
}
