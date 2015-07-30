package stages

import (
	"fmt"
	"io"

	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/logger"
	"github.com/DailyBurn/ratchet/util"
)

// IoWriter wraps any io.Writer objects.
// It can be used to write data out to a File, os.Stdout, or
// any other task that can be supported via io.Writer.
type IoWriter struct {
	Writer     io.Writer
	AddNewline bool
}

// NewIoWriter returns a new IoWriter wrapping the given io.Writer object
func NewIoWriter(writer io.Writer) *IoWriter {
	return &IoWriter{Writer: writer, AddNewline: false}
}

func (w *IoWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	var bytesWritten int
	var err error
	if w.AddNewline {
		bytesWritten, err = fmt.Fprintln(w.Writer, string(d))
	} else {
		bytesWritten, err = w.Writer.Write(d)
	}
	util.KillPipelineIfErr(err, killChan)
	logger.Debug("IoWriter:", bytesWritten, "bytes written")
}

func (w *IoWriter) Finish(outputChan chan data.JSON, killChan chan error) {
	if outputChan != nil {
		close(outputChan)
	}
}

func (w *IoWriter) String() string {
	return "IoWriter"
}
