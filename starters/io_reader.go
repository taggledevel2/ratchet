package starters

import (
	"bufio"
	"io"

	"github.com/DailyBurn/ratchet"
)

// IoReader is a simple stage that wraps an io.Reader.
// For reading files, it's better to use the FileReader.
type IoReader struct {
	Reader     io.Reader
	LineByLine bool // defaults to true
	BufferSize int
}

// NewIoReader returns a new IoReader wrapping the given io.Reader object
func NewIoReader(reader io.Reader) *IoReader {
	return &IoReader{Reader: reader, LineByLine: true, BufferSize: 1024}
}

// Start - see interface in stages.go for documentation.
func (r *IoReader) Start(outputChan chan ratchet.Data, killChan chan error) {
	if r.LineByLine {
		r.scanLines(outputChan, killChan)
	} else {
		r.bufferedRead(outputChan, killChan)
	}
	close(outputChan)
}

func (r *IoReader) scanLines(outputChan chan ratchet.Data, killChan chan error) {
	scanner := bufio.NewScanner(r.Reader)
	for scanner.Scan() {
		outputChan <- scanner.Bytes()
	}
	if err := scanner.Err(); err != nil {
		killChan <- err
	}
}

func (r *IoReader) bufferedRead(outputChan chan ratchet.Data, killChan chan error) {
	reader := bufio.NewReader(r.Reader)
	data := make([]byte, r.BufferSize)
	for {
		n, err := reader.Read(data)
		if err != nil && err != io.EOF {
			killChan <- err
		}
		if n == 0 {
			break
		}
		outputChan <- data
	}
}

func (r *IoReader) String() string {
	return "IoReader"
}
