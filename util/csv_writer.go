package util

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

type CSVWriter struct {
	Comma             rune
	UseCRLF           bool
	w                 *bufio.Writer
	AlwaysEncapsulate bool
	QuoteEscape       string
}

func NewCSVWriter(w io.Writer) *CSVWriter {
	return &CSVWriter{
		Comma:             ',',
		UseCRLF:           false,
		w:                 bufio.NewWriter(w),
		AlwaysEncapsulate: true,
		QuoteEscape:       `\`,
	}
}

func (w *CSVWriter) Write(record []string) (err error) {
	for n, field := range record {
		if n > 0 {
			if _, err = w.w.WriteRune(w.Comma); err != nil {
				return
			}
		}

		if !w.fieldNeedsQuotes(field) {
			if _, err = w.w.WriteString(field); err != nil {
				return
			}
			continue
		}
		if err = w.w.WriteByte('"'); err != nil {
			return
		}

		for _, r1 := range field {
			switch r1 {
			case '"':
				_, err = w.w.WriteString(fmt.Sprintf(`%v"`, w.QuoteEscape))
			case '\r':
				if !w.UseCRLF {
					err = w.w.WriteByte('\r')
				}
			case '\n':
				if w.UseCRLF {
					_, err = w.w.WriteString("\r\n")
				} else {
					err = w.w.WriteByte('\n')
				}
			default:
				_, err = w.w.WriteRune(r1)
			}
			if err != nil {
				return
			}
		}

		if err = w.w.WriteByte('"'); err != nil {
			return
		}
	}

	if w.UseCRLF {
		_, err = w.w.WriteString("\r\n")
	} else {
		err = w.w.WriteByte('\n')
	}

	return
}

func (w *CSVWriter) Flush() {
	w.w.Flush()
}

func (w *CSVWriter) Error() error {
	_, err := w.w.Write(nil)
	return err
}

func (w *CSVWriter) WriteAll(records [][]string) (err error) {
	for _, record := range records {
		err = w.Write(record)
		if err != nil {
			return err
		}
	}
	return w.w.Flush()
}

func (w *CSVWriter) fieldNeedsQuotes(field string) bool {
	if w.AlwaysEncapsulate {
		return true
	}
	if field == "" {
		return false
	}
	if field == `\.` || strings.IndexRune(field, w.Comma) >= 0 || strings.IndexAny(field, "\"\r\n") >= 0 {
		return true
	}

	r1, _ := utf8.DecodeRuneInString(field)
	return unicode.IsSpace(r1)
}
