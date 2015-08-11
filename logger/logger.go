// Package logger is a simple but customizable logger used by ratchet.
package logger

import (
	"io"
	"log"
	"os"
	"runtime"
)

const (
	LogLevelDebug = iota
	LogLevelInfo
	LogLevelError
	LogLevelStatus
)

// RatchetNotifier is an interface for receiving log events. See the
// Notifier variable.
type RatchetNotifier interface {
	RatchetNotify(lvl int, trace []byte, v ...interface{})
}

// RatchetNotifier can be set to receive log events in your external
// implementation code. Useful for doing custom alerting, etc.
var Notifier RatchetNotifier

// LogLevel can be set to one of:
// logger.LevelDebug, logger.LevelInfo, logger.LevelError, or logger.LevelStatus
var LogLevel = LogLevelInfo

var defaultLogger = log.New(os.Stdout, "", log.LstdFlags)

// Debug logs output when LogLevel is set to at least Debug level
func Debug(v ...interface{}) {
	logit(LogLevelDebug, v)
	if Notifier != nil {
		Notifier.RatchetNotify(LogLevelDebug, nil, v)
	}
}

// Info logs output when LogLevel is set to at least Info level
func Info(v ...interface{}) {
	logit(LogLevelInfo, v)
	if Notifier != nil {
		Notifier.RatchetNotify(LogLevelInfo, nil, v)
	}
}

// Error logs output when LogLevel is set to at least Error level
func Error(v ...interface{}) {
	logit(LogLevelError, v)
	if Notifier != nil {
		trace := make([]byte, 4096)
		runtime.Stack(trace, true)
		Notifier.RatchetNotify(LogLevelError, trace, v)
	}
}

// ErrorWithoutTrace logs output when LogLevel is set to at least Error level
// but doesn't send the stack trace to Notifier. This is useful only when
// using a RatchetNotifier implementation.
func ErrorWithoutTrace(v ...interface{}) {
	logit(LogLevelError, v)
	if Notifier != nil {
		Notifier.RatchetNotify(LogLevelError, nil, v)
	}
}

// Status logs output when LogLevel is set to at least Status level
// Status output is high-level status events like stages starting/completing.
func Status(v ...interface{}) {
	logit(LogLevelStatus, v)
	if Notifier != nil {
		Notifier.RatchetNotify(LogLevelStatus, nil, v)
	}
}

func logit(lvl int, v ...interface{}) {
	if lvl >= LogLevel {
		defaultLogger.Println(v...)
	}
}

// SetLogfile can be used to log to a file as well as Stdoud.
func SetLogfile(filepath string) {
	f, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err.Error())
	}
	out := io.MultiWriter(os.Stdout, f)
	SetOutput(out)
}

// SetOutput allows setting log output to any custom io.Writer.
func SetOutput(out io.Writer) {
	defaultLogger = log.New(out, "", log.LstdFlags)
}
