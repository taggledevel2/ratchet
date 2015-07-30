// Package logger is a simple but customizable logger used by ratchet.
package logger

import (
	"io"
	"log"
	"os"
	"runtime/debug"
)

const (
	LogLevelDebug  = 1
	LogLevelInfo   = 2
	LogLevelError  = 3
	LogLevelStatus = 4
)

// Notifier is an interface for receiving log events. See the
// RatchetNotifier variable.
type Notifier interface {
	RatchetNotify(lvl int, trace []byte, v ...interface{})
}

// RatchetNotifier can be set to receive log events in your external
// implementation code. Useful for doing custom alerting, etc.
var RatchetNotifier Notifier

// LogLevel can be set to one of:
// logger.LevelDebug, logger.LevelInfo, logger.LevelError, or logger.LevelStatus
var LogLevel = LogLevelInfo

var defaultLogger = log.New(os.Stdout, "", log.LstdFlags)

// Debug logs output when LogLevel is set to at least Debug level
func Debug(v ...interface{}) {
	if RatchetNotifier != nil {
		RatchetNotifier.RatchetNotify(LogLevelDebug, debug.Stack(), v)
	} else {
		logit(LogLevelDebug, v)
	}
}

// Info logs output when LogLevel is set to at least Info level
func Info(v ...interface{}) {
	if RatchetNotifier != nil {
		RatchetNotifier.RatchetNotify(LogLevelInfo, debug.Stack(), v)
	} else {
		logit(LogLevelInfo, v)
	}
}

// Error logs output when LogLevel is set to at least Error level
func Error(v ...interface{}) {
	if RatchetNotifier != nil {
		RatchetNotifier.RatchetNotify(LogLevelError, debug.Stack(), v)
	} else {
		logit(LogLevelError, v)
	}
}

// Status logs output when LogLevel is set to at least Status level
// Status output is high-level status events like stages starting/completing.
func Status(v ...interface{}) {
	if RatchetNotifier != nil {
		RatchetNotifier.RatchetNotify(LogLevelStatus, debug.Stack(), v)
	} else {
		logit(LogLevelStatus, v)
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
