package ratchet

import (
	"io"
	"log"
	"os"
)

const (
	LogLevelDebug  = 1
	LogLevelInfo   = 2
	LogLevelError  = 3
	LogLevelStatus = 4
)

// LogLevel can be set to one of:
// ratchet.LogLevelDebug, ratchet.LogLevelInfo, ratchet.LogLevelError, or ratchet.LogLevelStatus
var LogLevel = LogLevelInfo

// LogFunc can be overridden to totally customize how logging occurs.
var LogFunc func(lvl int, v ...interface{})

var logger = log.New(os.Stdout, "", log.LstdFlags)

// LogDebug logs output when LogLevel is set to at least Debug level
func LogDebug(v ...interface{}) {
	if LogFunc != nil {
		LogFunc(LogLevelDebug, v)
	} else {
		logit(LogLevelDebug, v)
	}
}

// LogInfo logs output when LogLevel is set to at least Info level
func LogInfo(v ...interface{}) {
	if LogFunc != nil {
		LogFunc(LogLevelInfo, v)
	} else {
		logit(LogLevelInfo, v)
	}
}

// LogError logs output when LogLevel is set to at least Error level
func LogError(v ...interface{}) {
	if LogFunc != nil {
		LogFunc(LogLevelError, v)
	} else {
		logit(LogLevelError, v)
	}
}

// LogStatus logs output when LogLevel is set to at least Status level
// Status output is high-level status events like stages starting/completing.
func LogStatus(v ...interface{}) {
	if LogFunc != nil {
		LogFunc(LogLevelStatus, v)
	} else {
		logit(LogLevelStatus, v)
	}
}

func logit(lvl int, v ...interface{}) {
	if lvl >= LogLevel {
		logger.Println(v...)
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
	logger = log.New(out, "", log.LstdFlags)
}
