package util

import (
	"errors"
	"runtime"
)

// KillPipelineIfErr is an error-checking helper.
func KillPipelineIfErr(err error, killChan chan error) {
	if err != nil {
		killChan <- ErrorWithStack(err)
	}
}

// ErrorWithStack returns a new error object that includes the stack trace.
func ErrorWithStack(err error) error {
	trace := make([]byte, 4096)
	runtime.Stack(trace, true)
	return errors.New(err.Error() + "\nStack Trace:\n" + string(trace))
}
