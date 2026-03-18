package common

import (
	"fmt"
	"runtime"
)

type Err struct {
	message string
}

func (e *Err) Error() string {
	return e.message
}

func NewErr(message string, wrapErr error) *Err {
	// get refenece to caller function
	pc, _, line, _ := runtime.Caller(1)
	funcName := runtime.FuncForPC(pc).Name()

	e := &Err{}
	e.message = fmt.Sprintf("\nError at: %s : %d\nMessage:%s\n%v", funcName, line, message, wrapErr)
	return e
}
