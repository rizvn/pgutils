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

func NewErr(message string, wrapError error) error {
	pc, _, line, _ := runtime.Caller(1)
	funcName := runtime.FuncForPC(pc)

	e := &Err{}
	e.message = fmt.Sprintf("\nError at %s:%d\nMessage:%s\n%v", funcName, line, message, wrapError)
	return e
}
