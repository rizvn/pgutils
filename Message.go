package main

import "time"

type Message struct {
	MsgID      int64
	ReadCount  int
	EnqueuedAt *time.Time
	VT         *time.Time
	Message    *string
	Headers    *string
}
