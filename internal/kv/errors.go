// Package kv internal/kv/errors.go
package kv

import "errors"

var ErrNotLeader = errors.New("not leader")

type OpError struct {
	msg string
}

func (e *OpError) Error() string {
	return e.msg
}
