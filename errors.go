package snorlax

import "errors"

var (
	ErrSubNoMessageType      = errors.New("no message type")
	ErrSubUnknownMessageType = errors.New("unknown message type")
	ErrSubUnmarshaling       = errors.New("failed to unmarshal message")
	ErrSubHandler            = errors.New("handler error")
)
