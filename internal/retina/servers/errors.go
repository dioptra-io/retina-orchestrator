package servers

import "errors"

var (
	// ErrServerClosed is used to denote the server is closed.
	ErrServerClosed = errors.New("server closed")
)
