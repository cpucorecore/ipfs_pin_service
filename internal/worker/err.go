package worker

import "errors"

var (
	ErrOutOfMaxRetry = errors.New("out of max retry")
)
