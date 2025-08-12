package store

type Status = int32

const (
	StatusUnknown Status = iota
	StatusReceived
	StatusActive
	StatusPinning
	StatusPinSucceeded
	StatusPinFailed
	StatusScheduledForUnpin
	StatusUnpinning
	StatusUnpinSucceeded
	StatusDeadLetter
)
