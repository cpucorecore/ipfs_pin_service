package model

type Status int32

const (
	StatusUnknown Status = iota
	StatusReceived
	StatusQueuedForPin
	StatusPinning
	StatusActive
	StatusScheduledForUnpin
	StatusUnpinning
	StatusUnpinSucceeded
	StatusDeadLetter
)

var statusToString = map[Status]string{
	StatusUnknown:           "Unknown",
	StatusReceived:          "Received",
	StatusQueuedForPin:      "QueuedForPin",
	StatusPinning:           "Pinning",
	StatusActive:            "Active",
	StatusScheduledForUnpin: "ScheduledForUnpin",
	StatusUnpinning:         "Unpinning",
	StatusUnpinSucceeded:    "UnpinSucceeded",
	StatusDeadLetter:        "DeadLetter",
}

var stringToStatus = func() map[string]Status {
	m := make(map[string]Status, len(statusToString))
	for k, v := range statusToString {
		m[v] = k
	}
	return m
}()

func (s Status) String() string {
	if v, ok := statusToString[s]; ok {
		return v
	}
	return "Unknown"
}

func (s Status) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

func (s *Status) UnmarshalText(text []byte) error {
	if v, ok := stringToStatus[string(text)]; ok {
		*s = v
		return nil
	}
	*s = StatusUnknown
	return nil
}
