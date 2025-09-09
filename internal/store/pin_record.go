package store

import pb "github.com/cpucorecore/ipfs_pin_service/proto"

type PinRecord = pb.PinRecord

func ClonePinRecord(pinRecord *PinRecord) (*PinRecord, []*PinRecord) {
	return &PinRecord{
		Cid:                 pinRecord.Cid,
		Status:              pinRecord.Status,
		ReceivedAt:          pinRecord.ReceivedAt,
		EnqueuedAt:          pinRecord.EnqueuedAt,
		PinStartAt:          pinRecord.PinStartAt,
		PinSucceededAt:      pinRecord.PinSucceededAt,
		ExpireAt:            pinRecord.ExpireAt,
		ScheduleUnpinAt:     pinRecord.ScheduleUnpinAt,
		UnpinStartAt:        pinRecord.UnpinStartAt,
		UnpinSucceededAt:    pinRecord.UnpinSucceededAt,
		LastUpdateAt:        pinRecord.LastUpdateAt,
		Size:                pinRecord.Size,
		PinAttemptCount:     pinRecord.PinAttemptCount,
		UnpinAttemptCount:   pinRecord.UnpinAttemptCount,
		SizeLimit:           pinRecord.SizeLimit,
		ProvideStartAt:      pinRecord.ProvideStartAt,
		ProvideSucceededAt:  pinRecord.ProvideSucceededAt,
		ProvideAttemptCount: pinRecord.ProvideAttemptCount,
		ProvideError:        pinRecord.ProvideError,
	}, pinRecord.History
}

func ResetPinRecordDynamicState(pinRecord *PinRecord, historyLen int) {
	pinRecord.Status = StatusUnknown
	pinRecord.ReceivedAt = 0
	pinRecord.EnqueuedAt = 0
	pinRecord.PinStartAt = 0
	pinRecord.PinSucceededAt = 0
	pinRecord.ExpireAt = 0
	pinRecord.ScheduleUnpinAt = 0
	pinRecord.UnpinStartAt = 0
	pinRecord.UnpinSucceededAt = 0
	pinRecord.PinAttemptCount = 0
	pinRecord.UnpinAttemptCount = 0
	pinRecord.ProvideStartAt = 0
	pinRecord.ProvideSucceededAt = 0
	pinRecord.ProvideAttemptCount = 0
	pinRecord.ProvideError = ""
	pinRecord.History = make([]*pb.PinRecord, 0, historyLen)
}

func AppendHistory(pinRecord *PinRecord, lastPinRecord *PinRecord, history []*PinRecord) {
	if len(history) > 0 {
		pinRecord.History = append(pinRecord.History, history...)
	}
	pinRecord.History = append(pinRecord.History, lastPinRecord)
}
