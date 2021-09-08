package internal

import "sync/atomic"

type CountWheel struct {
	ubound int32

	position int32
}

func NewCountWheel(refreshCount int32) *CountWheel {
	return &CountWheel{
		ubound:   refreshCount,
		position: 0,
	}
}

func (w *CountWheel) Spin() (refreshed bool) {
	if w.ubound == 0 {
		return false
	}

	atomic.AddInt32(&w.position, 1)
	return atomic.CompareAndSwapInt32(&w.position, w.ubound, 0)
}

func (w *CountWheel) Reset() {
	atomic.StoreInt32(&w.position, 0)
}
