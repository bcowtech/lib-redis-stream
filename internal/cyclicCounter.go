package internal

import "sync/atomic"

type CyclicCounter struct {
	max int32

	value int32
}

func NewCyclicCounter(max int32) *CyclicCounter {
	return &CyclicCounter{
		max:   max,
		value: 0,
	}
}

func (w *CyclicCounter) Spin() (refreshed bool) {
	if w.max == 0 {
		return false
	}

	atomic.AddInt32(&w.value, 1)
	return atomic.CompareAndSwapInt32(&w.value, w.max, 0)
}

func (w *CyclicCounter) Reset() {
	atomic.StoreInt32(&w.value, 0)
}
