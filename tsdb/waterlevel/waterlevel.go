package waterlevel

import (
	"sync/atomic"
)

var waterLevel int64

func Delta(w int) {
	if w == 0 {
		return
	}
	atomic.AddInt64(&waterLevel, int64(w))
}

func Get() int64 {
	return atomic.LoadInt64(&waterLevel)
}
