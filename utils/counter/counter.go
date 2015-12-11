package counter

import (
	"sync/atomic"
)

// A safe concurrent counter
type Counter int64

func (c *Counter) Increment() {
	atomic.AddInt64((*int64)(c), 1)
}

func (c *Counter) Decrement() {
	atomic.AddInt64((*int64)(c), -1)
}

func (c *Counter) Value() int {
	return int(atomic.LoadInt64((*int64)(c)))
}
