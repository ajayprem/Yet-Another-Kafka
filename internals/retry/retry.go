package retry

import (
	"math/rand"
	"time"
)

func Do(attempts int, baseDelay, maxDelay time.Duration, fn func() bool) bool {
	delay := baseDelay
	for range attempts {
		if fn() {
			return true
		}
		time.Sleep(delay + time.Duration(rand.Int63n(int64(delay)/2)))
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}
	return false
}
