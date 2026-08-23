package quotapool

import (
	"context"
	"math/rand/v2"
	"time"
)

// backoff produces the delay between admission attempts.
//
// Delays grow exponentially up to a ceiling and carry jitter. Jitter matters
// more than the growth curve here: without it, every process blocked on the
// same exhausted resource retries in lockstep and the herd re-collides on each
// wake-up, so capacity freed by one release is contended by all waiters at once.
type backoff struct {
	current time.Duration
	maximum time.Duration
}

// newBackoff returns a backoff starting at minimum and capped at maximum.
func newBackoff(minimum, maximum time.Duration) *backoff {
	return &backoff{
		current: minimum,
		maximum: maximum,
	}
}

// next reports the delay to wait before the next attempt and advances the
// schedule. The returned delay is drawn from the lower half of the current
// interval so that concurrent waiters spread out rather than realign.
func (b *backoff) next() time.Duration {
	delay := jitter(b.current)

	b.current *= 2
	if b.current > b.maximum {
		b.current = b.maximum
	}

	return delay
}

// jitter returns a random duration in [d/2, d].
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}

	half := d / 2

	//nolint:gosec // G404: retry spacing needs spread, not unpredictability.
	return half + time.Duration(rand.Int64N(int64(half)+1))
}

// sleep waits for d, reporting false if ctx ended first.
func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
