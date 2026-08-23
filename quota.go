package quotapool

import (
	"errors"
	"fmt"
	"time"
)

// Quota describes the limits a provider enforces on a single resource.
//
// Both dimensions are optional and independent. A zero value on either
// dimension means that dimension is unlimited; a zero Quota means the resource
// is not limited at all, which makes Acquire a no-op.
type Quota struct {
	// Concurrency caps how many permits may be held simultaneously across every
	// process sharing the store. Permits are returned by the release function
	// Acquire hands back, or reclaimed automatically when their lease expires.
	Concurrency int

	// Rate caps how many permits may be taken within Window. Unlike Concurrency,
	// rate permits are never returned: they age out of the window on their own.
	Rate int

	// Window is the sliding interval Rate applies to. It is required when Rate is
	// set and ignored otherwise.
	Window time.Duration
}

// ErrInvalidQuota reports a Quota whose dimensions are inconsistent, such as a
// Rate without a Window.
var ErrInvalidQuota = errors.New("quotapool: invalid quota")

// Validate reports whether q describes a usable set of limits.
//
// A zero Quota is valid and means "unlimited". Negative limits and a Rate
// without a positive Window are rejected, since neither can be enforced.
func (q Quota) Validate() error {
	if q.Concurrency < 0 {
		return fmt.Errorf("%w: concurrency must not be negative, got %d", ErrInvalidQuota, q.Concurrency)
	}

	if q.Rate < 0 {
		return fmt.Errorf("%w: rate must not be negative, got %d", ErrInvalidQuota, q.Rate)
	}

	if q.Rate > 0 && q.Window <= 0 {
		return fmt.Errorf("%w: rate %d requires a positive window", ErrInvalidQuota, q.Rate)
	}

	return nil
}

// IsUnlimited reports whether q constrains nothing, in which case the limiter
// admits every request without touching the store.
func (q Quota) IsUnlimited() bool {
	return q.Concurrency <= 0 && q.Rate <= 0
}

// HasConcurrency reports whether q constrains simultaneous holders.
func (q Quota) HasConcurrency() bool {
	return q.Concurrency > 0
}

// HasRate reports whether q constrains permits taken per window.
func (q Quota) HasRate() bool {
	return q.Rate > 0 && q.Window > 0
}

// Fits reports whether a batch of n permits could ever be admitted under q.
//
// A batch larger than a configured dimension can never succeed no matter how
// long the caller waits, so the limiter resolves it immediately instead of
// blocking until the wait budget expires.
func (q Quota) Fits(n int) bool {
	if n <= 0 {
		return false
	}

	if q.HasConcurrency() && n > q.Concurrency {
		return false
	}

	if q.HasRate() && n > q.Rate {
		return false
	}

	return true
}
