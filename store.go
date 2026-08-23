package quotapool

import (
	"context"
	"errors"
	"time"
)

// AcquireRequest is a single all-or-nothing admission attempt for one resource.
//
// A Store implementation must treat the request atomically: either every permit
// in Tokens is recorded against every dimension Quota constrains, or the store
// is left untouched and the attempt reports false.
type AcquireRequest struct {
	// Resource names the quota bucket, typically a provider or endpoint.
	Resource string

	// Quota is the limit configuration in force for Resource.
	Quota Quota

	// Tokens holds one unique identifier per requested permit. Its length is the
	// batch size. Concurrency permits are released later by these identifiers.
	Tokens []string

	// LeaseTTL bounds how long a concurrency permit survives without an explicit
	// release, so permits held by a crashed process are eventually reclaimed.
	LeaseTTL time.Duration

	// Now is the reference time for lease expiry and window boundaries. It is
	// supplied by the caller so tests can drive time deterministically.
	Now time.Time
}

// Count reports the batch size of the request.
func (r AcquireRequest) Count() int {
	return len(r.Tokens)
}

// Store persists quota state shared by every process using the same limits.
//
// Implementations must be safe for concurrent use. The atomicity guarantee in
// Acquire is the contract that makes batch admission meaningful; an
// implementation that admits permits one at a time does not satisfy it.
type Store interface {
	// Acquire attempts to record every permit in req. It reports true when the
	// whole batch was admitted, and false when any constrained dimension lacked
	// capacity. A false result is not an error: the caller is expected to retry.
	Acquire(ctx context.Context, req AcquireRequest) (bool, error)

	// Release returns previously admitted concurrency permits. Rate permits are
	// not returned, since they expire with their window. Releasing tokens that
	// are unknown or already expired must succeed without error.
	Release(ctx context.Context, resource string, tokens []string) error
}

// Sentinel errors reported by [Limiter] when it is configured to fail closed.
// Under the default fail-open policy these conditions are logged and the caller
// is admitted instead.
var (
	// ErrQuotaExceeded reports that the wait budget expired before the batch
	// could be admitted.
	ErrQuotaExceeded = errors.New("quotapool: quota exceeded")

	// ErrBatchTooLarge reports a batch larger than a configured dimension, which
	// no amount of waiting can satisfy.
	ErrBatchTooLarge = errors.New("quotapool: batch larger than quota")

	// ErrInvalidCount reports a non-positive batch size.
	ErrInvalidCount = errors.New("quotapool: count must be positive")
)
