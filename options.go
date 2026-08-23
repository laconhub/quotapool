package quotapool

import (
	"log/slog"
	"maps"
	"time"
)

// Defaults applied by [New] when the corresponding option is not supplied.
const (
	// DefaultMaxWait bounds how long Acquire blocks before the failure policy
	// decides the outcome.
	DefaultMaxWait = 8 * time.Second

	// DefaultLeaseTTL bounds how long a concurrency permit survives without an
	// explicit release. It should comfortably exceed the longest expected hold:
	// too short and live work loses its permit, too long and a crashed process
	// pins capacity until the lease lapses.
	DefaultLeaseTTL = 30 * time.Second

	// DefaultMinBackoff and DefaultMaxBackoff bound the retry interval used
	// while waiting for capacity.
	DefaultMinBackoff = 40 * time.Millisecond
	DefaultMaxBackoff = 250 * time.Millisecond

	// DefaultReleaseTimeout bounds the store call that returns permits. Release
	// runs on its own timeout rather than the request context, which is often
	// already cancelled by the time the caller is done.
	DefaultReleaseTimeout = 3 * time.Second
)

// Option customizes a [Limiter].
type Option func(*Limiter)

// WithQuotas registers the limits enforced for each resource.
//
// Resources absent from the map are unlimited. Quotas may also be changed at
// runtime with [Limiter.SetQuota], which is useful when a provider revises an
// account's limits without a deployment.
func WithQuotas(quotas map[string]Quota) Option {
	return func(l *Limiter) {
		maps.Copy(l.quotas, quotas)
	}
}

// WithMaxWait sets how long Acquire blocks waiting for capacity before the
// failure policy applies. A non-positive duration makes Acquire attempt once
// and return immediately.
func WithMaxWait(d time.Duration) Option {
	return func(l *Limiter) {
		l.maxWait = d
	}
}

// WithLeaseTTL sets the lease carried by concurrency permits.
func WithLeaseTTL(d time.Duration) Option {
	return func(l *Limiter) {
		if d > 0 {
			l.leaseTTL = d
		}
	}
}

// WithBackoff sets the bounds of the retry interval used while waiting.
func WithBackoff(minimum, maximum time.Duration) Option {
	return func(l *Limiter) {
		if minimum > 0 {
			l.minBackoff = minimum
		}

		if maximum > 0 {
			l.maxBackoff = maximum
		}
	}
}

// WithReleaseTimeout sets the timeout for the store call that returns permits.
func WithReleaseTimeout(d time.Duration) Option {
	return func(l *Limiter) {
		if d > 0 {
			l.releaseTimeout = d
		}
	}
}

// WithFailClosed makes Acquire report an error instead of admitting the caller
// when capacity cannot be confirmed.
//
// The default is to fail open. Choose between them by asking which failure the
// system tolerates better: fail-open risks an occasional provider-side
// rejection, fail-closed risks blocking work while the store is unreachable.
// Interactive paths usually prefer fail-open; billing and abuse controls
// usually prefer fail-closed.
func WithFailClosed() Option {
	return func(l *Limiter) {
		l.isFailOpen = false
	}
}

// WithLogger directs diagnostics to the given logger.
//
// The limiter logs at warn level whenever the failure policy is exercised,
// which is the signal that a quota is being hit or the store is unhealthy.
// Diagnostics are discarded when no logger is set.
func WithLogger(logger *slog.Logger) Option {
	return func(l *Limiter) {
		if logger != nil {
			l.logger = logger
		}
	}
}

// WithNowFunc overrides the clock used for lease and window arithmetic. It
// exists so tests can advance time deterministically.
func WithNowFunc(now func() time.Time) Option {
	return func(l *Limiter) {
		if now != nil {
			l.now = now
		}
	}
}

// discardLogger returns a logger that writes nothing, used when the caller
// supplies none.
func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}
