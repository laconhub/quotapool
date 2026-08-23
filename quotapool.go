package quotapool

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Release returns permits held by a successful [Limiter.Acquire].
//
// It is safe to call more than once and from any goroutine; only the first call
// reaches the store. Callers should defer it immediately after Acquire returns.
type Release func()

// Limiter admits batches of work against per-resource provider quotas.
//
// It is safe for concurrent use by multiple goroutines.
type Limiter struct {
	store Store

	mu     sync.RWMutex
	quotas map[string]Quota

	maxWait        time.Duration
	leaseTTL       time.Duration
	minBackoff     time.Duration
	maxBackoff     time.Duration
	releaseTimeout time.Duration

	isFailOpen bool

	logger *slog.Logger
	now    func() time.Time

	metrics metrics
}

// New returns a Limiter backed by store.
//
// Without [WithQuotas] every resource is unlimited, which makes Acquire a
// no-op. That is the intended default: a resource becomes limited only once its
// provider's limits are known.
func New(store Store, opts ...Option) *Limiter {
	limiter := &Limiter{
		store:          store,
		quotas:         map[string]Quota{},
		maxWait:        DefaultMaxWait,
		leaseTTL:       DefaultLeaseTTL,
		minBackoff:     DefaultMinBackoff,
		maxBackoff:     DefaultMaxBackoff,
		releaseTimeout: DefaultReleaseTimeout,
		isFailOpen:     true,
		logger:         discardLogger(),
		now:            time.Now,
	}

	for _, opt := range opts {
		opt(limiter)
	}

	return limiter
}

// SetQuota installs the limits for a resource, replacing any previous value.
//
// It reports [ErrInvalidQuota] when the limits are inconsistent. Passing a zero
// Quota makes the resource unlimited.
func (l *Limiter) SetQuota(resource string, quota Quota) error {
	if err := quota.Validate(); err != nil {
		return err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.quotas[resource] = quota

	return nil
}

// Quota reports the limits in force for a resource and whether any were
// registered.
func (l *Limiter) Quota(resource string) (Quota, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	quota, ok := l.quotas[resource]

	return quota, ok
}

// Acquire reserves n permits for a resource, blocking until they are all
// available or the wait budget expires.
//
// Admission is all-or-nothing: either every permit is reserved or none is. Use
// this whenever n units of work must start together.
//
// The returned Release must be called when the work finishes. It is never nil
// on a nil error, so `defer release()` is always safe:
//
//	release, err := limiter.Acquire(ctx, "vendor-a", 4)
//	if err != nil {
//		return err
//	}
//	defer release()
//
// Under the default fail-open policy the error is nil even when capacity could
// not be confirmed. With [WithFailClosed] it may report [ErrInvalidCount],
// [ErrBatchTooLarge], [ErrQuotaExceeded], or a store failure.
func (l *Limiter) Acquire(ctx context.Context, resource string, n int) (Release, error) {
	return l.acquire(ctx, resource, n, l.maxWait)
}

// TryAcquire reserves n permits without blocking, making a single attempt.
//
// It behaves like [Limiter.Acquire] in every other respect, and is the right
// choice when the caller has useful work to do instead of waiting.
func (l *Limiter) TryAcquire(ctx context.Context, resource string, n int) (Release, error) {
	return l.acquire(ctx, resource, n, 0)
}

// acquire holds the admission loop shared by Acquire and TryAcquire.
func (l *Limiter) acquire(ctx context.Context, resource string, n int, maxWait time.Duration) (Release, error) {
	if n <= 0 {
		return l.reject(resource, n, ErrInvalidCount, "non-positive batch size")
	}

	quota, ok := l.Quota(resource)
	if !ok || quota.IsUnlimited() {
		return noopRelease, nil
	}

	if !quota.Fits(n) {
		// No amount of waiting makes an oversized batch fit, so resolve it now
		// rather than burning the whole wait budget on attempts that must fail.
		return l.reject(resource, n, ErrBatchTooLarge, "batch larger than configured quota")
	}

	// A non-positive budget means "attempt once". Wrapping the context in an
	// already-expired timeout would instead fail the attempt itself, since the
	// store would receive a dead context and never reach the backend.
	attemptCtx := ctx

	if maxWait > 0 {
		var cancel context.CancelFunc

		attemptCtx, cancel = context.WithTimeout(ctx, maxWait)
		defer cancel()
	}

	tokens := newTokens(n)
	attempt := newBackoff(l.minBackoff, l.maxBackoff)

	for {
		admitted, err := l.store.Acquire(attemptCtx, AcquireRequest{
			Resource: resource,
			Quota:    quota,
			Tokens:   tokens,
			LeaseTTL: l.leaseTTL,
			Now:      l.now(),
		})
		if err != nil {
			return l.reject(resource, n, err, "store unavailable")
		}

		if admitted {
			l.metrics.admitted.Add(1)

			return l.releaseFunc(resource, quota, tokens), nil
		}

		if maxWait <= 0 {
			return l.reject(resource, n, ErrQuotaExceeded, "no capacity available")
		}

		if !sleep(attemptCtx, attempt.next()) {
			return l.reject(resource, n, ErrQuotaExceeded, "wait budget expired")
		}
	}
}

// reject applies the failure policy: fail-open admits the caller with a warning,
// fail-closed reports the error.
func (l *Limiter) reject(resource string, n int, err error, reason string) (Release, error) {
	l.metrics.rejected.Add(1)

	if !l.isFailOpen {
		l.metrics.failedClosed.Add(1)

		return nil, fmt.Errorf("quotapool: acquiring %d permits for %q: %w", n, resource, err)
	}

	l.metrics.failedOpen.Add(1)
	l.logger.Warn("quotapool: admitting request without confirmed capacity",
		slog.String("resource", resource),
		slog.Int("count", n),
		slog.String("reason", reason),
		slog.String("error", err.Error()),
	)

	return noopRelease, nil
}

// releaseFunc builds the Release returned to a caller that was admitted.
func (l *Limiter) releaseFunc(resource string, quota Quota, tokens []string) Release {
	if !quota.HasConcurrency() {
		// Rate permits age out of their window on their own; there is nothing to
		// hand back.
		return noopRelease
	}

	var once sync.Once

	return func() {
		once.Do(func() {
			// Release runs on its own timeout: the request context is frequently
			// already cancelled by the time work completes, and dropping the
			// release would pin capacity until the lease expires.
			ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), l.releaseTimeout)
			defer cancel()

			if err := l.store.Release(ctx, resource, tokens); err != nil {
				l.metrics.releaseFailed.Add(1)
				l.logger.Warn("quotapool: releasing permits failed; capacity returns when the lease expires",
					slog.String("resource", resource),
					slog.Int("count", len(tokens)),
					slog.String("error", err.Error()),
				)

				return
			}

			l.metrics.released.Add(1)
		})
	}
}

// noopRelease is handed to callers that hold no permits.
func noopRelease() {}

var (
	// processToken distinguishes this process from every other one sharing the
	// store. Drawing it once matters: tokens only need to be unique, and reading
	// system entropy per batch put a syscall on the admission path.
	processToken = randomHex()

	// batchCounter distinguishes batches within this process.
	batchCounter atomic.Uint64
)

// newTokens mints one unique identifier per permit. Members of a batch share a
// prefix so a single batch is recognizable in the store during debugging.
//
// Tokens must not collide across processes: a repeated identifier would let one
// process release a permit another still holds. A random per-process prefix
// combined with a monotonic batch counter makes collisions negligible without
// requiring coordination between processes.
func newTokens(n int) []string {
	prefix := processToken + ":" + strconv.FormatUint(batchCounter.Add(1), 10) + ":"
	tokens := make([]string, n)

	for i := range tokens {
		tokens[i] = prefix + strconv.Itoa(i)
	}

	return tokens
}

// randomHex returns 16 random bytes as a hex string. It falls back to the
// current time when the system entropy source fails, which keeps the limiter
// usable at the cost of uniqueness guarantees rather than failing at startup.
func randomHex() string {
	var buf [16]byte

	if _, err := rand.Read(buf[:]); err != nil {
		return "t" + strconv.FormatInt(time.Now().UnixNano(), 10)
	}

	return hex.EncodeToString(buf[:])
}

// metrics counts admission outcomes. Counters are the cheapest useful signal
// here: the ratio of rejected to admitted answers "am I being throttled", which
// is the first question asked when a provider starts refusing calls.
type metrics struct {
	admitted      atomic.Uint64
	rejected      atomic.Uint64
	failedOpen    atomic.Uint64
	failedClosed  atomic.Uint64
	released      atomic.Uint64
	releaseFailed atomic.Uint64
}

// Stats is a point-in-time snapshot of admission outcomes.
type Stats struct {
	// Admitted counts batches that reserved capacity.
	Admitted uint64

	// Rejected counts batches that did not, whatever the reason.
	Rejected uint64

	// FailedOpen counts rejected batches admitted anyway by the fail-open policy.
	FailedOpen uint64

	// FailedClosed counts rejected batches reported as errors.
	FailedClosed uint64

	// Released counts permits handed back successfully.
	Released uint64

	// ReleaseFailed counts releases that errored, leaving capacity to be
	// reclaimed by lease expiry. A persistently rising value means permits are
	// being held longer than intended.
	ReleaseFailed uint64
}

// Stats returns a snapshot of admission outcomes since the limiter was created.
//
// The counters are read independently, so a snapshot taken during heavy traffic
// may not be internally consistent. They are intended for trend monitoring, not
// exact accounting.
func (l *Limiter) Stats() Stats {
	return Stats{
		Admitted:      l.metrics.admitted.Load(),
		Rejected:      l.metrics.rejected.Load(),
		FailedOpen:    l.metrics.failedOpen.Load(),
		FailedClosed:  l.metrics.failedClosed.Load(),
		Released:      l.metrics.released.Load(),
		ReleaseFailed: l.metrics.releaseFailed.Load(),
	}
}
