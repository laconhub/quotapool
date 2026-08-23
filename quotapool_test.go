package quotapool

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// testResource is the resource name used across tests in this package.
const testResource = "vendor"

// stubStore is a Store whose behaviour each test sets explicitly, so limiter
// policy can be exercised without a real backend.
type stubStore struct {
	mu sync.Mutex

	admit     bool
	acquireBy error
	releaseBy error

	acquireCalls int
	releaseCalls int
	lastRequest  AcquireRequest
}

func (s *stubStore) Acquire(_ context.Context, req AcquireRequest) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.acquireCalls++
	s.lastRequest = req

	if s.acquireBy != nil {
		return false, s.acquireBy
	}

	return s.admit, nil
}

func (s *stubStore) Release(_ context.Context, _ string, _ []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.releaseCalls++

	return s.releaseBy
}

func (s *stubStore) calls() (acquires, releases int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.acquireCalls, s.releaseCalls
}

// newTestLimiter builds a limiter with waits short enough to keep tests fast.
func newTestLimiter(t *testing.T, store Store, opts ...Option) *Limiter {
	t.Helper()

	base := []Option{
		WithMaxWait(30 * time.Millisecond),
		WithBackoff(time.Millisecond, 2*time.Millisecond),
	}

	return New(store, append(base, opts...)...)
}

func TestLimiter_SetQuota(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		quota       Quota
		expectError bool
	}{
		{name: "valid quota", quota: Quota{Concurrency: 4}, expectError: false},
		{name: "zero quota", quota: Quota{}, expectError: false},
		{name: "rate without window", quota: Quota{Rate: 5}, expectError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			limiter := New(NewMemoryStore())

			err := limiter.SetQuota(testResource, tt.quota)
			if tt.expectError {
				if !errors.Is(err, ErrInvalidQuota) {
					t.Fatalf("expected ErrInvalidQuota, got %v", err)
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			got, ok := limiter.Quota(testResource)
			if !ok {
				t.Fatal("expected the quota to be registered")
			}

			if got != tt.quota {
				t.Fatalf("expected %+v, got %+v", tt.quota, got)
			}
		})
	}
}

func TestLimiter_QuotaUnregistered(t *testing.T) {
	t.Parallel()

	limiter := New(NewMemoryStore())

	if _, ok := limiter.Quota("never-registered"); ok {
		t.Fatal("expected no quota to be reported for an unregistered resource")
	}
}

// TestLimiter_AcquireUnregisteredResourceSkipsStore documents the default:
// a resource is unlimited until its provider limits are known, and unlimited
// resources must not pay a store round trip.
func TestLimiter_AcquireUnregisteredResourceSkipsStore(t *testing.T) {
	t.Parallel()

	store := &stubStore{admit: false}
	limiter := newTestLimiter(t, store)

	release, err := limiter.Acquire(context.Background(), "unregistered", 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	release()

	if acquires, _ := store.calls(); acquires != 0 {
		t.Fatalf("expected the store to be skipped, got %d calls", acquires)
	}
}

func TestLimiter_AcquireAdmitted(t *testing.T) {
	t.Parallel()

	store := &stubStore{admit: true}
	limiter := newTestLimiter(t, store)

	if err := limiter.SetQuota(testResource, Quota{Concurrency: 4}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	release, err := limiter.Acquire(context.Background(), testResource, 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if store.lastRequest.Count() != 4 {
		t.Fatalf("expected a batch of 4 tokens, got %d", store.lastRequest.Count())
	}

	release()

	if _, releases := store.calls(); releases != 1 {
		t.Fatalf("expected exactly one release, got %d", releases)
	}

	stats := limiter.Stats()
	if stats.Admitted != 1 || stats.Released != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

// TestLimiter_ReleaseIsIdempotent guards the documented contract that callers
// may invoke Release more than once, which happens when a defer and an explicit
// call coexist on an error path.
func TestLimiter_ReleaseIsIdempotent(t *testing.T) {
	t.Parallel()

	store := &stubStore{admit: true}
	limiter := newTestLimiter(t, store)

	if err := limiter.SetQuota(testResource, Quota{Concurrency: 2}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	release, err := limiter.Acquire(context.Background(), testResource, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	release()
	release()
	release()

	if _, releases := store.calls(); releases != 1 {
		t.Fatalf("expected exactly one release, got %d", releases)
	}
}

// TestLimiter_ReleaseSucceedsAfterContextCancel covers the reason release runs
// on its own timeout: the request context is usually already cancelled by the
// time the work being limited has finished.
func TestLimiter_ReleaseSucceedsAfterContextCancel(t *testing.T) {
	t.Parallel()

	store := &stubStore{admit: true}
	limiter := newTestLimiter(t, store)

	if err := limiter.SetQuota(testResource, Quota{Concurrency: 2}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	release, err := limiter.Acquire(ctx, testResource, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cancel()
	release()

	if _, releases := store.calls(); releases != 1 {
		t.Fatalf("expected the release to reach the store, got %d calls", releases)
	}

	if got := limiter.Stats().Released; got != 1 {
		t.Fatalf("expected one successful release, got %d", got)
	}
}

// TestLimiter_RateOnlyQuotaNeedsNoRelease documents that rate permits age out
// of their window and are never handed back.
func TestLimiter_RateOnlyQuotaNeedsNoRelease(t *testing.T) {
	t.Parallel()

	store := &stubStore{admit: true}
	limiter := newTestLimiter(t, store)

	if err := limiter.SetQuota(testResource, Quota{Rate: 10, Window: time.Minute}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	release, err := limiter.Acquire(context.Background(), testResource, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	release()

	if _, releases := store.calls(); releases != 0 {
		t.Fatalf("expected no release call for a rate-only quota, got %d", releases)
	}
}

func TestLimiter_AcquireFailOpen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		store  *stubStore
		quota  Quota
		count  int
		reason string
	}{
		{
			name:   "wait budget expires",
			store:  &stubStore{admit: false},
			quota:  Quota{Concurrency: 4},
			count:  1,
			reason: "exhausted quota",
		},
		{
			name:   "store unavailable",
			store:  &stubStore{acquireBy: errors.New("connection refused")},
			quota:  Quota{Concurrency: 4},
			count:  1,
			reason: "backend failure",
		},
		{
			name:   "batch larger than quota",
			store:  &stubStore{admit: true},
			quota:  Quota{Concurrency: 2},
			count:  8,
			reason: "impossible request",
		},
		{
			name:   "non-positive batch",
			store:  &stubStore{admit: true},
			quota:  Quota{Concurrency: 2},
			count:  0,
			reason: "invalid request",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			limiter := newTestLimiter(t, tt.store)
			if err := limiter.SetQuota(testResource, tt.quota); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			release, err := limiter.Acquire(context.Background(), testResource, tt.count)
			if err != nil {
				t.Fatalf("fail-open must not report an error for %s, got %v", tt.reason, err)
			}

			if release == nil {
				t.Fatal("fail-open must return a usable release function")
			}

			release()

			if got := limiter.Stats().FailedOpen; got != 1 {
				t.Fatalf("expected one fail-open, got %d", got)
			}
		})
	}
}

func TestLimiter_AcquireFailClosed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		store       *stubStore
		quota       Quota
		count       int
		expectedErr error
	}{
		{
			name:        "wait budget expires",
			store:       &stubStore{admit: false},
			quota:       Quota{Concurrency: 4},
			count:       1,
			expectedErr: ErrQuotaExceeded,
		},
		{
			name:        "batch larger than quota",
			store:       &stubStore{admit: true},
			quota:       Quota{Concurrency: 2},
			count:       8,
			expectedErr: ErrBatchTooLarge,
		},
		{
			name:        "non-positive batch",
			store:       &stubStore{admit: true},
			quota:       Quota{Concurrency: 2},
			count:       0,
			expectedErr: ErrInvalidCount,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			limiter := newTestLimiter(t, tt.store, WithFailClosed())
			if err := limiter.SetQuota(testResource, tt.quota); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			release, err := limiter.Acquire(context.Background(), testResource, tt.count)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("expected %v, got %v", tt.expectedErr, err)
			}

			if release != nil {
				t.Fatal("expected no release function on a failed acquire")
			}

			if got := limiter.Stats().FailedClosed; got != 1 {
				t.Fatalf("expected one fail-closed, got %d", got)
			}
		})
	}
}

func TestLimiter_AcquireFailClosedPropagatesStoreError(t *testing.T) {
	t.Parallel()

	storeErr := errors.New("connection refused")
	limiter := newTestLimiter(t, &stubStore{acquireBy: storeErr}, WithFailClosed())

	if err := limiter.SetQuota(testResource, Quota{Concurrency: 4}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err := limiter.Acquire(context.Background(), testResource, 1)
	if !errors.Is(err, storeErr) {
		t.Fatalf("expected the store error to be wrapped, got %v", err)
	}
}

// TestLimiter_AcquireRetriesUntilCapacityFrees verifies that Acquire waits
// rather than failing on the first rejection.
func TestLimiter_AcquireRetriesUntilCapacityFrees(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	limiter := New(store,
		WithMaxWait(2*time.Second),
		WithBackoff(time.Millisecond, 5*time.Millisecond),
		WithQuotas(map[string]Quota{testResource: {Concurrency: 1}}),
	)

	held, err := limiter.Acquire(context.Background(), testResource, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	freed := make(chan struct{})

	go func() {
		time.Sleep(20 * time.Millisecond)
		held()
		close(freed)
	}()

	release, err := limiter.Acquire(context.Background(), testResource, 1)
	if err != nil {
		t.Fatalf("expected the retry to succeed once capacity freed, got %v", err)
	}

	defer release()

	<-freed

	if got := limiter.Stats().Admitted; got != 2 {
		t.Fatalf("expected two admissions, got %d", got)
	}
}

func TestLimiter_TryAcquireDoesNotBlock(t *testing.T) {
	t.Parallel()

	limiter := New(NewMemoryStore(),
		WithMaxWait(10*time.Second),
		WithFailClosed(),
		WithQuotas(map[string]Quota{testResource: {Concurrency: 1}}),
	)

	held, err := limiter.Acquire(context.Background(), testResource, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	defer held()

	start := time.Now()

	_, err = limiter.TryAcquire(context.Background(), testResource, 1)
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded, got %v", err)
	}

	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("TryAcquire blocked for %v, expected an immediate return", elapsed)
	}
}

func TestLimiter_AcquireHonorsContextCancellation(t *testing.T) {
	t.Parallel()

	limiter := New(NewMemoryStore(),
		WithMaxWait(10*time.Second),
		WithBackoff(time.Millisecond, 2*time.Millisecond),
		WithFailClosed(),
		WithQuotas(map[string]Quota{testResource: {Concurrency: 1}}),
	)

	held, err := limiter.Acquire(context.Background(), testResource, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	defer held()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()

	if _, err := limiter.Acquire(ctx, testResource, 1); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded, got %v", err)
	}

	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Acquire ignored context cancellation, waited %v", elapsed)
	}
}

// TestLimiter_AcquireIsAtomicUnderConcurrency is the end-to-end statement of the
// library's purpose: with room for two batches of four, exactly two of many
// competing callers may proceed, and no caller ever gets a partial batch.
func TestLimiter_AcquireIsAtomicUnderConcurrency(t *testing.T) {
	t.Parallel()

	const (
		batchSize = 4
		limit     = batchSize * 2
		callers   = 16
	)

	limiter := New(NewMemoryStore(),
		WithMaxWait(20*time.Millisecond),
		WithBackoff(time.Millisecond, 2*time.Millisecond),
		WithFailClosed(),
		WithQuotas(map[string]Quota{testResource: {Concurrency: limit}}),
	)

	var (
		mu        sync.Mutex
		succeeded int
		wg        sync.WaitGroup
	)

	for range callers {
		wg.Go(func() {
			release, err := limiter.Acquire(context.Background(), testResource, batchSize)
			if err != nil {
				return
			}

			mu.Lock()
			succeeded++
			mu.Unlock()

			time.Sleep(50 * time.Millisecond)
			release()
		})
	}

	wg.Wait()

	if succeeded != limit/batchSize {
		t.Fatalf("expected exactly %d batches to be admitted, got %d", limit/batchSize, succeeded)
	}
}

func TestLimiter_StatsCountReleaseFailures(t *testing.T) {
	t.Parallel()

	store := &stubStore{admit: true, releaseBy: errors.New("connection refused")}
	limiter := newTestLimiter(t, store)

	if err := limiter.SetQuota(testResource, Quota{Concurrency: 2}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	release, err := limiter.Acquire(context.Background(), testResource, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	release()

	stats := limiter.Stats()
	if stats.ReleaseFailed != 1 || stats.Released != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestNewTokens(t *testing.T) {
	t.Parallel()

	tokens := newTokens(4)
	if len(tokens) != 4 {
		t.Fatalf("expected 4 tokens, got %d", len(tokens))
	}

	seen := map[string]bool{}
	for _, token := range tokens {
		if seen[token] {
			t.Fatalf("duplicate token %q would let one holder release another's permit", token)
		}

		seen[token] = true
	}

	other := newTokens(4)
	for _, token := range other {
		if seen[token] {
			t.Fatalf("token %q collided across batches", token)
		}
	}
}

// TestLimiter_TryAcquirePassesLiveContext guards a regression that a store
// ignoring the context would hide: wrapping a zero wait budget in a timeout
// hands the store an already-expired context, so every attempt fails at the
// backend instead of being evaluated against the quota.
func TestLimiter_TryAcquirePassesLiveContext(t *testing.T) {
	t.Parallel()

	store := &contextCheckingStore{}
	limiter := New(store, WithQuotas(map[string]Quota{testResource: {Concurrency: 4}}))

	if _, err := limiter.TryAcquire(context.Background(), testResource, 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if store.receivedErr != nil {
		t.Fatalf("store received a dead context: %v", store.receivedErr)
	}
}

// contextCheckingStore records whether the context it received was already done.
type contextCheckingStore struct {
	receivedErr error
}

func (s *contextCheckingStore) Acquire(ctx context.Context, _ AcquireRequest) (bool, error) {
	s.receivedErr = ctx.Err()

	return true, nil
}

func (s *contextCheckingStore) Release(_ context.Context, _ string, _ []string) error {
	return nil
}
