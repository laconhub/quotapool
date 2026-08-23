package quotapool

import (
	"log/slog"
	"testing"
	"time"
)

func TestNewAppliesDefaults(t *testing.T) {
	t.Parallel()

	limiter := New(NewMemoryStore())

	if limiter.maxWait != DefaultMaxWait {
		t.Fatalf("expected max wait %v, got %v", DefaultMaxWait, limiter.maxWait)
	}

	if limiter.leaseTTL != DefaultLeaseTTL {
		t.Fatalf("expected lease TTL %v, got %v", DefaultLeaseTTL, limiter.leaseTTL)
	}

	if !limiter.isFailOpen {
		t.Fatal("expected the default policy to be fail-open")
	}

	if limiter.logger == nil || limiter.now == nil {
		t.Fatal("expected a logger and clock to be installed")
	}
}

func TestWithQuotas(t *testing.T) {
	t.Parallel()

	quotas := map[string]Quota{
		"vendor-a": {Concurrency: 10},
		"vendor-b": {Rate: 160, Window: time.Minute},
	}

	limiter := New(NewMemoryStore(), WithQuotas(quotas))

	for resource, expected := range quotas {
		got, ok := limiter.Quota(resource)
		if !ok {
			t.Fatalf("expected %q to be registered", resource)
		}

		if got != expected {
			t.Fatalf("%q: expected %+v, got %+v", resource, expected, got)
		}
	}
}

func TestOptionsRejectNonPositiveValues(t *testing.T) {
	t.Parallel()

	limiter := New(NewMemoryStore(),
		WithLeaseTTL(0),
		WithBackoff(0, 0),
		WithReleaseTimeout(-time.Second),
	)

	if limiter.leaseTTL != DefaultLeaseTTL {
		t.Fatalf("expected the zero lease TTL to be ignored, got %v", limiter.leaseTTL)
	}

	if limiter.minBackoff != DefaultMinBackoff || limiter.maxBackoff != DefaultMaxBackoff {
		t.Fatalf("expected zero backoff bounds to be ignored, got %v/%v", limiter.minBackoff, limiter.maxBackoff)
	}

	if limiter.releaseTimeout != DefaultReleaseTimeout {
		t.Fatalf("expected the negative release timeout to be ignored, got %v", limiter.releaseTimeout)
	}
}

func TestWithLoggerIgnoresNil(t *testing.T) {
	t.Parallel()

	limiter := New(NewMemoryStore(), WithLogger(nil))
	if limiter.logger == nil {
		t.Fatal("expected the discard logger to be retained when nil is supplied")
	}

	custom := slog.Default()

	limiter = New(NewMemoryStore(), WithLogger(custom))
	if limiter.logger != custom {
		t.Fatal("expected the supplied logger to be installed")
	}
}

func TestWithNowFunc(t *testing.T) {
	t.Parallel()

	fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	limiter := New(NewMemoryStore(), WithNowFunc(func() time.Time { return fixed }))

	if got := limiter.now(); !got.Equal(fixed) {
		t.Fatalf("expected the injected clock to be used, got %v", got)
	}

	limiter = New(NewMemoryStore(), WithNowFunc(nil))
	if limiter.now == nil {
		t.Fatal("expected the default clock to be retained when nil is supplied")
	}
}

func TestWithMaxWaitAcceptsZero(t *testing.T) {
	t.Parallel()

	// Zero is meaningful here, unlike the other durations: it turns Acquire into
	// a single non-blocking attempt.
	limiter := New(NewMemoryStore(), WithMaxWait(0))
	if limiter.maxWait != 0 {
		t.Fatalf("expected a zero max wait to be honored, got %v", limiter.maxWait)
	}
}
