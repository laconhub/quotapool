package quotapool

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"
)

// tokensFor builds n distinct tokens with a recognizable prefix.
func tokensFor(prefix string, n int) []string {
	tokens := make([]string, n)
	for i := range tokens {
		tokens[i] = prefix + ":" + strconv.Itoa(i)
	}

	return tokens
}

func TestMemoryStore_Acquire(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		quota    Quota
		batches  []int
		expected []bool
	}{
		{
			name:     "unlimited quota admits everything",
			quota:    Quota{},
			batches:  []int{100, 100},
			expected: []bool{true, true},
		},
		{
			name:     "concurrency admits up to the limit",
			quota:    Quota{Concurrency: 4},
			batches:  []int{2, 2, 1},
			expected: []bool{true, true, false},
		},
		{
			name:     "batch is rejected whole when it does not fit",
			quota:    Quota{Concurrency: 4},
			batches:  []int{3, 2},
			expected: []bool{true, false},
		},
		{
			name:     "rate admits up to the limit within the window",
			quota:    Quota{Rate: 3, Window: time.Minute},
			batches:  []int{2, 1, 1},
			expected: []bool{true, true, false},
		},
		{
			name:     "both dimensions must fit",
			quota:    Quota{Concurrency: 10, Rate: 2, Window: time.Minute},
			batches:  []int{2, 1},
			expected: []bool{true, false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := NewMemoryStore()

			for i, size := range tt.batches {
				admitted, err := store.Acquire(context.Background(), AcquireRequest{
					Resource: testResource,
					Quota:    tt.quota,
					Tokens:   tokensFor("batch"+strconv.Itoa(i), size),
					LeaseTTL: time.Minute,
					Now:      now,
				})
				if err != nil {
					t.Fatalf("batch %d: unexpected error: %v", i, err)
				}

				if admitted != tt.expected[i] {
					t.Fatalf("batch %d: expected admitted=%v, got %v", i, tt.expected[i], admitted)
				}
			}
		})
	}
}

// TestMemoryStore_AcquireIsAllOrNothing verifies the defining guarantee: a
// rejected batch must leave no permits behind, otherwise a later batch would be
// throttled by capacity nobody holds.
func TestMemoryStore_AcquireIsAllOrNothing(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	quota := Quota{Concurrency: 4}
	now := time.Now()

	admitted, err := store.Acquire(context.Background(), AcquireRequest{
		Resource: testResource,
		Quota:    quota,
		Tokens:   tokensFor("oversized", 5),
		LeaseTTL: time.Minute,
		Now:      now,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if admitted {
		t.Fatal("expected oversized batch to be rejected")
	}

	admitted, err = store.Acquire(context.Background(), AcquireRequest{
		Resource: testResource,
		Quota:    quota,
		Tokens:   tokensFor("fitting", 4),
		LeaseTTL: time.Minute,
		Now:      now,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !admitted {
		t.Fatal("rejected batch consumed capacity: full quota should still be free")
	}
}

// TestMemoryStore_AcquireRejectsPartialCommitAcrossDimensions covers the case
// that motivates checking both dimensions before writing either: a batch that
// fits concurrency but not rate must not consume concurrency permits.
func TestMemoryStore_AcquireRejectsPartialCommitAcrossDimensions(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	quota := Quota{Concurrency: 10, Rate: 2, Window: time.Minute}
	now := time.Now()

	_, err := store.Acquire(context.Background(), AcquireRequest{
		Resource: testResource,
		Quota:    quota,
		Tokens:   tokensFor("first", 2),
		LeaseTTL: time.Minute,
		Now:      now,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Rate is now exhausted; this batch fits concurrency but must be rejected.
	admitted, err := store.Acquire(context.Background(), AcquireRequest{
		Resource: testResource,
		Quota:    quota,
		Tokens:   tokensFor("second", 2),
		LeaseTTL: time.Minute,
		Now:      now,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if admitted {
		t.Fatal("expected rate exhaustion to reject the batch")
	}

	// The rejected batch must not have taken concurrency permits: after the rate
	// window rolls over, the full concurrency limit should still be available.
	admitted, err = store.Acquire(context.Background(), AcquireRequest{
		Resource: testResource,
		Quota:    quota,
		Tokens:   tokensFor("third", 2),
		LeaseTTL: time.Minute,
		Now:      now.Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !admitted {
		t.Fatal("rejected batch leaked concurrency permits")
	}
}

func TestMemoryStore_Release(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	quota := Quota{Concurrency: 2}
	now := time.Now()
	held := tokensFor("held", 2)

	admitted, err := store.Acquire(context.Background(), AcquireRequest{
		Resource: testResource,
		Quota:    quota,
		Tokens:   held,
		LeaseTTL: time.Minute,
		Now:      now,
	})
	if err != nil || !admitted {
		t.Fatalf("setup failed: admitted=%v err=%v", admitted, err)
	}

	if err := store.Release(context.Background(), testResource, held); err != nil {
		t.Fatalf("unexpected release error: %v", err)
	}

	admitted, err = store.Acquire(context.Background(), AcquireRequest{
		Resource: testResource,
		Quota:    quota,
		Tokens:   tokensFor("next", 2),
		LeaseTTL: time.Minute,
		Now:      now,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !admitted {
		t.Fatal("expected released capacity to be reusable")
	}
}

func TestMemoryStore_ReleaseUnknownTokensSucceeds(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()

	if err := store.Release(context.Background(), "never-seen", tokensFor("ghost", 2)); err != nil {
		t.Fatalf("expected releasing unknown tokens to succeed, got %v", err)
	}
}

// TestMemoryStore_LeaseExpiryReclaimsCapacity covers recovery from a process
// that acquired permits and died without releasing them.
func TestMemoryStore_LeaseExpiryReclaimsCapacity(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	quota := Quota{Concurrency: 2}
	now := time.Now()

	admitted, err := store.Acquire(context.Background(), AcquireRequest{
		Resource: testResource,
		Quota:    quota,
		Tokens:   tokensFor("abandoned", 2),
		LeaseTTL: 30 * time.Second,
		Now:      now,
	})
	if err != nil || !admitted {
		t.Fatalf("setup failed: admitted=%v err=%v", admitted, err)
	}

	admitted, err = store.Acquire(context.Background(), AcquireRequest{
		Resource: testResource,
		Quota:    quota,
		Tokens:   tokensFor("after-expiry", 2),
		LeaseTTL: 30 * time.Second,
		Now:      now.Add(31 * time.Second),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !admitted {
		t.Fatal("expected expired leases to free capacity")
	}
}

func TestMemoryStore_RateWindowSlides(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	quota := Quota{Rate: 2, Window: time.Second}
	now := time.Now()

	for i := range 2 {
		admitted, err := store.Acquire(context.Background(), AcquireRequest{
			Resource: testResource,
			Quota:    quota,
			Tokens:   tokensFor("early"+strconv.Itoa(i), 1),
			Now:      now,
		})
		if err != nil || !admitted {
			t.Fatalf("setup failed at %d: admitted=%v err=%v", i, admitted, err)
		}
	}

	admitted, err := store.Acquire(context.Background(), AcquireRequest{
		Resource: testResource,
		Quota:    quota,
		Tokens:   tokensFor("blocked", 1),
		Now:      now,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if admitted {
		t.Fatal("expected the window to be full")
	}

	admitted, err = store.Acquire(context.Background(), AcquireRequest{
		Resource: testResource,
		Quota:    quota,
		Tokens:   tokensFor("later", 1),
		Now:      now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !admitted {
		t.Fatal("expected the window to have slid past earlier permits")
	}
}

func TestMemoryStore_ConcurrentAcquireNeverExceedsLimit(t *testing.T) {
	t.Parallel()

	const (
		limit   = 8
		workers = 64
	)

	store := NewMemoryStore()
	quota := Quota{Concurrency: limit}
	now := time.Now()

	var (
		mu       sync.Mutex
		admitted int
		wg       sync.WaitGroup
	)

	for i := range workers {
		wg.Go(func() {
			ok, err := store.Acquire(context.Background(), AcquireRequest{
				Resource: testResource,
				Quota:    quota,
				Tokens:   tokensFor("worker"+strconv.Itoa(i), 1),
				LeaseTTL: time.Minute,
				Now:      now,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)

				return
			}

			if ok {
				mu.Lock()
				admitted++
				mu.Unlock()
			}
		})
	}

	wg.Wait()

	if admitted != limit {
		t.Fatalf("expected exactly %d admissions, got %d", limit, admitted)
	}
}
