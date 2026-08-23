//go:build integration

package quotapool

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// newRedisStore connects to the Redis instance named by QUOTAPOOL_REDIS_ADDR,
// defaulting to a local server.
//
// Each store gets a prefix unique to this test *and* this run: a prefix derived
// only from the test name survives in Redis between runs, so the second run of
// a suite starts against capacity the first run already consumed. Keys are also
// removed on cleanup so repeated local runs do not accumulate garbage.
func newRedisStore(t *testing.T) *RedisStore {
	t.Helper()

	addr := os.Getenv("QUOTAPOOL_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	client := redis.NewClient(&redis.Options{Addr: addr})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unavailable at %s: %v", addr, err)
	}

	prefix := "quotapool-test:" + t.Name() + ":" + randomHex()

	t.Cleanup(func() {
		deleteKeys(t, client, prefix+":*")
		_ = client.Close()
	})

	return NewRedisStore(client, WithKeyPrefix(prefix))
}

// deleteKeys removes every key matching pattern, reporting failures as test
// logs rather than errors: leftover keys expire on their own and must not mask
// the result of the test that just ran.
func deleteKeys(t *testing.T, client *redis.Client, pattern string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	iter := client.Scan(ctx, 0, pattern, 100).Iterator()
	for iter.Next(ctx) {
		if err := client.Del(ctx, iter.Val()).Err(); err != nil {
			t.Logf("cleanup: deleting %s: %v", iter.Val(), err)
		}
	}

	if err := iter.Err(); err != nil {
		t.Logf("cleanup: scanning %s: %v", pattern, err)
	}
}

func TestRedisStore_AcquireConcurrency(t *testing.T) {
	t.Parallel()

	store := newRedisStore(t)
	quota := Quota{Concurrency: 4}
	now := time.Now()

	admitted, err := store.Acquire(context.Background(), AcquireRequest{
		Resource: testResource,
		Quota:    quota,
		Tokens:   tokensFor("first", 3),
		LeaseTTL: time.Minute,
		Now:      now,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !admitted {
		t.Fatal("expected the first batch to be admitted")
	}

	admitted, err = store.Acquire(context.Background(), AcquireRequest{
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
		t.Fatal("expected the oversized second batch to be rejected whole")
	}
}

func TestRedisStore_AcquireIsAtomicAcrossDimensions(t *testing.T) {
	t.Parallel()

	store := newRedisStore(t)
	quota := Quota{Concurrency: 10, Rate: 2, Window: time.Minute}
	now := time.Now()

	if _, err := store.Acquire(context.Background(), AcquireRequest{
		Resource: testResource,
		Quota:    quota,
		Tokens:   tokensFor("first", 2),
		LeaseTTL: time.Minute,
		Now:      now,
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Rate is exhausted. This batch fits concurrency, and must still be rejected
	// without consuming concurrency permits.
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

func TestRedisStore_Release(t *testing.T) {
	t.Parallel()

	store := newRedisStore(t)
	quota := Quota{Concurrency: 2}
	now := time.Now()
	held := tokensFor("held", 2)

	if _, err := store.Acquire(context.Background(), AcquireRequest{
		Resource: testResource,
		Quota:    quota,
		Tokens:   held,
		LeaseTTL: time.Minute,
		Now:      now,
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := store.Release(context.Background(), testResource, held); err != nil {
		t.Fatalf("unexpected release error: %v", err)
	}

	admitted, err := store.Acquire(context.Background(), AcquireRequest{
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

func TestRedisStore_LeaseExpiryReclaimsCapacity(t *testing.T) {
	t.Parallel()

	store := newRedisStore(t)
	quota := Quota{Concurrency: 2}
	now := time.Now()

	if _, err := store.Acquire(context.Background(), AcquireRequest{
		Resource: testResource,
		Quota:    quota,
		Tokens:   tokensFor("abandoned", 2),
		LeaseTTL: 5 * time.Second,
		Now:      now,
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	admitted, err := store.Acquire(context.Background(), AcquireRequest{
		Resource: testResource,
		Quota:    quota,
		Tokens:   tokensFor("after-expiry", 2),
		LeaseTTL: 5 * time.Second,
		Now:      now.Add(6 * time.Second),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !admitted {
		t.Fatal("expected expired leases to free capacity")
	}
}

// TestRedisStore_LimiterEndToEnd exercises the full path a user takes, which is
// the only place the limiter and a real store are wired together.
func TestRedisStore_LimiterEndToEnd(t *testing.T) {
	t.Parallel()

	limiter := New(newRedisStore(t),
		WithMaxWait(50*time.Millisecond),
		WithBackoff(time.Millisecond, 5*time.Millisecond),
		WithFailClosed(),
		WithQuotas(map[string]Quota{testResource: {Concurrency: 4}}),
	)

	release, err := limiter.Acquire(context.Background(), testResource, 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := limiter.TryAcquire(context.Background(), testResource, 1); err == nil {
		t.Fatal("expected the exhausted quota to reject the second caller")
	}

	release()

	release, err = limiter.Acquire(context.Background(), testResource, 4)
	if err != nil {
		t.Fatalf("expected capacity to be reusable after release, got %v", err)
	}

	release()
}
