package quotapool_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/laconhub/quotapool"
)

// Limits differ in shape between providers: some cap concurrent sessions,
// others cap requests per interval, and some cap both.
func ExampleNew() {
	limiter := quotapool.New(quotapool.NewMemoryStore(), quotapool.WithQuotas(map[string]quotapool.Quota{
		"vendor-a": {Concurrency: 10},
		"vendor-b": {Rate: 160, Window: time.Minute},
		"vendor-c": {Concurrency: 4, Rate: 3, Window: time.Second},
	}))

	quota, ok := limiter.Quota("vendor-c")
	fmt.Println(ok, quota.Concurrency, quota.Rate)
	// Output: true 4 3
}

// Acquire reserves the whole batch or none of it, so callers that need several
// permits to start together never observe a partial start.
func ExampleLimiter_Acquire() {
	limiter := quotapool.New(quotapool.NewMemoryStore(), quotapool.WithQuotas(map[string]quotapool.Quota{
		"speech": {Concurrency: 8},
	}))

	release, err := limiter.Acquire(context.Background(), "speech", 4)
	if err != nil {
		fmt.Println("acquire:", err)

		return
	}
	defer release()

	fmt.Println("all four voices admitted together")
	// Output: all four voices admitted together
}

// TryAcquire makes a single attempt, which suits callers that have other work
// to do instead of waiting for capacity.
func ExampleLimiter_TryAcquire() {
	limiter := quotapool.New(quotapool.NewMemoryStore(),
		quotapool.WithFailClosed(),
		quotapool.WithQuotas(map[string]quotapool.Quota{"vendor": {Concurrency: 1}}),
	)

	held, err := limiter.Acquire(context.Background(), "vendor", 1)
	if err != nil {
		fmt.Println("acquire:", err)

		return
	}
	defer held()

	if _, err := limiter.TryAcquire(context.Background(), "vendor", 1); errors.Is(err, quotapool.ErrQuotaExceeded) {
		fmt.Println("no capacity right now")
	}
	// Output: no capacity right now
}

// The default policy admits the caller when capacity cannot be confirmed.
// WithFailClosed inverts that for paths where over-admitting is worse than
// blocking.
func ExampleWithFailClosed() {
	limiter := quotapool.New(quotapool.NewMemoryStore(),
		quotapool.WithFailClosed(),
		quotapool.WithMaxWait(10*time.Millisecond),
		quotapool.WithQuotas(map[string]quotapool.Quota{"vendor": {Concurrency: 2}}),
	)

	// A batch larger than the quota can never be admitted, so it resolves
	// immediately instead of consuming the wait budget.
	_, err := limiter.Acquire(context.Background(), "vendor", 5)
	fmt.Println(errors.Is(err, quotapool.ErrBatchTooLarge))
	// Output: true
}

// Stats answers the first question asked when a provider starts refusing
// calls: how often is this limiter turning work away?
func ExampleLimiter_Stats() {
	limiter := quotapool.New(quotapool.NewMemoryStore(), quotapool.WithQuotas(map[string]quotapool.Quota{
		"vendor": {Concurrency: 4},
	}))

	release, err := limiter.Acquire(context.Background(), "vendor", 2)
	if err != nil {
		fmt.Println("acquire:", err)

		return
	}

	release()

	stats := limiter.Stats()
	fmt.Println(stats.Admitted, stats.Released, stats.Rejected)
	// Output: 1 1 0
}
