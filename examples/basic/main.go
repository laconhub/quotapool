// Command basic shows the shapes provider quotas come in and how a batch is
// admitted atomically.
//
// It runs against the in-process store so no Redis is needed:
//
//	go run ./examples/basic
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/laconhub/quotapool"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	limiter := quotapool.New(
		quotapool.NewMemoryStore(),
		quotapool.WithLogger(logger),
		quotapool.WithMaxWait(200*time.Millisecond),
		quotapool.WithQuotas(map[string]quotapool.Quota{
			"sessions":   {Concurrency: 4},                               // concurrent sessions
			"per-minute": {Rate: 10, Window: time.Minute},                // requests per minute
			"both":       {Concurrency: 2, Rate: 5, Window: time.Second}, // both dimensions
		}),
	)

	ctx := context.Background()

	fmt.Println("== a batch is admitted whole ==")

	release, err := limiter.Acquire(ctx, "sessions", 4)
	if err != nil {
		fmt.Println("acquire:", err)

		return
	}

	fmt.Println("reserved 4 of 4 concurrent sessions")

	// The quota is now exhausted. Under the default fail-open policy the limiter
	// admits the caller anyway and logs a warning, so the request path keeps
	// working when the quota (or the store) does not.
	extra, err := limiter.TryAcquire(ctx, "sessions", 1)
	if err == nil {
		fmt.Println("fail-open: admitted despite the exhausted quota, with a warning logged above")

		extra()
	}

	release()
	fmt.Println("released; capacity is available again")

	fmt.Println()
	fmt.Println("== fail-closed reports the exhaustion instead ==")

	strict := quotapool.New(
		quotapool.NewMemoryStore(),
		quotapool.WithFailClosed(),
		quotapool.WithMaxWait(50*time.Millisecond),
		quotapool.WithQuotas(map[string]quotapool.Quota{"sessions": {Concurrency: 4}}),
	)

	held, err := strict.Acquire(ctx, "sessions", 4)
	if err != nil {
		fmt.Println("acquire:", err)

		return
	}

	if _, err := strict.TryAcquire(ctx, "sessions", 1); errors.Is(err, quotapool.ErrQuotaExceeded) {
		fmt.Println("fail-closed: reported ErrQuotaExceeded")
	}

	held()

	fmt.Println()
	fmt.Println("== an oversized batch resolves immediately ==")

	// Waiting cannot make a batch of 8 fit a limit of 4, so the limiter says so
	// now rather than burning the whole wait budget.
	start := time.Now()

	_, err = strict.Acquire(ctx, "sessions", 8)
	fmt.Printf("batch of 8 against a limit of 4: %v (after %v)\n", errors.Is(err, quotapool.ErrBatchTooLarge), time.Since(start).Round(time.Millisecond))

	fmt.Println()
	fmt.Println("== unregistered resources are unlimited ==")

	release, err = limiter.Acquire(ctx, "not-configured", 1000)
	if err != nil {
		fmt.Println("acquire:", err)

		return
	}

	release()
	fmt.Println("admitted without touching the store")

	fmt.Println()
	fmt.Printf("stats: %+v\n", limiter.Stats())
}
