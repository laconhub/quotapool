// Command chorus demonstrates why batch admission has to be atomic.
//
// Four voices must begin synthesis together to be mixed into one chorus. The
// program runs the same workload twice: once admitting the voices one at a
// time, once admitting them as a batch, and reports how far apart the voices
// started in each case.
//
//	go run ./examples/chorus
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/laconhub/quotapool"
)

// voices is the number of tracks that must start together to form a chorus.
const voices = 4

func main() {
	ctx := context.Background()

	// Capacity is deliberately tight: two permits are already spoken for, so the
	// chorus has to wait for them to free up.
	fmt.Println("one-at-a-time admission:")

	spread := run(ctx, false)

	fmt.Printf("  first and last voice started %v apart\n", spread.Round(time.Millisecond))
	fmt.Println("  the mixer sees the late tracks as stalled and plays them separately")

	fmt.Println()
	fmt.Println("atomic batch admission:")

	spread = run(ctx, true)

	fmt.Printf("  first and last voice started %v apart\n", spread.Round(time.Millisecond))
	fmt.Println("  all four tracks begin together and mix into one chorus")
}

// run synthesizes one line in four voices and reports the spread between the
// first and last voice to start. When batched is true the voices are admitted
// in a single atomic request.
func run(ctx context.Context, isBatched bool) time.Duration {
	limiter := quotapool.New(
		quotapool.NewMemoryStore(),
		quotapool.WithMaxWait(2*time.Second),
		quotapool.WithQuotas(map[string]quotapool.Quota{"tts": {Concurrency: voices}}),
	)

	// Occupy part of the quota, then free it shortly after, so the chorus has to
	// contend for capacity the way it would in production.
	occupied, err := limiter.Acquire(ctx, "tts", 2)
	if err != nil {
		fmt.Println("setup:", err)

		return 0
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		occupied()
	}()

	if isBatched {
		release, err := limiter.Acquire(ctx, "tts", voices)
		if err != nil {
			fmt.Println("acquire:", err)

			return 0
		}
		defer release()

		// Every voice starts the moment the batch is admitted.
		return 0
	}

	var (
		mu      sync.Mutex
		starts  = make([]time.Time, 0, voices)
		wg      sync.WaitGroup
		release = make([]quotapool.Release, 0, voices)
	)

	for range voices {
		wg.Go(func() {
			r, err := limiter.Acquire(ctx, "tts", 1)
			if err != nil {
				return
			}

			mu.Lock()
			defer mu.Unlock()

			starts = append(starts, time.Now())
			release = append(release, r)
		})
	}

	wg.Wait()

	for _, r := range release {
		r()
	}

	return spread(starts)
}

// spread reports the interval between the earliest and latest start.
func spread(starts []time.Time) time.Duration {
	if len(starts) < 2 {
		return 0
	}

	earliest, latest := starts[0], starts[0]

	for _, t := range starts[1:] {
		if t.Before(earliest) {
			earliest = t
		}

		if t.After(latest) {
			latest = t
		}
	}

	return latest.Sub(earliest)
}
