package quotapool

import (
	"context"
	"testing"
	"time"
)

func BenchmarkLimiter_AcquireUnlimited(b *testing.B) {
	limiter := New(NewMemoryStore())
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		release, err := limiter.Acquire(ctx, "unregistered", 1)
		if err != nil {
			b.Fatal(err)
		}

		release()
	}
}

func BenchmarkLimiter_AcquireConcurrency(b *testing.B) {
	limiter := New(NewMemoryStore(), WithQuotas(map[string]Quota{
		testResource: {Concurrency: 1024},
	}))
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		release, err := limiter.Acquire(ctx, testResource, 1)
		if err != nil {
			b.Fatal(err)
		}

		release()
	}
}

func BenchmarkLimiter_AcquireBatch(b *testing.B) {
	for _, size := range []int{1, 4, 16} {
		b.Run(benchName(size), func(b *testing.B) {
			limiter := New(NewMemoryStore(), WithQuotas(map[string]Quota{
				testResource: {Concurrency: 1024},
			}))
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				release, err := limiter.Acquire(ctx, testResource, size)
				if err != nil {
					b.Fatal(err)
				}

				release()
			}
		})
	}
}

func BenchmarkLimiter_AcquireParallel(b *testing.B) {
	limiter := New(NewMemoryStore(), WithQuotas(map[string]Quota{
		testResource: {Concurrency: 4096},
	}))

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()

		for pb.Next() {
			release, err := limiter.Acquire(ctx, testResource, 1)
			if err != nil {
				b.Fatal(err)
			}

			release()
		}
	})
}

func BenchmarkLimiter_AcquireBothDimensions(b *testing.B) {
	limiter := New(NewMemoryStore(), WithQuotas(map[string]Quota{
		testResource: {Concurrency: 1024, Rate: 1_000_000, Window: time.Hour},
	}))
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		release, err := limiter.Acquire(ctx, testResource, 1)
		if err != nil {
			b.Fatal(err)
		}

		release()
	}
}

func BenchmarkNewTokens(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		_ = newTokens(4)
	}
}

// benchName renders a batch size as a stable sub-benchmark name.
func benchName(size int) string {
	switch size {
	case 1:
		return "batch-1"
	case 4:
		return "batch-4"
	default:
		return "batch-16"
	}
}
