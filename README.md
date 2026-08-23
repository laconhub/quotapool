# quotapool

[![Go Reference](https://pkg.go.dev/badge/github.com/laconhub/quotapool.svg)](https://pkg.go.dev/github.com/laconhub/quotapool)
[![Go Report Card](https://goreportcard.com/badge/github.com/laconhub/quotapool)](https://goreportcard.com/report/github.com/laconhub/quotapool)
[![Tests](https://github.com/laconhub/quotapool/actions/workflows/test.yml/badge.svg)](https://github.com/laconhub/quotapool/actions/workflows/test.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**A distributed quota limiter for third-party API providers — with atomic batch admission.**

English | [简体中文](README.zh-CN.md)

---

## Why

Most rate limiters model one dimension (`N per second`) and hand out one permit at a time. Real provider quotas rarely fit that shape:

| Provider style | Limit shape |
| --- | --- |
| Realtime speech vendors | max **concurrent sessions** |
| Cloud LLM APIs | max **requests per minute** |
| Voice cloning services | max **requests per second** |
| Many vendors | **several of these at once** |

And when several service replicas share one provider account, a process-local limiter cannot see the quota the rest of the fleet is already consuming.

`quotapool` models both a **concurrency** dimension (a distributed semaphore with leases) and a **rate** dimension (a sliding window), per resource, and admits them **together in a single atomic step**.

## The feature that matters: atomic batch admission

```go
release, err := limiter.Acquire(ctx, "vendor-a", 4) // all four, or none
```

`Acquire(ctx, resource, n)` reserves **all `n` permits or none of them**. This is not a convenience wrapper around `n` single acquisitions — it is the point of the library.

Whenever `n` units of work must *start together*, admitting them one at a time is a correctness bug, not a performance detail. The first few start, the rest queue behind the limit, and any caller that assumed simultaneity observes the partial start as a stall or a failure.

<details>
<summary><b>A concrete failure this prevents</b></summary>

Synthesizing one line of speech in four voices, then mixing the results into a chorus.

If the limiter admits the four voices at different times, the mixer treats the late tracks as stalled and emits them separately. The listener hears the same line read four times in a row instead of once in chorus — a product-visible defect produced entirely by the limiter's admission granularity.

</details>

## Install

```bash
go get github.com/laconhub/quotapool
```

Requires Go 1.25+.

## Quick start

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/laconhub/quotapool"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	limiter := quotapool.New(
		quotapool.NewRedisStore(rdb),
		quotapool.WithQuotas(map[string]quotapool.Quota{
			"vendor-a": {Concurrency: 10},                               // concurrent sessions
			"vendor-b": {Rate: 160, Window: time.Minute},                // requests per minute
			"vendor-c": {Rate: 3, Window: time.Second},                  // requests per second
			"vendor-d": {Concurrency: 4, Rate: 100, Window: time.Minute}, // both
		}),
	)

	// Reserve four permits atomically.
	release, err := limiter.Acquire(context.Background(), "vendor-a", 4)
	if err != nil {
		log.Fatal(err)
	}
	defer release()

	// ... do the work that needed all four permits ...
}
```

## Core concepts

### Quota shapes

```go
quotapool.Quota{Concurrency: 10}                        // 10 simultaneous holders
quotapool.Quota{Rate: 160, Window: time.Minute}         // 160 admissions per minute
quotapool.Quota{Concurrency: 4, Rate: 3, Window: time.Second} // both enforced
quotapool.Quota{}                                       // unlimited — Acquire is a no-op
```

A zero value on either dimension means that dimension is unconstrained. An unregistered resource is unlimited, and never touches the store.

Quotas can be changed at runtime, which is useful when a provider revises an account's limits without a deployment:

```go
err := limiter.SetQuota("vendor-a", quotapool.Quota{Concurrency: 20})
```

### Concurrency permits are leased

Concurrency permits are returned by the `release` function `Acquire` hands back. They also carry a lease (`WithLeaseTTL`, default 30s), so a process that crashes mid-work does not pin capacity forever.

Set the lease comfortably above your longest expected hold: too short and live work loses its permit, too long and a crashed process holds capacity until the lease lapses.

Rate permits are never returned — they age out of their window on their own.

### Fail-open by default

When capacity cannot be confirmed — the store is unreachable, the wait budget expired, or the batch can never fit — the limiter **admits the caller anyway** and logs at warn level.

```go
limiter := quotapool.New(store, quotapool.WithFailClosed()) // invert
```

Choose by asking which failure your system tolerates better:

| Policy | Risk | Typical use |
| --- | --- | --- |
| Fail-open (default) | an occasional provider-side rejection | interactive request paths |
| Fail-closed | blocked work while the store is unhealthy | billing, abuse control |

For an interactive system, a limiter that goes down should not take the request path with it.

### Blocking and non-blocking

```go
release, err := limiter.Acquire(ctx, "vendor", 4)    // waits up to WithMaxWait (default 8s)
release, err := limiter.TryAcquire(ctx, "vendor", 4) // single attempt, returns immediately
```

While waiting, retries use exponential backoff with jitter. Jitter matters more than the growth curve: without it, every process blocked on the same exhausted resource retries in lockstep and re-collides on each wake-up, so capacity freed by one release is contended by all waiters at once.

## Stores

| Store | Scope | Use for |
| --- | --- | --- |
| `NewRedisStore(client)` | every process sharing the store | fleets sharing one provider account |
| `NewMemoryStore()` | one process | single-instance deployments, tests |

`RedisClient` is a narrow interface satisfied by `*redis.Client`, `*redis.ClusterClient`, `*redis.Ring` and `redis.UniversalClient`, so you can pass whichever client you already have.

Implement `Store` yourself to back the limiter with something else. The contract is one method pair, and the atomicity guarantee in `Acquire` is what makes batch admission meaningful.

### How the Redis store works

Both dimensions are sorted sets scored by expiry — concurrency permits carry their lease deadline, rate permits carry their admission time. Expired members are trimmed on every attempt.

Admission runs as a single Lua script that **checks every constrained dimension before writing anything**. Checking and committing in separate round trips would let a batch consume rate tokens it never gets to use once the concurrency check fails, silently shrinking the effective rate limit over time.

Keys are wrapped in a hash tag (`quotapool:{resource}:concurrency`) so both land on the same Redis Cluster slot.

## Observability

```go
stats := limiter.Stats()
// Admitted, Rejected, FailedOpen, FailedClosed, Released, ReleaseFailed
```

The `Rejected`/`Admitted` ratio answers the first question asked when a provider starts refusing calls: *am I being throttled?* A rising `ReleaseFailed` means permits are being reclaimed by lease expiry rather than returned, so effective capacity is lower than configured.

Attach a logger to see every time the failure policy is exercised:

```go
limiter := quotapool.New(store, quotapool.WithLogger(slog.Default()))
```

## Configuration

| Option | Default | Purpose |
| --- | --- | --- |
| `WithQuotas(map)` | none | per-resource limits |
| `WithMaxWait(d)` | `8s` | how long `Acquire` blocks |
| `WithLeaseTTL(d)` | `30s` | lease on concurrency permits |
| `WithBackoff(min, max)` | `40ms`, `250ms` | retry interval bounds |
| `WithReleaseTimeout(d)` | `3s` | timeout for returning permits |
| `WithFailClosed()` | fail-open | invert the failure policy |
| `WithLogger(l)` | discard | `*slog.Logger` for diagnostics |
| `WithNowFunc(f)` | `time.Now` | inject a clock in tests |

## Errors

Reported only when configured with `WithFailClosed()`:

| Error | Meaning |
| --- | --- |
| `ErrQuotaExceeded` | wait budget expired before capacity freed |
| `ErrBatchTooLarge` | batch larger than a configured dimension — waiting cannot help |
| `ErrInvalidCount` | non-positive batch size |
| `ErrInvalidQuota` | `SetQuota` received inconsistent limits |

## Benchmarks

`MemoryStore`, Apple M4 Pro, Go 1.25:

```
BenchmarkLimiter_AcquireUnlimited-14        17.50 ns/op      0 B/op    0 allocs/op
BenchmarkLimiter_AcquireBatch/batch-1-14     1397 ns/op    752 B/op   14 allocs/op
BenchmarkLimiter_AcquireBatch/batch-4-14     1375 ns/op    944 B/op   18 allocs/op
BenchmarkLimiter_AcquireBatch/batch-16-14    2786 ns/op   1713 B/op   30 allocs/op
```

Unregistered resources short-circuit before any store call. With `RedisStore` the cost is dominated by the round trip.

## Testing

```bash
make test              # unit tests with race detection
make test-integration  # requires a Redis instance
make lint
make benchmark
```

Integration tests are behind the `integration` build tag and read `QUOTAPOOL_REDIS_ADDR` (default `localhost:6379`). They skip cleanly when no server is reachable.

## FAQ

**Is it safe for concurrent use?** Yes. A `Limiter` is safe for use by multiple goroutines.

**What if `release()` is never called?** The permits are reclaimed when their lease expires. Calling `release()` more than once is safe; only the first call reaches the store.

**Can I use one Redis for several independent limiters?** Yes — give each a namespace with `WithKeyPrefix`.

**Does this replace `golang.org/x/time/rate`?** No. That limiter is process-local, single-dimension, and single-permit. Use it when those fit; use this one when the quota is shared across processes, has more than one shape, or must be taken in batches.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[Apache License 2.0](LICENSE)
