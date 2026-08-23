// Package quotapool provides a distributed quota limiter for third-party API
// providers whose rate limits differ in shape.
//
// Most limiters model a single dimension ("N per second") and hand out one
// permit at a time. Provider quotas rarely fit that shape:
//
//   - some vendors cap concurrent sessions;
//   - some cap requests per minute;
//   - some cap requests per second;
//   - some cap several of these at once.
//
// quotapool models both a concurrency dimension (a distributed semaphore with
// leases) and a rate dimension (a sliding window), configured per resource, and
// admits them together in a single atomic step.
//
// # Atomic batch admission
//
// The defining feature is atomic multi-permit admission: Acquire(ctx, res, n)
// either reserves all n permits or none. This matters whenever n units of work
// must start together. Admitting them one at a time lets the first few start
// while the rest queue behind the limit, and callers that assume simultaneity
// observe the partial start as a stall or a failure.
//
// A concrete case: synthesizing one line of speech in four voices and mixing the
// results. If the limiter admits the voices at different times, the mixer treats
// the late tracks as stalled and emits them separately, so the listener hears
// the same line read four times in a row instead of once in chorus.
//
// # Failure policy
//
// By default the limiter fails open: when the backing store errors, the wait
// budget expires, or the request can never fit the configured quota, Acquire
// logs and admits the caller anyway. For interactive systems an occasional
// provider-side rejection is usually preferable to blocking the request path.
// Call [WithFailClosed] to invert this.
//
// # Usage
//
//	store := quotapool.NewRedisStore(rdb)
//	limiter := quotapool.New(store, quotapool.WithQuotas(map[string]quotapool.Quota{
//		"vendor-a": {Concurrency: 10},
//		"vendor-b": {Rate: 160, Window: time.Minute},
//	}))
//
//	release, err := limiter.Acquire(ctx, "vendor-a", 4)
//	if err != nil {
//		return err
//	}
//	defer release()
//
// A [Limiter] is safe for concurrent use by multiple goroutines.
package quotapool
