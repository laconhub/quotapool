package quotapool

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// DefaultKeyPrefix is the namespace RedisStore prepends to every key it owns.
const DefaultKeyPrefix = "quotapool"

// RedisClient is the subset of the go-redis API that [RedisStore] depends on.
//
// It is satisfied by *redis.Client, *redis.ClusterClient, *redis.Ring and
// redis.UniversalClient, so callers can reuse whichever client they already
// have without adapting it.
type RedisClient interface {
	redis.Scripter

	ZRem(ctx context.Context, key string, members ...any) *redis.IntCmd
}

// acquireScript admits a batch against both dimensions in one atomic step.
//
// Capacity is checked for every constrained dimension before anything is
// written, so a batch that fits the concurrency limit but not the rate limit
// leaves no partial state behind. Checking and committing in separate round
// trips would let a batch consume rate tokens it never gets to use once the
// concurrency check fails, silently shrinking the effective rate limit.
//
// KEYS[1] concurrency key, KEYS[2] rate key.
// ARGV[1] now (ms), ARGV[2] batch size, ARGV[3] concurrency limit,
// ARGV[4] lease (ms), ARGV[5] rate limit, ARGV[6] window (ms),
// ARGV[7..] one member identifier per permit.
// Returns 1 when the whole batch was admitted, 0 otherwise.
//
//nolint:dupword // Lua block terminators repeat by design; not prose.
var acquireScript = redis.NewScript(`
local now       = tonumber(ARGV[1])
local n         = tonumber(ARGV[2])
local concLimit = tonumber(ARGV[3])
local leaseMs   = tonumber(ARGV[4])
local rateLimit = tonumber(ARGV[5])
local windowMs  = tonumber(ARGV[6])

if concLimit > 0 then
    redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
    if redis.call('ZCARD', KEYS[1]) + n > concLimit then
        return 0
    end
end

if rateLimit > 0 then
    redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now - windowMs)
    if redis.call('ZCARD', KEYS[2]) + n > rateLimit then
        return 0
    end
end

if concLimit > 0 then
    local expiry = now + leaseMs
    for i = 1, n do
        redis.call('ZADD', KEYS[1], expiry, ARGV[6 + i])
    end
    redis.call('PEXPIRE', KEYS[1], leaseMs * 2)
end

if rateLimit > 0 then
    for i = 1, n do
        redis.call('ZADD', KEYS[2], now, ARGV[6 + i])
    end
    redis.call('PEXPIRE', KEYS[2], windowMs)
end

return 1
`)

// RedisStore keeps quota state in Redis so every process sharing the same
// provider account draws from one pool.
//
// Both dimensions are sorted sets scored by expiry: concurrency permits carry
// their lease deadline, rate permits carry their admission time. Expired
// members are trimmed on each attempt, which keeps a crashed process from
// holding capacity forever.
type RedisStore struct {
	client RedisClient
	prefix string
}

// RedisStoreOption customizes a [RedisStore].
type RedisStoreOption func(*RedisStore)

// WithKeyPrefix overrides the key namespace, allowing several independent
// limiters to share one Redis instance.
func WithKeyPrefix(prefix string) RedisStoreOption {
	return func(s *RedisStore) {
		if prefix != "" {
			s.prefix = prefix
		}
	}
}

// NewRedisStore returns a [Store] backed by the given Redis client.
//
// The client is not closed by the store; its lifetime stays with the caller.
func NewRedisStore(client RedisClient, opts ...RedisStoreOption) *RedisStore {
	store := &RedisStore{
		client: client,
		prefix: DefaultKeyPrefix,
	}

	for _, opt := range opts {
		opt(store)
	}

	return store
}

// concurrencyKey and rateKey wrap the resource in a hash tag so both keys land
// on the same Redis Cluster slot, which the multi-key script requires.
func (s *RedisStore) concurrencyKey(resource string) string {
	return s.prefix + ":{" + resource + "}:concurrency"
}

func (s *RedisStore) rateKey(resource string) string {
	return s.prefix + ":{" + resource + "}:rate"
}

// Acquire implements [Store].
func (s *RedisStore) Acquire(ctx context.Context, req AcquireRequest) (bool, error) {
	if req.Quota.IsUnlimited() || req.Count() == 0 {
		return true, nil
	}

	keys := []string{
		s.concurrencyKey(req.Resource),
		s.rateKey(req.Resource),
	}

	args := make([]any, 0, 6+req.Count())
	args = append(args,
		req.Now.UnixMilli(),
		req.Count(),
		req.Quota.Concurrency,
		req.LeaseTTL.Milliseconds(),
		req.Quota.Rate,
		req.Quota.Window.Milliseconds(),
	)

	for _, token := range req.Tokens {
		args = append(args, token)
	}

	admitted, err := acquireScript.Run(ctx, s.client, keys, args...).Int64()
	if err != nil {
		return false, fmt.Errorf("quotapool: running acquire script: %w", err)
	}

	return admitted == 1, nil
}

// Release implements [Store].
func (s *RedisStore) Release(ctx context.Context, resource string, tokens []string) error {
	if len(tokens) == 0 {
		return nil
	}

	members := make([]any, len(tokens))
	for i, token := range tokens {
		members[i] = token
	}

	if err := s.client.ZRem(ctx, s.concurrencyKey(resource), members...).Err(); err != nil {
		return fmt.Errorf("quotapool: releasing concurrency permits: %w", err)
	}

	return nil
}
