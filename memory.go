package quotapool

import (
	"context"
	"slices"
	"sync"
	"time"
)

// MemoryStore keeps quota state in process memory.
//
// It enforces exactly the semantics [RedisStore] enforces, but only among
// goroutines of a single process. Use it for single-instance deployments and
// for tests; a fleet sharing one provider account needs a distributed store.
type MemoryStore struct {
	mu        sync.Mutex
	resources map[string]*memoryResource
}

// memoryResource holds the permits recorded for one resource. Both dimensions
// store expiry deadlines: concurrency entries expire at their lease deadline,
// rate entries at the end of the window they were admitted in.
type memoryResource struct {
	concurrency map[string]time.Time
	rate        []time.Time
}

// NewMemoryStore returns an empty in-process [Store].
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		resources: map[string]*memoryResource{},
	}
}

// resourceLocked returns the state for a resource, creating it on first use.
// The caller must hold s.mu.
func (s *MemoryStore) resourceLocked(resource string) *memoryResource {
	state, ok := s.resources[resource]
	if !ok {
		state = &memoryResource{
			concurrency: map[string]time.Time{},
			rate:        []time.Time{},
		}
		s.resources[resource] = state
	}

	return state
}

// Acquire implements [Store].
func (s *MemoryStore) Acquire(_ context.Context, req AcquireRequest) (bool, error) {
	if req.Quota.IsUnlimited() || req.Count() == 0 {
		return true, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state := s.resourceLocked(req.Resource)
	state.evict(req.Quota, req.Now)

	// Capacity is checked for every constrained dimension before anything is
	// recorded, so a rejected batch leaves no partial state behind.
	if req.Quota.HasConcurrency() && len(state.concurrency)+req.Count() > req.Quota.Concurrency {
		return false, nil
	}

	if req.Quota.HasRate() && len(state.rate)+req.Count() > req.Quota.Rate {
		return false, nil
	}

	if req.Quota.HasConcurrency() {
		expiry := req.Now.Add(req.LeaseTTL)
		for _, token := range req.Tokens {
			state.concurrency[token] = expiry
		}
	}

	if req.Quota.HasRate() {
		for range req.Tokens {
			state.rate = append(state.rate, req.Now)
		}
	}

	return true, nil
}

// Release implements [Store].
func (s *MemoryStore) Release(_ context.Context, resource string, tokens []string) error {
	if len(tokens) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.resources[resource]
	if !ok {
		return nil
	}

	for _, token := range tokens {
		delete(state.concurrency, token)
	}

	return nil
}

// evict drops permits that no longer count against either dimension: leases
// past their deadline, and rate entries older than the current window.
func (r *memoryResource) evict(quota Quota, now time.Time) {
	for token, expiry := range r.concurrency {
		if !expiry.After(now) {
			delete(r.concurrency, token)
		}
	}

	if !quota.HasRate() {
		r.rate = r.rate[:0]
		return
	}

	cutoff := now.Add(-quota.Window)
	r.rate = slices.DeleteFunc(r.rate, func(admitted time.Time) bool {
		return !admitted.After(cutoff)
	})
}
