package quotapool

import (
	"context"
	"testing"
	"time"
)

func TestBackoff_Next(t *testing.T) {
	t.Parallel()

	const (
		minimum = 10 * time.Millisecond
		maximum = 40 * time.Millisecond
	)

	b := newBackoff(minimum, maximum)

	// Each delay is drawn from the lower half of the current interval, so the
	// observable bound is the ceiling rather than an exact schedule.
	for i := range 8 {
		delay := b.next()
		if delay < 0 || delay > maximum {
			t.Fatalf("attempt %d: delay %v outside [0, %v]", i, delay, maximum)
		}
	}
}

func TestBackoff_NextIsCapped(t *testing.T) {
	t.Parallel()

	b := newBackoff(time.Millisecond, 4*time.Millisecond)

	for range 20 {
		b.next()
	}

	if b.current != 4*time.Millisecond {
		t.Fatalf("expected the interval to settle at the ceiling, got %v", b.current)
	}
}

func TestJitter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    time.Duration
		expected func(time.Duration) bool
	}{
		{
			name:     "zero stays zero",
			input:    0,
			expected: func(d time.Duration) bool { return d == 0 },
		},
		{
			name:     "negative stays zero",
			input:    -time.Second,
			expected: func(d time.Duration) bool { return d == 0 },
		},
		{
			name:  "positive lands in the lower half",
			input: 100 * time.Millisecond,
			expected: func(d time.Duration) bool {
				return d >= 50*time.Millisecond && d <= 100*time.Millisecond
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for range 100 {
				if got := jitter(tt.input); !tt.expected(got) {
					t.Fatalf("jitter(%v) returned %v", tt.input, got)
				}
			}
		})
	}
}

// TestJitterSpreadsWaiters checks the property that motivates jitter: repeated
// draws must not collapse onto a single value, or every waiter would retry in
// lockstep and re-collide on each wake-up.
func TestJitterSpreadsWaiters(t *testing.T) {
	t.Parallel()

	seen := map[time.Duration]bool{}
	for range 200 {
		seen[jitter(100*time.Millisecond)] = true
	}

	if len(seen) < 10 {
		t.Fatalf("expected jitter to spread retries, got %d distinct delays", len(seen))
	}
}

func TestSleep(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ctx      func() (context.Context, context.CancelFunc)
		delay    time.Duration
		expected bool
	}{
		{
			name: "completes normally",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			delay:    time.Millisecond,
			expected: true,
		},
		{
			name: "interrupted by cancellation",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), time.Millisecond)
			},
			delay:    5 * time.Second,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := tt.ctx()
			defer cancel()

			if got := sleep(ctx, tt.delay); got != tt.expected {
				t.Fatalf("expected %v, got %v", tt.expected, got)
			}
		})
	}
}
