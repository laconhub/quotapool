package quotapool

import (
	"errors"
	"testing"
	"time"
)

func TestQuota_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		quota       Quota
		expectError bool
	}{
		{
			name:        "zero quota is unlimited and valid",
			quota:       Quota{},
			expectError: false,
		},
		{
			name:        "concurrency only",
			quota:       Quota{Concurrency: 10},
			expectError: false,
		},
		{
			name:        "rate with window",
			quota:       Quota{Rate: 100, Window: time.Minute},
			expectError: false,
		},
		{
			name:        "both dimensions",
			quota:       Quota{Concurrency: 5, Rate: 100, Window: time.Second},
			expectError: false,
		},
		{
			name:        "negative concurrency",
			quota:       Quota{Concurrency: -1},
			expectError: true,
		},
		{
			name:        "negative rate",
			quota:       Quota{Rate: -1},
			expectError: true,
		},
		{
			name:        "rate without window",
			quota:       Quota{Rate: 100},
			expectError: true,
		},
		{
			name:        "rate with negative window",
			quota:       Quota{Rate: 100, Window: -time.Second},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.quota.Validate()
			if tt.expectError {
				if !errors.Is(err, ErrInvalidQuota) {
					t.Fatalf("expected ErrInvalidQuota, got %v", err)
				}

				return
			}

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}

func TestQuota_IsUnlimited(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		quota    Quota
		expected bool
	}{
		{name: "zero value", quota: Quota{}, expected: true},
		{name: "concurrency set", quota: Quota{Concurrency: 1}, expected: false},
		{name: "rate set", quota: Quota{Rate: 1, Window: time.Second}, expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.quota.IsUnlimited(); got != tt.expected {
				t.Fatalf("expected %v, got %v", tt.expected, got)
			}
		})
	}
}

func TestQuota_Fits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		quota    Quota
		count    int
		expected bool
	}{
		{
			name:     "batch within concurrency",
			quota:    Quota{Concurrency: 4},
			count:    4,
			expected: true,
		},
		{
			name:     "batch exceeds concurrency",
			quota:    Quota{Concurrency: 4},
			count:    5,
			expected: false,
		},
		{
			name:     "batch exceeds rate",
			quota:    Quota{Rate: 3, Window: time.Second},
			count:    4,
			expected: false,
		},
		{
			name:     "batch fits one dimension but not the other",
			quota:    Quota{Concurrency: 10, Rate: 2, Window: time.Second},
			count:    5,
			expected: false,
		},
		{
			name:     "unlimited quota accepts any positive batch",
			quota:    Quota{},
			count:    1000,
			expected: true,
		},
		{
			name:     "zero batch never fits",
			quota:    Quota{Concurrency: 4},
			count:    0,
			expected: false,
		},
		{
			name:     "negative batch never fits",
			quota:    Quota{Concurrency: 4},
			count:    -1,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.quota.Fits(tt.count); got != tt.expected {
				t.Fatalf("expected %v, got %v", tt.expected, got)
			}
		})
	}
}
