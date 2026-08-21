package application

import (
	"testing"
	"time"
)

func TestRetryDelayNonPositiveRetry(t *testing.T) {
	base := 1 * time.Second
	max := 30 * time.Second

	for _, retry := range []int{0, -1, -10} {
		got := RetryDelay(retry, base, max)

		if got != base {
			t.Errorf(
				"RetryDelay(%d) = %s, want %s",
				retry,
				got,
				base,
			)
		}
	}
}

func TestRetryDelayEqualJitterRange(t *testing.T) {
	base := 1 * time.Second
	max := 30 * time.Second

	cases := []struct {
		retry int
		min   time.Duration
		limit time.Duration
	}{
		{retry: 1, min: 500 * time.Millisecond, limit: 1 * time.Second},
		{retry: 2, min: 1 * time.Second, limit: 2 * time.Second},
		{retry: 3, min: 2 * time.Second, limit: 4 * time.Second},
		{retry: 4, min: 4 * time.Second, limit: 8 * time.Second},
		{retry: 5, min: 8 * time.Second, limit: 16 * time.Second},
	}

	for _, tc := range cases {
		for i := 0; i < 200; i++ {
			got := RetryDelay(tc.retry, base, max)

			if got < tc.min || got >= tc.limit {
				t.Fatalf(
					"RetryDelay(%d) = %s, want in [%s, %s)",
					tc.retry,
					got,
					tc.min,
					tc.limit,
				)
			}
		}
	}
}

func TestRetryDelayMaxCap(t *testing.T) {
	base := 1 * time.Second
	max := 30 * time.Second

	for _, retry := range []int{6, 10, 100, 1_000_000} {
		for i := 0; i < 200; i++ {
			got := RetryDelay(retry, base, max)

			if got > max {
				t.Fatalf(
					"RetryDelay(%d) = %s, exceeds max %s",
					retry,
					got,
					max,
				)
			}

			if got < max/2 {
				t.Fatalf(
					"RetryDelay(%d) = %s, want at least %s",
					retry,
					got,
					max/2,
				)
			}
		}
	}
}

func TestRetryDelayTinyDelaysAreSafe(t *testing.T) {
	max := 30 * time.Second

	for _, base := range []time.Duration{0, 1 * time.Nanosecond, 3 * time.Nanosecond} {
		for retry := 1; retry <= 5; retry++ {
			got := RetryDelay(retry, base, max)

			if got < 0 || got > max {
				t.Errorf(
					"RetryDelay(%d) with base %s = %s, want in [0, %s]",
					retry,
					base,
					got,
					max,
				)
			}
		}
	}
}

func TestRetryDelayJitterVaries(t *testing.T) {
	base := 1 * time.Second
	max := 30 * time.Second

	seen := make(map[time.Duration]struct{})

	for i := 0; i < 100; i++ {
		seen[RetryDelay(4, base, max)] = struct{}{}
	}

	// 100 draws from a 4-second nanosecond range collide entirely
	// with negligible probability, so this cannot flake.
	if len(seen) < 2 {
		t.Errorf(
			"expected jitter to vary, got %d distinct value(s)",
			len(seen),
		)
	}
}
