package application

import (
	"math/rand/v2"
	"time"
)

// RetryDelay computes the delay before the given retry (1-based) using
// exponential backoff with equal jitter.
//
// The exponential delay is baseDelay × 2^(retry-1), capped at maxDelay.
// Equal jitter keeps half of that delay fixed and randomizes the other
// half, so the result falls in [delay/2, delay).
func RetryDelay(
	retry int,
	baseDelay time.Duration,
	maxDelay time.Duration,
) time.Duration {
	if retry <= 0 {
		return baseDelay
	}

	if baseDelay <= 0 {
		return 0
	}

	delay := baseDelay

	for i := 1; i < retry; i++ {
		// Doubling past this point would reach or exceed maxDelay
		// (or overflow), so jump straight to the cap.
		if delay >= maxDelay/2 {
			delay = maxDelay
			break
		}

		delay *= 2
	}

	if delay > maxDelay {
		delay = maxDelay
	}

	half := delay / 2
	if half <= 0 {
		return delay
	}

	return half + rand.N(half)
}
