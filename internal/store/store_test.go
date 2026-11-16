package store

import (
	"testing"
	"time"
)

func TestRetryDelayBounds(t *testing.T) {
	for _, tc := range []struct {
		attempt int32
		ceiling time.Duration
	}{{1, time.Second}, {2, 2 * time.Second}, {6, 32 * time.Second}, {7, 60 * time.Second}, {101, 60 * time.Second}} {
		seen := map[time.Duration]bool{}
		for range 100 {
			delay := RetryDelay(tc.attempt)
			if delay < tc.ceiling/2 || delay > tc.ceiling {
				t.Fatalf("attempt %d delay %s", tc.attempt, delay)
			}
			seen[delay] = true
		}
		if len(seen) < 2 {
			t.Fatal("jitter did not vary")
		}
	}
}
