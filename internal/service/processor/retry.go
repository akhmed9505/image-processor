package processor

import (
	"math/rand/v2"
	"time"
)

// RetryDelay returns jittered delays for attempts 1–4; zero means no attempts remain.
// It can also be passed to the recovery repository's retry-delay callback.
func RetryDelay(attempts int) time.Duration {
	if attempts < 1 || attempts >= MaxAttempts {
		return 0
	}
	delays := [...]time.Duration{5 * time.Second, 15 * time.Second, 45 * time.Second, 135 * time.Second}
	base := delays[attempts-1]
	spread := base / 5
	return base - spread + time.Duration(rand.Int64N(int64(2*spread)+1))
}
