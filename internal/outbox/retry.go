package outbox

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

// ErrPermanent marks a send error that retrying won't fix, such as a rejected
// address. A Sender wraps it together with the cause:
// fmt.Errorf("%w: %w", ErrPermanent, err).
var ErrPermanent = errors.New("permanent send failure")

// RetryConfig tunes a RetrySender. Every field must be set.
type RetryConfig struct {
	// MaxAttempts is the most times one message is tried, counting the first.
	MaxAttempts int
	// BaseDelay caps the wait before the first retry. The cap doubles with
	// each retry, up to MaxDelay.
	BaseDelay time.Duration
	// MaxDelay is the most any one wait can be. Keep the total of the waits
	// well under the worker's lease.
	MaxDelay time.Duration
}

// RetrySender is a Sender that retries another Sender's transient failures,
// with exponential backoff and full jitter.
type RetrySender struct {
	next   Sender
	cfg    RetryConfig
	jitter func(time.Duration) time.Duration
}

// NewRetrySender returns a RetrySender that sends with next.
func NewRetrySender(next Sender, cfg RetryConfig) *RetrySender {
	return &RetrySender{next: next, cfg: cfg, jitter: fullJitter}
}

// Send sends m with the wrapped Sender. Before retry n (counting from 1) it
// waits jitter(min(MaxDelay, BaseDelay·2ⁿ⁻¹)), up to MaxAttempts tries in all,
// and then returns the last error. It returns at once on an error that wraps
// ErrPermanent, and with an error wrapping ctx.Err() if ctx is done while it
// waits.
func (s *RetrySender) Send(ctx context.Context, m Message) error {
	delay := s.cfg.BaseDelay

	var err error
	for i := range s.cfg.MaxAttempts {
		if err = s.next.Send(ctx, m); err == nil {
			return nil
		}

		if errors.Is(err, ErrPermanent) {
			return err
		}

		if i < s.cfg.MaxAttempts-1 {
			select {
			case <-time.After(s.jitter(delay)):
			case <-ctx.Done():
				return fmt.Errorf("%w after: %w", ctx.Err(), err)
			}

			delay = min(delay*2, s.cfg.MaxDelay)
		}
	}

	return err
}

// fullJitter returns a random duration from 0 to d, inclusive.
func fullJitter(d time.Duration) time.Duration {
	return rand.N(d + 1)
}
