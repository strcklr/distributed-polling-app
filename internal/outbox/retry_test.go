package outbox

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"testing/synctest"
	"time"
)

var errTransient = errors.New("421 try again later")

var testRetry = RetryConfig{
	MaxAttempts: 4,
	BaseDelay:   100 * time.Millisecond,
	MaxDelay:    time.Second,
}

// The retry tests run in a synctest bubble, so the waits between tries take
// no real time and can be checked exactly.

func TestRetrySenderSucceedsFirstTry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		next := &flakySender{}

		if err := newRetrySender(next, testRetry).Send(t.Context(), testMessage); err != nil {
			t.Fatalf("Send() error = %v", err)
		}
		if got := len(next.calls); got != 1 {
			t.Errorf("tries = %d, want 1", got)
		}
	})
}

func TestRetrySenderRetriesTransientErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		next := &flakySender{errs: []error{errTransient, errTransient}}

		if err := newRetrySender(next, testRetry).Send(t.Context(), testMessage); err != nil {
			t.Fatalf("Send() error = %v", err)
		}
		assertWaits(t, next, 100*time.Millisecond, 200*time.Millisecond)
		for i, m := range next.messages {
			if m != testMessage {
				t.Errorf("try %d sent %+v, want %+v", i+1, m, testMessage)
			}
		}
	})
}

func TestRetrySenderGivesUpAfterMaxAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		next := &flakySender{always: errTransient}

		err := newRetrySender(next, testRetry).Send(t.Context(), testMessage)
		if !errors.Is(err, errTransient) {
			t.Fatalf("Send() error = %v, want %v", err, errTransient)
		}
		assertWaits(t, next, 100*time.Millisecond, 200*time.Millisecond, 400*time.Millisecond)
	})
}

func TestRetrySenderCapsDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		next := &flakySender{always: errTransient}
		cfg := RetryConfig{MaxAttempts: 5, BaseDelay: 100 * time.Millisecond, MaxDelay: 250 * time.Millisecond}

		newRetrySender(next, cfg).Send(t.Context(), testMessage)

		assertWaits(t, next, 100*time.Millisecond, 200*time.Millisecond, 250*time.Millisecond, 250*time.Millisecond)
	})
}

func TestRetrySenderAppliesJitter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		next := &flakySender{always: errTransient}
		var caps []time.Duration
		s := NewRetrySender(next, testRetry)
		s.jitter = func(d time.Duration) time.Duration {
			caps = append(caps, d)
			return d / 2
		}

		s.Send(t.Context(), testMessage)

		want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}
		if !slices.Equal(caps, want) {
			t.Errorf("jitter called with %v, want %v", caps, want)
		}
		assertWaits(t, next, 50*time.Millisecond, 100*time.Millisecond, 200*time.Millisecond)
	})
}

func TestRetrySenderStopsOnPermanentError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cause := errors.New("550 no such mailbox")
		next := &flakySender{always: fmt.Errorf("%w: %w", ErrPermanent, cause)}

		err := newRetrySender(next, testRetry).Send(t.Context(), testMessage)
		if !errors.Is(err, ErrPermanent) || !errors.Is(err, cause) {
			t.Fatalf("Send() error = %v, want it to wrap ErrPermanent and %v", err, cause)
		}
		if got := len(next.calls); got != 1 {
			t.Errorf("tries = %d, want 1", got)
		}
	})
}

func TestRetrySenderStopsWhenCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		next := &flakySender{always: errTransient}
		timer := time.AfterFunc(30*time.Millisecond, cancel)
		defer timer.Stop()
		start := time.Now()

		err := newRetrySender(next, testRetry).Send(ctx, testMessage)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Send() error = %v, want %v", err, context.Canceled)
		}
		if got := time.Since(start); got != 30*time.Millisecond {
			t.Errorf("Send returned after %v, want 30ms, when ctx was canceled", got)
		}
		if got := len(next.calls); got != 1 {
			t.Errorf("tries = %d, want 1", got)
		}
	})
}

func TestFullJitter(t *testing.T) {
	const d = 100 * time.Millisecond
	lo, hi := d, time.Duration(0)
	for range 1000 {
		got := fullJitter(d)
		if got < 0 || got > d {
			t.Fatalf("fullJitter(%v) = %v, want 0 to %v", d, got, d)
		}
		lo, hi = min(lo, got), max(hi, got)
	}
	if lo > d/4 || hi < 3*d/4 {
		t.Errorf("1000 samples ranged %v to %v, want them spread over 0 to %v", lo, hi, d)
	}
	if got := fullJitter(0); got != 0 {
		t.Errorf("fullJitter(0) = %v, want 0", got)
	}
}

var testMessage = Message{EventID: "p1#created", To: "alice@example.com", Subject: "Vote", Body: "Lunch?"}

// newRetrySender returns a RetrySender without jitter, so each wait is the
// full backoff.
func newRetrySender(next Sender, cfg RetryConfig) *RetrySender {
	s := NewRetrySender(next, cfg)
	s.jitter = func(d time.Duration) time.Duration { return d }
	return s
}

// flakySender returns errs in order, then always (nil by default). It records
// when each try happened.
type flakySender struct {
	errs   []error
	always error

	calls    []time.Time
	messages []Message
}

func (s *flakySender) Send(ctx context.Context, m Message) error {
	s.calls = append(s.calls, time.Now())
	s.messages = append(s.messages, m)
	if len(s.errs) > 0 {
		err := s.errs[0]
		s.errs = s.errs[1:]
		return err
	}
	return s.always
}

func assertWaits(t *testing.T, s *flakySender, want ...time.Duration) {
	t.Helper()
	var got []time.Duration
	for i := 1; i < len(s.calls); i++ {
		got = append(got, s.calls[i].Sub(s.calls[i-1]))
	}
	if !slices.Equal(got, want) {
		t.Errorf("waits between tries = %v, want %v", got, want)
	}
}
