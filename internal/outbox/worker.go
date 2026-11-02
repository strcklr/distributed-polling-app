package outbox

import (
	"context"
	"log/slog"
	"time"
)

// Config tunes a Worker. Every field must be set.
type Config struct {
	// ID identifies the worker in its claims. It must be unique among the
	// running workers.
	ID string
	// PollInterval is how long Run waits between ticks.
	PollInterval time.Duration
	// Lease is how long a claim lasts. It must be much longer than delivering
	// one event takes.
	Lease time.Duration
	// BatchSize is the most events one tick reads.
	BatchSize int
	// Events is the most events delivered at once.
	Events int
	// Sends is the most messages sent at once for one event.
	Sends int
}

// Worker delivers outbox events: it claims each pending event, sends its
// messages, and completes the event once every message is sent.
type Worker struct {
	source Source
	sender Sender
	logger *slog.Logger
	cfg    Config
}

// New returns a Worker that reads events from source and sends their
// messages with sender.
func New(source Source, sender Sender, logger *slog.Logger, cfg Config) *Worker {
	return &Worker{source: source, sender: sender, logger: logger, cfg: cfg}
}

// Run calls Tick right away and then every PollInterval until ctx is
// canceled, and then returns nil. It logs Tick's errors and keeps going.
func (w *Worker) Run(ctx context.Context) error {
	return errNotImplemented
}

// Tick reads up to BatchSize pending events and hands them through a channel
// to Events goroutines. It returns once every event is handled, with an error
// only if reading the events fails.
//
// For each event, Tick claims it, skipping it on ErrNotAvailable, and sends
// its messages, Sends at a time. It completes the event only when every
// message was sent, so after a failed send the lease expires and a later
// tick delivers the event again. A failure on one event is logged and doesn't
// stop the others.
func (w *Worker) Tick(ctx context.Context) error {
	return errNotImplemented
}
