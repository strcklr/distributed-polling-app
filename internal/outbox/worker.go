package outbox

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
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
	ticker := time.NewTicker(w.cfg.PollInterval)

	defer ticker.Stop()

	for {
		if err := w.Tick(ctx); err != nil {
			w.logger.Error("run tick", "err", err)
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return nil
		}
	}
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
	events, err := w.source.Pending(ctx, w.cfg.BatchSize)
	if err != nil {
		return err
	}

	ch := make(chan Event)
	var wg sync.WaitGroup
	for range w.cfg.Events {
		wg.Go(func() {
			for e := range ch {
				w.deliver(ctx, e)
			}
		})
	}

	for _, e := range events {
		ch <- e
	}

	close(ch)
	wg.Wait()

	return nil
}

func (w *Worker) deliver(ctx context.Context, e Event) {
	log := w.logger.With("eventID", e.ID, "pollID", e.PollID)
	if err := w.source.Claim(ctx, e.ID, w.cfg.ID, w.cfg.Lease); err != nil {
		if errors.Is(err, ErrNotAvailable) {
			log.Debug("event already claimed", "err", err)
			return
		}
		log.Error("error claiming event", "err", err)
		return
	}

	msgs, err := Messages(e)
	if err != nil {
		log.Error("create messages", "err", err)
		return
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(w.cfg.Sends)
	for _, m := range msgs {
		g.Go(func() error { return w.sender.Send(gctx, m) })
	}

	if err := g.Wait(); err != nil {
		log.Error("deliver message", "err", err)
		return
	}

	if err := w.source.Complete(ctx, e.ID, w.cfg.ID); err != nil {
		if errors.Is(err, ErrLeaseLost) {
			log.Warn("complete message, lost lease", "err", err)
		} else {
			log.Error("complete message", "err", err)
		}
	}
}
