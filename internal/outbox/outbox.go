// Package outbox delivers poll events from the outbox to their recipients,
// following docs/adr/0002-outbox.md.
package outbox

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// EventType is the kind of state change an event records.
type EventType string

const (
	EventPollCreated EventType = "POLL_CREATED"
	EventPollClosed  EventType = "POLL_CLOSED"
)

var (
	// ErrNotAvailable means an event can't be claimed: another worker holds a
	// live lease on it, or it's already delivered or doesn't exist.
	ErrNotAvailable = errors.New("event not available")
	// ErrLeaseLost means the worker completing an event doesn't hold its claim.
	ErrLeaseLost = errors.New("lease lost")
)

var errNotImplemented = errors.New("not implemented")

// Event is an undelivered outbox event, with the snapshot its emails need.
// Counts is set only for EventPollClosed.
type Event struct {
	ID         string
	Type       EventType
	PollID     string
	Question   string
	Options    []string
	Recipients []string
	Counts     []int
}

// Message is one email to one recipient. EventID and To together identify
// it, so a redelivered event produces the same messages.
type Message struct {
	EventID string
	To      string
	Subject string
	Body    string
}

// Source is where a Worker reads events and records their delivery.
type Source interface {
	// Pending returns up to limit undelivered events, oldest first. It can
	// include events another worker has claimed.
	Pending(ctx context.Context, limit int) ([]Event, error)
	// Claim leases the event to owner for the given duration, or returns
	// ErrNotAvailable.
	Claim(ctx context.Context, eventID, owner string, lease time.Duration) error
	// Complete marks an event that owner claimed as delivered, or returns
	// ErrLeaseLost if owner doesn't hold the claim.
	Complete(ctx context.Context, eventID, owner string) error
}

// Sender delivers one message.
type Sender interface {
	Send(ctx context.Context, m Message) error
}

// LogSender is a Sender that logs each message instead of sending it.
type LogSender struct {
	Logger *slog.Logger
}

// Send logs m.
func (s LogSender) Send(ctx context.Context, m Message) error {
	s.Logger.InfoContext(ctx, "sending email", "eventID", m.EventID, "to", m.To, "subject", m.Subject, "body", m.Body)
	return nil
}
