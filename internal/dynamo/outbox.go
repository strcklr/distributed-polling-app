package dynamo

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/strcklr/distributed-polling-app/internal/outbox"
)

var _ outbox.Source = (*Outbox)(nil)

var errNotImplemented = errors.New("not implemented")

// Outbox is an outbox.Source backed by the outbox table. A claim is a lease
// recorded on the event as leaseUntil and claimedBy, as in ADR 0002.
type Outbox struct {
	client *dynamodb.Client
	table  string
	now    func() time.Time
}

// NewOutbox returns an Outbox that reads and claims events in table.
func NewOutbox(client *dynamodb.Client, table string) *Outbox {
	return &Outbox{client: client, table: table, now: time.Now}
}

// Pending queries pendingIndex for up to limit events, oldest first.
func (o *Outbox) Pending(ctx context.Context, limit int) ([]outbox.Event, error) {
	// TODO(#14): Query pendingIndex on pending = pendingValue with Limit, and
	// map each eventItem to an outbox.Event.
	return nil, errNotImplemented
}

// Claim sets leaseUntil and claimedBy if the event is pending and has no
// lease, or its lease has expired.
func (o *Outbox) Claim(ctx context.Context, eventID, owner string, lease time.Duration) error {
	// TODO(#14): conditional UpdateItem from ADR 0002's claim expression, with
	// times from o.now() in Unix milliseconds. A failed condition is
	// outbox.ErrNotAvailable.
	return errNotImplemented
}

// Complete removes pending, leaseUntil and claimedBy if owner holds the
// claim, which takes the event out of pendingIndex. The event stays in the
// table as a record.
func (o *Outbox) Complete(ctx context.Context, eventID, owner string) error {
	// TODO(#14): REMOVE pending, leaseUntil, claimedBy on condition
	// claimedBy = :me. A failed condition is outbox.ErrLeaseLost.
	return errNotImplemented
}
