package dynamo

import "github.com/strcklr/distributed-polling-app/internal/outbox"

const (
	eventPollCreated = string(outbox.EventPollCreated)
	eventPollClosed  = string(outbox.EventPollClosed)

	pendingIndex = "pending-index"
	// pendingValue marks an undelivered event. Every pending event shares it,
	// so pendingIndex lists them all under one key.
	pendingValue = "1"
)

// eventItem is an outbox item, following the schema in ADR 0002.
type eventItem struct {
	EventID    string       `dynamodbav:"eventId"`
	Type       string       `dynamodbav:"type"`
	PollID     string       `dynamodbav:"pollId"`
	OccurredAt string       `dynamodbav:"occurredAt"`
	Payload    eventPayload `dynamodbav:"payload"`
	Pending    string       `dynamodbav:"pending,omitempty"`
	// LeaseUntil is when the current claim expires, in Unix milliseconds.
	LeaseUntil int64  `dynamodbav:"leaseUntil,omitempty"`
	ClaimedBy  string `dynamodbav:"claimedBy,omitempty"`
}

// eventPayload is the snapshot an email needs, taken when the event is
// written. Counts is set only for POLL_CLOSED.
type eventPayload struct {
	Question   string   `dynamodbav:"question"`
	Options    []string `dynamodbav:"options"`
	Recipients []string `dynamodbav:"recipients"`
	Counts     []int    `dynamodbav:"counts,omitempty"`
}
