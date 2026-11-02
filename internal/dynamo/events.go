package dynamo

const (
	eventPollCreated = "POLL_CREATED"
	eventPollClosed  = "POLL_CLOSED"

	pendingIndex = "pending-index"
	// pendingValue marks an undelivered event. Every pending event shares it,
	// so pendingIndex lists them all under one key.
	pendingValue = "1"
)

// eventItem is an outbox item, following the schema in ADR 0002. The worker's
// lease attributes come with #14.
type eventItem struct {
	EventID    string       `dynamodbav:"eventId"`
	Type       string       `dynamodbav:"type"`
	PollID     string       `dynamodbav:"pollId"`
	OccurredAt string       `dynamodbav:"occurredAt"`
	Payload    eventPayload `dynamodbav:"payload"`
	Pending    string       `dynamodbav:"pending,omitempty"`
}

// eventPayload is the snapshot an email needs, taken when the event is
// written. Counts is set only for POLL_CLOSED.
type eventPayload struct {
	Question   string   `dynamodbav:"question"`
	Options    []string `dynamodbav:"options"`
	Recipients []string `dynamodbav:"recipients"`
	Counts     []int    `dynamodbav:"counts,omitempty"`
}
