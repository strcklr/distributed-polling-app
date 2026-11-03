package outbox

// Messages returns one message per recipient of e, in the order of
// e.Recipients: an invitation to vote for EventPollCreated, and the results
// for EventPollClosed.
//
// An invitation's subject is "Vote: <question>". Its body has the question,
// each option with the index to vote with, and how to vote on poll e.PollID.
//
// A results subject is "Results: <question>". Its body has a
// "<option>: <count>" line for each option, then "Winner: <option>", or
// "Tie: <option>, <option>" when options share the highest count.
//
// It returns an error for an unknown event type, or for a closed event whose
// counts don't match its options.
func Messages(e Event) ([]Message, error) {
	// TODO(#14): render the subject and body once with text/template (parsed at
	// package level with template.Must), then copy them into a message per
	// recipient.
	return nil, errNotImplemented
}
