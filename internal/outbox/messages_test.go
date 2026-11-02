package outbox

import (
	"strings"
	"testing"
)

func TestMessagesInvitation(t *testing.T) {
	e := Event{
		ID:         "p1#created",
		Type:       EventPollCreated,
		PollID:     "p1",
		Question:   "Lunch?",
		Options:    []string{"Tacos", "Pizza"},
		Recipients: []string{"alice@example.com", "bob@example.com"},
	}

	msgs, err := Messages(e)
	if err != nil {
		t.Fatalf("Messages() error = %v", err)
	}
	assertRecipients(t, msgs, e)
	for _, m := range msgs {
		if m.Subject != "Vote: Lunch?" {
			t.Errorf("subject = %q, want %q", m.Subject, "Vote: Lunch?")
		}
		assertBodyContains(t, m, "Lunch?", "p1", "Tacos", "Pizza")
	}
}

func TestMessagesResults(t *testing.T) {
	tests := []struct {
		name    string
		options []string
		counts  []int
		want    []string
		notWant string
	}{
		{
			name:    "winner",
			options: []string{"Tacos", "Pizza"},
			counts:  []int{1, 2},
			want:    []string{"Tacos: 1", "Pizza: 2", "Winner: Pizza"},
			notWant: "Tie:",
		},
		{
			name:    "tie",
			options: []string{"Tacos", "Pizza", "Sushi"},
			counts:  []int{2, 1, 2},
			want:    []string{"Tacos: 2", "Pizza: 1", "Sushi: 2", "Tie: Tacos, Sushi"},
			notWant: "Winner:",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := Event{
				ID:         "p1#closed",
				Type:       EventPollClosed,
				PollID:     "p1",
				Question:   "Lunch?",
				Options:    tt.options,
				Recipients: []string{"alice@example.com", "bob@example.com"},
				Counts:     tt.counts,
			}

			msgs, err := Messages(e)
			if err != nil {
				t.Fatalf("Messages() error = %v", err)
			}
			assertRecipients(t, msgs, e)
			for _, m := range msgs {
				if m.Subject != "Results: Lunch?" {
					t.Errorf("subject = %q, want %q", m.Subject, "Results: Lunch?")
				}
				assertBodyContains(t, m, tt.want...)
				if strings.Contains(m.Body, tt.notWant) {
					t.Errorf("body contains %q:\n%s", tt.notWant, m.Body)
				}
			}
		})
	}
}

func TestMessagesInvalidEvent(t *testing.T) {
	tests := []struct {
		name   string
		typ    EventType
		counts []int
	}{
		{name: "unknown type", typ: "POLL_RENAMED"},
		{name: "closed without counts", typ: EventPollClosed},
		{name: "closed with too few counts", typ: EventPollClosed, counts: []int{1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := Event{
				ID:         "p1#event",
				Type:       tt.typ,
				PollID:     "p1",
				Question:   "Lunch?",
				Options:    []string{"Tacos", "Pizza"},
				Recipients: []string{"alice@example.com"},
				Counts:     tt.counts,
			}

			msgs, err := Messages(e)
			if err == nil {
				t.Fatal("Messages() error = nil, want an error")
			}
			if msgs != nil {
				t.Errorf("Messages() = %v, want nil with an error", msgs)
			}
		})
	}
}

func assertRecipients(t *testing.T, msgs []Message, e Event) {
	t.Helper()
	if len(msgs) != len(e.Recipients) {
		t.Fatalf("got %d messages, want %d", len(msgs), len(e.Recipients))
	}
	for i, m := range msgs {
		if m.To != e.Recipients[i] || m.EventID != e.ID {
			t.Errorf("message %d is (%s, %s), want (%s, %s)", i, m.EventID, m.To, e.ID, e.Recipients[i])
		}
	}
}

func assertBodyContains(t *testing.T, m Message, want ...string) {
	t.Helper()
	for _, w := range want {
		if !strings.Contains(m.Body, w) {
			t.Errorf("body for %s doesn't contain %q:\n%s", m.To, w, m.Body)
		}
	}
}
