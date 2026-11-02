package dynamo

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/strcklr/distributed-polling-app/internal/outbox"
)

const testLease = 2 * time.Minute

func TestOutboxPendingOldestFirst(t *testing.T) {
	s, o, clock := newOutbox(t)
	putEvent(t, s, pendingEvent("c", clock.now.Add(2*time.Minute)))
	putEvent(t, s, pendingEvent("a", clock.now))
	putEvent(t, s, pendingEvent("b", clock.now.Add(time.Minute)))

	assertPending(t, o, 10, "a#created", "b#created", "c#created")
	assertPending(t, o, 2, "a#created", "b#created")
}

func TestOutboxPendingSkipsDelivered(t *testing.T) {
	s, o, clock := newOutbox(t)
	delivered := pendingEvent("a", clock.now)
	delivered.Pending = ""
	putEvent(t, s, delivered)
	putEvent(t, s, pendingEvent("b", clock.now))

	assertPending(t, o, 10, "b#created")
}

func TestOutboxPendingReturnsEvents(t *testing.T) {
	s, o, _ := newOutbox(t)
	p := createPoll(t, s, "alice@example.com")
	castVotes(t, s, p.ID, map[string]int{"alice@example.com": 1})

	got, err := o.Pending(t.Context(), 10)
	if err != nil {
		t.Fatalf("Pending() error = %v", err)
	}
	want := map[string]outbox.Event{
		p.ID + "#created": {
			ID: p.ID + "#created", Type: outbox.EventPollCreated, PollID: p.ID,
			Question: p.Question, Options: p.Options, Recipients: p.Participants,
		},
		p.ID + "#closed": {
			ID: p.ID + "#closed", Type: outbox.EventPollClosed, PollID: p.ID,
			Question: p.Question, Options: p.Options, Recipients: p.Participants, Counts: []int{0, 1},
		},
	}
	if len(got) != len(want) {
		t.Fatalf("Pending() returned %d events, want %d: %+v", len(got), len(want), got)
	}
	for _, e := range got {
		if !reflect.DeepEqual(e, want[e.ID]) {
			t.Errorf("event = %+v\nwant    %+v", e, want[e.ID])
		}
	}
}

func TestOutboxClaim(t *testing.T) {
	s, o, clock := newOutbox(t)
	ev := pendingEvent("a", clock.now)
	putEvent(t, s, ev)

	if err := o.Claim(t.Context(), ev.EventID, "worker-1", testLease); err != nil {
		t.Fatalf("first Claim() error = %v", err)
	}
	stored := storedEvent(t, s, ev.EventID)
	if want := clock.now.Add(testLease).UnixMilli(); stored.LeaseUntil != want || stored.ClaimedBy != "worker-1" {
		t.Errorf("lease = (%d, %q), want (%d, %q)", stored.LeaseUntil, stored.ClaimedBy, want, "worker-1")
	}

	for _, owner := range []string{"worker-1", "worker-2"} {
		if err := o.Claim(t.Context(), ev.EventID, owner, testLease); !errors.Is(err, outbox.ErrNotAvailable) {
			t.Errorf("Claim(%s) during the lease error = %v, want %v", owner, err, outbox.ErrNotAvailable)
		}
	}

	clock.now = clock.now.Add(testLease)
	if err := o.Claim(t.Context(), ev.EventID, "worker-2", testLease); !errors.Is(err, outbox.ErrNotAvailable) {
		t.Errorf("Claim() as the lease ends error = %v, want %v", err, outbox.ErrNotAvailable)
	}

	clock.now = clock.now.Add(time.Millisecond)
	if err := o.Claim(t.Context(), ev.EventID, "worker-2", testLease); err != nil {
		t.Fatalf("Claim() after the lease error = %v", err)
	}
	if got := storedEvent(t, s, ev.EventID).ClaimedBy; got != "worker-2" {
		t.Errorf("claimedBy = %q, want %q", got, "worker-2")
	}
}

func TestOutboxClaimNotPending(t *testing.T) {
	s, o, clock := newOutbox(t)
	delivered := pendingEvent("a", clock.now)
	delivered.Pending = ""
	putEvent(t, s, delivered)

	for _, id := range []string{delivered.EventID, "missing#created"} {
		if err := o.Claim(t.Context(), id, "worker-1", testLease); !errors.Is(err, outbox.ErrNotAvailable) {
			t.Errorf("Claim(%s) error = %v, want %v", id, err, outbox.ErrNotAvailable)
		}
	}
}

func TestOutboxConcurrentClaims(t *testing.T) {
	s, o, clock := newOutbox(t)
	ev := pendingEvent("a", clock.now)
	putEvent(t, s, ev)

	var mu sync.Mutex
	var winners []string
	var wg sync.WaitGroup
	for i := range 10 {
		owner := fmt.Sprintf("worker-%d", i)
		wg.Go(func() {
			err := o.Claim(t.Context(), ev.EventID, owner, testLease)
			switch {
			case err == nil:
				mu.Lock()
				winners = append(winners, owner)
				mu.Unlock()
			case !errors.Is(err, outbox.ErrNotAvailable):
				t.Errorf("Claim(%s) error = %v", owner, err)
			}
		})
	}
	wg.Wait()

	if len(winners) != 1 {
		t.Fatalf("claims won by %v, want exactly one", winners)
	}
	if got := storedEvent(t, s, ev.EventID).ClaimedBy; got != winners[0] {
		t.Errorf("claimedBy = %q, want the winner %q", got, winners[0])
	}
}

func TestOutboxComplete(t *testing.T) {
	s, o, clock := newOutbox(t)
	ev := pendingEvent("a", clock.now)
	putEvent(t, s, ev)
	if err := o.Claim(t.Context(), ev.EventID, "worker-1", testLease); err != nil {
		t.Fatalf("Claim() error = %v", err)
	}

	if err := o.Complete(t.Context(), ev.EventID, "worker-1"); err != nil {
		t.Fatalf("Complete() error = %v", err)
	}

	stored := storedEvent(t, s, ev.EventID)
	if stored.Pending != "" || stored.LeaseUntil != 0 || stored.ClaimedBy != "" {
		t.Errorf("after Complete() pending, leaseUntil, claimedBy = (%q, %d, %q), want all removed",
			stored.Pending, stored.LeaseUntil, stored.ClaimedBy)
	}
	assertPending(t, o, 10)
}

func TestOutboxCompleteWithoutClaim(t *testing.T) {
	s, o, clock := newOutbox(t)
	stolen := pendingEvent("a", clock.now)
	unclaimed := pendingEvent("b", clock.now.Add(time.Second))
	putEvent(t, s, stolen)
	putEvent(t, s, unclaimed)

	if err := o.Claim(t.Context(), stolen.EventID, "worker-1", testLease); err != nil {
		t.Fatalf("Claim(worker-1) error = %v", err)
	}
	clock.now = clock.now.Add(testLease + time.Millisecond)
	if err := o.Claim(t.Context(), stolen.EventID, "worker-2", testLease); err != nil {
		t.Fatalf("Claim(worker-2) error = %v", err)
	}

	for _, id := range []string{stolen.EventID, unclaimed.EventID, "missing#created"} {
		if err := o.Complete(t.Context(), id, "worker-1"); !errors.Is(err, outbox.ErrLeaseLost) {
			t.Errorf("Complete(%s) error = %v, want %v", id, err, outbox.ErrLeaseLost)
		}
	}
	if got := storedEvent(t, s, stolen.EventID).ClaimedBy; got != "worker-2" {
		t.Errorf("claimedBy = %q, want %q", got, "worker-2")
	}
	assertPending(t, o, 10, stolen.EventID, unclaimed.EventID)
}

type testClock struct {
	now time.Time
}

func (c *testClock) Now() time.Time {
	return c.now
}

// newOutbox returns a store and an Outbox on the same tables. The Outbox
// reads the time from the returned clock, which tests move forward by hand.
func newOutbox(t *testing.T) (*Store, *Outbox, *testClock) {
	t.Helper()
	s := newStore(t)
	clock := &testClock{now: time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)}
	o := NewOutbox(s.client, s.outbox)
	o.now = clock.Now
	return s, o, clock
}

func pendingEvent(pollID string, occurredAt time.Time) eventItem {
	return eventItem{
		EventID:    pollID + "#created",
		Type:       eventPollCreated,
		PollID:     pollID,
		OccurredAt: occurredAt.UTC().Format(time.RFC3339),
		Payload: eventPayload{
			Question:   "Lunch?",
			Options:    []string{"Tacos", "Pizza"},
			Recipients: []string{"alice@example.com"},
		},
		Pending: pendingValue,
	}
}

// putEvent writes ev to the outbox table directly, so tests control its
// occurredAt and pending.
func putEvent(t *testing.T, s *Store, ev eventItem) {
	t.Helper()
	av, err := attributevalue.MarshalMap(ev)
	if err != nil {
		t.Fatalf("marshal %s: %v", ev.EventID, err)
	}
	if _, err := s.client.PutItem(t.Context(), &dynamodb.PutItemInput{TableName: aws.String(s.outbox), Item: av}); err != nil {
		t.Fatalf("PutItem(%s) error = %v", ev.EventID, err)
	}
}

func assertPending(t *testing.T, o *Outbox, limit int, want ...string) {
	t.Helper()
	events, err := o.Pending(t.Context(), limit)
	if err != nil {
		t.Fatalf("Pending(%d) error = %v", limit, err)
	}
	var got []string
	for _, e := range events {
		got = append(got, e.ID)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Pending(%d) = %v, want %v", limit, got, want)
	}
}
