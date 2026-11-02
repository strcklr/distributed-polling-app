package dynamo

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/strcklr/distributed-polling-app/internal/poll"
)

func TestCreateWritesCreatedEvent(t *testing.T) {
	s := newStore(t)
	before := time.Now().UTC().Add(-time.Second)
	p := createPoll(t, s, "alice@example.com", "bob@example.com")

	ev := storedEvent(t, s, p.ID+"#created")
	want := eventItem{
		EventID: p.ID + "#created",
		Type:    eventPollCreated,
		PollID:  p.ID,
		Payload: eventPayload{Question: p.Question, Options: p.Options, Recipients: p.Participants},
		Pending: pendingValue,
	}
	assertEvent(t, ev, want, before)
	assertEventIDs(t, s, p.ID+"#created")
}

func TestCreateDuplicateWritesNoEvent(t *testing.T) {
	s := newStore(t)
	p := createPoll(t, s, "alice@example.com")
	duplicate, err := poll.New("Dinner?", []string{"Soup", "Salad"}, []string{"bob@example.com"})
	if err != nil {
		t.Fatalf("poll.New() error = %v", err)
	}
	duplicate.ID = p.ID

	if err := s.Create(t.Context(), duplicate); !errors.Is(err, poll.ErrAlreadyExists) {
		t.Fatalf("Create() error = %v, want %v", err, poll.ErrAlreadyExists)
	}

	if got := storedEvent(t, s, p.ID+"#created").Payload.Question; got != p.Question {
		t.Errorf("event question = %q, want the original %q", got, p.Question)
	}
	assertEventIDs(t, s, p.ID+"#created")
}

func TestVoteWritesNoEvent(t *testing.T) {
	s := newStore(t)
	p := createPoll(t, s, "alice@example.com", "bob@example.com")

	if _, err := s.CastVote(t.Context(), p.ID, "alice@example.com", 0); err != nil {
		t.Fatalf("CastVote() error = %v", err)
	}

	assertEventIDs(t, s, p.ID+"#created")
}

func TestLastVoteWritesClosedEvent(t *testing.T) {
	s := newStore(t)
	p := createPoll(t, s, "alice@example.com", "bob@example.com", "carol@example.com")
	castVotes(t, s, p.ID, map[string]int{"alice@example.com": 1, "bob@example.com": 0})
	before := time.Now().UTC().Add(-time.Second)
	castVotes(t, s, p.ID, map[string]int{"carol@example.com": 1})

	ev := storedEvent(t, s, p.ID+"#closed")
	want := eventItem{
		EventID: p.ID + "#closed",
		Type:    eventPollClosed,
		PollID:  p.ID,
		Payload: eventPayload{Question: p.Question, Options: p.Options, Recipients: p.Participants, Counts: []int{1, 2}},
		Pending: pendingValue,
	}
	assertEvent(t, ev, want, before)
	assertEventIDs(t, s, p.ID+"#created", p.ID+"#closed")
}

func TestConcurrentLastVotesWriteClosedEvent(t *testing.T) {
	const n = 50
	participants := make([]string, n)
	for i := range participants {
		participants[i] = fmt.Sprintf("voter-%d@example.com", i)
	}
	s := newStore(t)
	p := createPoll(t, s, participants...)

	var wg sync.WaitGroup
	for _, participant := range participants {
		wg.Go(func() {
			if _, err := s.CastVote(t.Context(), p.ID, participant, 0); err != nil {
				t.Errorf("CastVote(%s) error = %v", participant, err)
			}
		})
	}
	wg.Wait()

	if got := storedEvent(t, s, p.ID+"#closed").Payload.Counts; !slices.Equal(got, []int{n, 0}) {
		t.Errorf("closed event counts = %v, want [%d 0]", got, n)
	}
	assertEventIDs(t, s, p.ID+"#created", p.ID+"#closed")
}

// The read-repair tests start from the state a crash between the last vote
// and the close leaves behind: every participant has voted, but the stored
// status is still OPEN and there's no POLL_CLOSED event.

func TestGetRepairsFullyVotedOpenPoll(t *testing.T) {
	s := newStore(t)
	p := createStuckPoll(t, s)

	got, err := s.Get(t.Context(), p.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Status != poll.StatusClosed {
		t.Errorf("Get() status = %s, want %s", got.Status, poll.StatusClosed)
	}
	assertRepaired(t, s, p)
}

func TestCastVoteRepairsFullyVotedOpenPoll(t *testing.T) {
	s := newStore(t)
	p := createStuckPoll(t, s)

	if _, err := s.CastVote(t.Context(), p.ID, "alice@example.com", 0); !errors.Is(err, poll.ErrClosed) {
		t.Errorf("CastVote() error = %v, want %v", err, poll.ErrClosed)
	}
	assertRepaired(t, s, p)
}

func TestGetDoesNotCloseOpenPoll(t *testing.T) {
	s := newStore(t)
	p := createPoll(t, s, "alice@example.com", "bob@example.com")
	castVotes(t, s, p.ID, map[string]int{"alice@example.com": 0})

	if _, err := s.Get(t.Context(), p.ID); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got := storedStatus(t, s, p.ID); got != poll.StatusOpen {
		t.Errorf("stored status = %s, want %s", got, poll.StatusOpen)
	}
	assertEventIDs(t, s, p.ID+"#created")
}

// createStuckPoll stores a poll that every participant has voted on, with its
// stored status still OPEN.
func createStuckPoll(t *testing.T, s *Store) poll.Poll {
	t.Helper()
	p, err := poll.New("Lunch?", []string{"Tacos", "Pizza"}, []string{"alice@example.com", "bob@example.com"})
	if err != nil {
		t.Fatalf("poll.New() error = %v", err)
	}
	for participant, option := range map[string]int{"alice@example.com": 0, "bob@example.com": 1} {
		if err := p.Vote(participant, option); err != nil {
			t.Fatalf("Vote(%s) error = %v", participant, err)
		}
	}
	p.Status = poll.StatusOpen
	if err := s.Create(t.Context(), p); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	return p
}

func assertRepaired(t *testing.T, s *Store, p poll.Poll) {
	t.Helper()
	if got := storedStatus(t, s, p.ID); got != poll.StatusClosed {
		t.Errorf("stored status = %s, want %s", got, poll.StatusClosed)
	}
	if got := storedEvent(t, s, p.ID+"#closed").Payload.Counts; !slices.Equal(got, p.Counts) {
		t.Errorf("closed event counts = %v, want %v", got, p.Counts)
	}
}

func castVotes(t *testing.T, s *Store, pollID string, votes map[string]int) {
	t.Helper()
	for participant, option := range votes {
		if _, err := s.CastVote(t.Context(), pollID, participant, option); err != nil {
			t.Fatalf("CastVote(%s) error = %v", participant, err)
		}
	}
}

// assertEvent compares ev with want, and checks that ev's OccurredAt is an
// RFC 3339 time between notBefore and now.
func assertEvent(t *testing.T, ev, want eventItem, notBefore time.Time) {
	t.Helper()
	occurredAt, err := time.Parse(time.RFC3339, ev.OccurredAt)
	if err != nil {
		t.Errorf("occurredAt = %q, not RFC 3339: %v", ev.OccurredAt, err)
	} else if occurredAt.Before(notBefore) || occurredAt.After(time.Now().Add(time.Second)) {
		t.Errorf("occurredAt = %s, want between %s and now", occurredAt, notBefore)
	}

	want.OccurredAt = ev.OccurredAt
	if ev.EventID != want.EventID ||
		ev.Type != want.Type ||
		ev.PollID != want.PollID ||
		ev.Pending != want.Pending ||
		ev.Payload.Question != want.Payload.Question ||
		!slices.Equal(ev.Payload.Options, want.Payload.Options) ||
		!slices.Equal(ev.Payload.Recipients, want.Payload.Recipients) ||
		!slices.Equal(ev.Payload.Counts, want.Payload.Counts) {
		t.Errorf("event = %+v\nwant    %+v", ev, want)
	}
}

func storedEvent(t *testing.T, s *Store, eventID string) eventItem {
	t.Helper()
	out, err := s.client.GetItem(t.Context(), &dynamodb.GetItemInput{
		TableName:      aws.String(s.outbox),
		Key:            map[string]types.AttributeValue{"eventId": &types.AttributeValueMemberS{Value: eventID}},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		t.Fatalf("GetItem(%s) error = %v", eventID, err)
	}
	if out.Item == nil {
		t.Fatalf("event %s not found", eventID)
	}
	var ev eventItem
	if err := attributevalue.UnmarshalMap(out.Item, &ev); err != nil {
		t.Fatalf("unmarshal %s: %v", eventID, err)
	}
	return ev
}

// assertEventIDs checks that the outbox holds exactly the events in want.
func assertEventIDs(t *testing.T, s *Store, want ...string) {
	t.Helper()
	out, err := s.client.Scan(t.Context(), &dynamodb.ScanInput{
		TableName:      aws.String(s.outbox),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		t.Fatalf("Scan(%s) error = %v", s.outbox, err)
	}
	var got []string
	for _, item := range out.Items {
		var ev eventItem
		if err := attributevalue.UnmarshalMap(item, &ev); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}
		got = append(got, ev.EventID)
	}
	slices.Sort(got)
	want = slices.Sorted(slices.Values(want))
	if !slices.Equal(got, want) {
		t.Errorf("outbox events = %v, want %v", got, want)
	}
}
