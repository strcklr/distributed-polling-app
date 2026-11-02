package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

var testConfig = Config{
	ID:           "worker-1",
	PollInterval: 5 * time.Second,
	Lease:        2 * time.Minute,
	BatchSize:    10,
	Events:       2,
	Sends:        3,
}

func TestTickDeliversEveryRecipient(t *testing.T) {
	created := createdEvent("p1", "alice@example.com", "bob@example.com", "carol@example.com")
	closed := closedEvent("p2", "dave@example.com", "erin@example.com")
	src := newFakeSource(created, closed)
	snd := &fakeSender{}

	if err := newWorker(src, snd, testConfig).Tick(t.Context()); err != nil {
		t.Fatalf("Tick() error = %v", err)
	}

	assertSent(t, snd, created, closed)
	assertCompleted(t, src, created.ID, closed.ID)
	for id, c := range src.claims {
		if c.owner != testConfig.ID || c.lease != testConfig.Lease {
			t.Errorf("claim on %s = (%s, %s), want (%s, %s)", id, c.owner, c.lease, testConfig.ID, testConfig.Lease)
		}
	}
}

func TestTickClaimsBeforeSending(t *testing.T) {
	e := createdEvent("p1", "alice@example.com", "bob@example.com")
	src := newFakeSource(e)
	snd := &fakeSender{onSend: func(m Message) {
		if owner := src.owner(m.EventID); owner != testConfig.ID {
			t.Errorf("sent to %s while %s was claimed by %q, want %q", m.To, m.EventID, owner, testConfig.ID)
		}
	}}

	if err := newWorker(src, snd, testConfig).Tick(t.Context()); err != nil {
		t.Fatalf("Tick() error = %v", err)
	}
	assertSent(t, snd, e)
}

func TestTickReadsBatchSize(t *testing.T) {
	var events []Event
	for i := range 5 {
		events = append(events, createdEvent(fmt.Sprintf("p%d", i), "alice@example.com"))
	}
	src := newFakeSource(events...)
	cfg := testConfig
	cfg.BatchSize = 2

	if err := newWorker(src, &fakeSender{}, cfg).Tick(t.Context()); err != nil {
		t.Fatalf("Tick() error = %v", err)
	}

	if !slices.Equal(src.limits, []int{2}) {
		t.Errorf("Pending() limits = %v, want [2]", src.limits)
	}
	assertCompleted(t, src, events[0].ID, events[1].ID)
}

func TestTickSkipsUnavailableEvent(t *testing.T) {
	taken := createdEvent("p1", "alice@example.com")
	free := createdEvent("p2", "bob@example.com")
	src := newFakeSource(taken, free)
	src.unavailable[taken.ID] = true
	snd := &fakeSender{}

	if err := newWorker(src, snd, testConfig).Tick(t.Context()); err != nil {
		t.Fatalf("Tick() error = %v", err)
	}

	assertSent(t, snd, free)
	assertCompleted(t, src, free.ID)
}

func TestTickDoesNotCompleteAfterFailedSend(t *testing.T) {
	failing := createdEvent("p1", "alice@example.com", "bob@example.com")
	ok := createdEvent("p2", "carol@example.com")
	src := newFakeSource(failing, ok)
	snd := &fakeSender{fail: map[string]bool{"bob@example.com": true}}

	if err := newWorker(src, snd, testConfig).Tick(t.Context()); err != nil {
		t.Fatalf("Tick() error = %v", err)
	}

	assertCompleted(t, src, ok.ID)
}

func TestTickDoesNotSendInvalidEvent(t *testing.T) {
	invalid := closedEvent("p1", "alice@example.com")
	invalid.Counts = nil
	ok := createdEvent("p2", "bob@example.com")
	src := newFakeSource(invalid, ok)
	snd := &fakeSender{}

	if err := newWorker(src, snd, testConfig).Tick(t.Context()); err != nil {
		t.Fatalf("Tick() error = %v", err)
	}

	assertSent(t, snd, ok)
	assertCompleted(t, src, ok.ID)
}

func TestTickContinuesAfterLostLease(t *testing.T) {
	lost := createdEvent("p1", "alice@example.com")
	ok := createdEvent("p2", "bob@example.com")
	src := newFakeSource(lost, ok)
	snd := &fakeSender{onSend: func(m Message) {
		if m.EventID == lost.ID {
			src.steal(lost.ID, "worker-2")
		}
	}}

	if err := newWorker(src, snd, testConfig).Tick(t.Context()); err != nil {
		t.Fatalf("Tick() error = %v", err)
	}

	assertCompleted(t, src, ok.ID)
}

func TestTickReturnsPendingError(t *testing.T) {
	src := newFakeSource()
	src.pendingErr = errors.New("throttled")

	if err := newWorker(src, &fakeSender{}, testConfig).Tick(t.Context()); !errors.Is(err, src.pendingErr) {
		t.Errorf("Tick() error = %v, want %v", err, src.pendingErr)
	}
}

// The concurrency tests run in a synctest bubble, where time is fake: each
// send sleeps for a second, and the clock only moves once every goroutine is
// blocked, so the most sends in flight is exact rather than timing-dependent.

func TestTickLimitsSendsPerEvent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var recipients []string
		for i := range 20 {
			recipients = append(recipients, fmt.Sprintf("voter-%d@example.com", i))
		}
		e := createdEvent("p1", recipients...)
		src := newFakeSource(e)
		snd := &fakeSender{delay: time.Second}
		cfg := testConfig
		cfg.Events, cfg.Sends = 1, 4

		if err := newWorker(src, snd, cfg).Tick(t.Context()); err != nil {
			t.Fatalf("Tick() error = %v", err)
		}

		assertSent(t, snd, e)
		if snd.maxInFlight != cfg.Sends {
			t.Errorf("most sends in flight = %d, want %d", snd.maxInFlight, cfg.Sends)
		}
	})
}

func TestTickLimitsEventsInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var events []Event
		for i := range 6 {
			events = append(events, createdEvent(fmt.Sprintf("p%d", i), "alice@example.com"))
		}
		src := newFakeSource(events...)
		snd := &fakeSender{delay: time.Second}
		cfg := testConfig
		cfg.Events, cfg.Sends = 3, 5

		if err := newWorker(src, snd, cfg).Tick(t.Context()); err != nil {
			t.Fatalf("Tick() error = %v", err)
		}

		assertSent(t, snd, events...)
		if snd.maxInFlight != cfg.Events {
			t.Errorf("most sends in flight = %d, want %d", snd.maxInFlight, cfg.Events)
		}
	})
}

func TestRunTicksUntilCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newFakeSource()
		snd := &fakeSender{}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- newWorker(src, snd, testConfig).Run(ctx) }()

		synctest.Wait()
		if got := len(src.limits); got != 1 {
			t.Fatalf("Pending() called %d times before the first interval, want 1", got)
		}

		e := createdEvent("p1", "alice@example.com")
		src.add(e)
		time.Sleep(testConfig.PollInterval)
		synctest.Wait()
		assertSent(t, snd, e)
		assertCompleted(t, src, e.ID)

		cancel()
		synctest.Wait()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run() error = %v, want nil", err)
			}
		default:
			t.Fatal("Run() didn't return after ctx was canceled")
		}
	})
}

func newWorker(src *fakeSource, snd *fakeSender, cfg Config) *Worker {
	return New(src, snd, slog.New(slog.DiscardHandler), cfg)
}

func createdEvent(pollID string, recipients ...string) Event {
	return Event{
		ID:         pollID + "#created",
		Type:       EventPollCreated,
		PollID:     pollID,
		Question:   "Lunch?",
		Options:    []string{"Tacos", "Pizza"},
		Recipients: recipients,
	}
}

func closedEvent(pollID string, recipients ...string) Event {
	e := createdEvent(pollID, recipients...)
	e.ID = pollID + "#closed"
	e.Type = EventPollClosed
	e.Counts = []int{len(recipients), 0}
	return e
}

// assertSent checks that exactly one message went to each recipient of each
// event in want.
func assertSent(t *testing.T, snd *fakeSender, want ...Event) {
	t.Helper()
	var wantKeys []string
	for _, e := range want {
		for _, r := range e.Recipients {
			wantKeys = append(wantKeys, e.ID+" "+r)
		}
	}
	var got []string
	for _, m := range snd.sent {
		got = append(got, m.EventID+" "+m.To)
	}
	slices.Sort(got)
	slices.Sort(wantKeys)
	if !slices.Equal(got, wantKeys) {
		t.Errorf("sent %v, want %v", got, wantKeys)
	}
}

func assertCompleted(t *testing.T, src *fakeSource, want ...string) {
	t.Helper()
	got := slices.Sorted(slices.Values(src.completed))
	want = slices.Sorted(slices.Values(want))
	if !slices.Equal(got, want) {
		t.Errorf("completed %v, want %v", got, want)
	}
}

type claim struct {
	owner string
	lease time.Duration
}

// fakeSource is a Source over a list of events. Claims always succeed unless
// the event is marked unavailable, and Complete fails unless the caller holds
// the claim.
type fakeSource struct {
	mu          sync.Mutex
	events      []Event
	unavailable map[string]bool
	claims      map[string]claim
	completed   []string
	limits      []int
	pendingErr  error
}

func newFakeSource(events ...Event) *fakeSource {
	return &fakeSource{events: slices.Clone(events), unavailable: map[string]bool{}, claims: map[string]claim{}}
}

func (s *fakeSource) Pending(ctx context.Context, limit int) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.limits = append(s.limits, limit)
	if s.pendingErr != nil {
		return nil, s.pendingErr
	}
	return slices.Clone(s.events[:min(limit, len(s.events))]), nil
}

func (s *fakeSource) Claim(ctx context.Context, eventID, owner string, lease time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unavailable[eventID] {
		return ErrNotAvailable
	}
	s.claims[eventID] = claim{owner: owner, lease: lease}
	return nil
}

func (s *fakeSource) Complete(ctx context.Context, eventID, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claims[eventID].owner != owner {
		return ErrLeaseLost
	}
	s.completed = append(s.completed, eventID)
	s.events = slices.DeleteFunc(s.events, func(e Event) bool { return e.ID == eventID })
	return nil
}

func (s *fakeSource) add(e Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
}

func (s *fakeSource) owner(eventID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claims[eventID].owner
}

// steal hands the claim on eventID to owner, as if the lease had expired and
// another worker had claimed it.
func (s *fakeSource) steal(eventID, owner string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claims[eventID] = claim{owner: owner, lease: s.claims[eventID].lease}
}

// fakeSender records each message it sends and the most sends in flight at
// once. Sends to addresses in fail return an error.
type fakeSender struct {
	onSend func(Message)
	delay  time.Duration
	fail   map[string]bool

	mu          sync.Mutex
	sent        []Message
	inFlight    int
	maxInFlight int
}

func (s *fakeSender) Send(ctx context.Context, m Message) error {
	if s.onSend != nil {
		s.onSend(m)
	}
	s.mu.Lock()
	s.inFlight++
	s.maxInFlight = max(s.maxInFlight, s.inFlight)
	s.mu.Unlock()

	time.Sleep(s.delay)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.inFlight--
	if s.fail[m.To] {
		return fmt.Errorf("send to %s: mailbox unavailable", m.To)
	}
	s.sent = append(s.sent, m)
	return nil
}
