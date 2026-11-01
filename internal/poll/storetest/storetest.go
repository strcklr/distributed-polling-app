// Package storetest is a test suite that every poll.Store implementation must pass.
package storetest

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/strcklr/distributed-polling-app/internal/poll"
)

// NewStore returns an empty store. Each call must return a store that
// doesn't share polls with stores from earlier calls.
type NewStore func(t *testing.T) poll.Store

// RunCreateGet tests Create and Get.
func RunCreateGet(t *testing.T, newStore NewStore) {
	t.Run("Get returns the created poll", func(t *testing.T) {
		s, p := storeWithPoll(t, newStore)

		got, err := s.Get(t.Context(), p.ID)
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		assertPollEqual(t, got, p)
	})

	t.Run("Get round-trips votes, counts and status", func(t *testing.T) {
		s := newStore(t)
		p := newPoll(t, "alice", "bob")
		mustVote(t, &p, "alice", 1)
		mustVote(t, &p, "bob", 0)
		if err := s.Create(t.Context(), p); err != nil {
			t.Fatalf("Create() error = %v", err)
		}

		got, err := s.Get(t.Context(), p.ID)
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		assertPollEqual(t, got, p)
	})

	t.Run("Get of a missing poll returns ErrNotFound", func(t *testing.T) {
		s := newStore(t)

		_, err := s.Get(t.Context(), "missing")
		if !errors.Is(err, poll.ErrNotFound) {
			t.Errorf("Get() error = %v, want %v", err, poll.ErrNotFound)
		}
	})

	t.Run("Create with an existing ID returns ErrAlreadyExists", func(t *testing.T) {
		s, p := storeWithPoll(t, newStore)
		duplicate := newPoll(t, "carol")
		duplicate.ID = p.ID

		err := s.Create(t.Context(), duplicate)
		if !errors.Is(err, poll.ErrAlreadyExists) {
			t.Errorf("Create() error = %v, want %v", err, poll.ErrAlreadyExists)
		}
		assertStored(t, s, p)
	})

	t.Run("Create stores a copy", func(t *testing.T) {
		s, p := storeWithPoll(t, newStore)
		want := clonePoll(p)

		p.Options[0] = "Sushi"
		p.Counts[0] = 99
		p.Votes["mallory"] = 0

		assertStored(t, s, want)
	})

	t.Run("Get returns a copy", func(t *testing.T) {
		s, p := storeWithPoll(t, newStore)

		got, err := s.Get(t.Context(), p.ID)
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		got.Options[0] = "Sushi"
		got.Counts[0] = 99
		got.Votes["mallory"] = 0
		got.Status = poll.StatusClosed

		assertStored(t, s, p)
	})
}

// RunCastVote tests CastVote, including concurrent votes on one poll.
func RunCastVote(t *testing.T, newStore NewStore) {
	t.Run("records the vote", func(t *testing.T) {
		s, p := storeWithPoll(t, newStore)

		got, err := s.CastVote(t.Context(), p.ID, "alice", 1)
		if err != nil {
			t.Fatalf("CastVote() error = %v", err)
		}
		mustVote(t, &p, "alice", 1)
		assertPollEqual(t, got, p)
		assertStored(t, s, p)
	})

	t.Run("last vote closes the poll", func(t *testing.T) {
		s, p := storeWithPoll(t, newStore)
		castVotes(t, s, p.ID, "alice", "bob")

		got, err := s.CastVote(t.Context(), p.ID, "carol", 0)
		if err != nil {
			t.Fatalf("CastVote() error = %v", err)
		}
		if got.Status != poll.StatusClosed {
			t.Errorf("returned Status = %s, want %s", got.Status, poll.StatusClosed)
		}
		stored, err := s.Get(t.Context(), p.ID)
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if stored.Status != poll.StatusClosed {
			t.Errorf("stored Status = %s, want %s", stored.Status, poll.StatusClosed)
		}
	})

	t.Run("rejected votes", func(t *testing.T) {
		tests := []struct {
			name        string
			pollID      string
			priorVotes  []string
			participant string
			option      int
			wantErr     error
		}{
			{name: "missing poll", pollID: "missing", participant: "alice", wantErr: poll.ErrNotFound},
			{name: "not a participant", participant: "mallory", wantErr: poll.ErrNotParticipant},
			{name: "negative option", participant: "alice", option: -1, wantErr: poll.ErrInvalidOption},
			{name: "option out of range", participant: "alice", option: 2, wantErr: poll.ErrInvalidOption},
			{name: "already voted", priorVotes: []string{"alice"}, participant: "alice", wantErr: poll.ErrAlreadyVoted},
			{name: "closed poll", priorVotes: []string{"alice", "bob", "carol"}, participant: "alice", wantErr: poll.ErrClosed},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				s, p := storeWithPoll(t, newStore)
				castVotes(t, s, p.ID, tt.priorVotes...)
				before, err := s.Get(t.Context(), p.ID)
				if err != nil {
					t.Fatalf("Get() error = %v", err)
				}
				pollID := tt.pollID
				if pollID == "" {
					pollID = p.ID
				}

				_, err = s.CastVote(t.Context(), pollID, tt.participant, tt.option)

				if !errors.Is(err, tt.wantErr) {
					t.Errorf("CastVote() error = %v, want %v", err, tt.wantErr)
				}
				assertStored(t, s, before)
			})
		}
	})

	t.Run("concurrent votes all count and close the poll once", func(t *testing.T) {
		const n = 50
		participants := make([]string, n)
		for i := range participants {
			participants[i] = fmt.Sprintf("voter-%d", i)
		}
		s := newStore(t)
		p := newPoll(t, participants...)
		if err := s.Create(t.Context(), p); err != nil {
			t.Fatalf("Create() error = %v", err)
		}

		var (
			wg     sync.WaitGroup
			mu     sync.Mutex
			closed int
		)
		for i, participant := range participants {
			wg.Go(func() {
				got, err := s.CastVote(t.Context(), p.ID, participant, i%2)
				if err != nil {
					t.Errorf("CastVote(%s) error = %v", participant, err)
					return
				}
				if got.Status == poll.StatusClosed {
					mu.Lock()
					closed++
					mu.Unlock()
				}
			})
		}
		wg.Wait()

		if closed != 1 {
			t.Errorf("%d CastVote calls returned a closed poll, want exactly 1", closed)
		}
		got, err := s.Get(t.Context(), p.ID)
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if got.Status != poll.StatusClosed {
			t.Errorf("Status = %s, want %s", got.Status, poll.StatusClosed)
		}
		if got.Counts[0] != n/2 || got.Counts[1] != n/2 || len(got.Votes) != n {
			t.Errorf("Counts = %v with %d votes, want [%d %d] with %d votes", got.Counts, len(got.Votes), n/2, n/2, n)
		}
	})
}

func newPoll(t *testing.T, participants ...string) poll.Poll {
	t.Helper()
	p, err := poll.New("Lunch?", []string{"Tacos", "Pizza"}, participants)
	if err != nil {
		t.Fatalf("poll.New() error = %v", err)
	}
	return p
}

func storeWithPoll(t *testing.T, newStore NewStore) (poll.Store, poll.Poll) {
	t.Helper()
	s := newStore(t)
	p := newPoll(t, "alice", "bob", "carol")
	if err := s.Create(t.Context(), p); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	return s, p
}

func mustVote(t *testing.T, p *poll.Poll, participant string, option int) {
	t.Helper()
	if err := p.Vote(participant, option); err != nil {
		t.Fatalf("Vote(%s, %d) error = %v", participant, option, err)
	}
}

func castVotes(t *testing.T, s poll.Store, pollID string, participants ...string) {
	t.Helper()
	for _, participant := range participants {
		if _, err := s.CastVote(t.Context(), pollID, participant, 0); err != nil {
			t.Fatalf("CastVote(%s) error = %v", participant, err)
		}
	}
}

func assertStored(t *testing.T, s poll.Store, want poll.Poll) {
	t.Helper()
	got, err := s.Get(t.Context(), want.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	assertPollEqual(t, got, want)
}

// assertPollEqual treats nil and empty slices and maps as equal, since a
// store may not preserve the difference.
func assertPollEqual(t *testing.T, got, want poll.Poll) {
	t.Helper()
	if got.ID != want.ID ||
		got.Question != want.Question ||
		got.Status != want.Status ||
		!slices.Equal(got.Options, want.Options) ||
		!slices.Equal(got.Participants, want.Participants) ||
		!slices.Equal(got.Counts, want.Counts) ||
		!maps.Equal(got.Votes, want.Votes) {
		t.Errorf("poll = %+v\nwant   %+v", got, want)
	}
}

func clonePoll(p poll.Poll) poll.Poll {
	p.Options = slices.Clone(p.Options)
	p.Participants = slices.Clone(p.Participants)
	p.Counts = slices.Clone(p.Counts)
	p.Votes = maps.Clone(p.Votes)
	return p
}
