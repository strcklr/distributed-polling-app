package poll

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func newStoreWithPoll(t *testing.T) (*MemoryStore, Poll) {
	t.Helper()
	s := NewMemoryStore()
	p := newTestPoll(t)
	if err := s.Create(t.Context(), p); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	return s, p
}

func TestMemoryStoreGet(t *testing.T) {
	s, p := newStoreWithPoll(t)

	got, err := s.Get(t.Context(), p.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !reflect.DeepEqual(got, p) {
		t.Errorf("Get() = %+v, want %+v", got, p)
	}
}

func TestMemoryStoreGetMissing(t *testing.T) {
	s := NewMemoryStore()

	_, err := s.Get(t.Context(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get() error = %v, want %v", err, ErrNotFound)
	}
}

func TestMemoryStoreCreateStoresCopy(t *testing.T) {
	s, p := newStoreWithPoll(t)

	p.Counts[0] = 99
	p.Options[0] = "Sushi"

	got, err := s.Get(t.Context(), p.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Counts[0] != 0 || got.Options[0] != "Tacos" {
		t.Errorf("stored poll changed with caller's copy: Counts = %v, Options = %v", got.Counts, got.Options)
	}
}

func TestMemoryStoreGetReturnsCopy(t *testing.T) {
	s, p := newStoreWithPoll(t)

	first, err := s.Get(t.Context(), p.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	first.Counts[0] = 99
	first.Status = StatusClosed

	second, err := s.Get(t.Context(), p.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if second.Counts[0] != 0 || second.Status != StatusOpen {
		t.Errorf("stored poll changed through a returned copy: Counts = %v, Status = %s", second.Counts, second.Status)
	}
}

func TestMemoryStoreCastVote(t *testing.T) {
	s, p := newStoreWithPoll(t)

	got, err := s.CastVote(t.Context(), p.ID, "alice", 1)
	if err != nil {
		t.Fatalf("CastVote() error = %v", err)
	}
	if got.Counts[1] != 1 {
		t.Errorf("returned Counts = %v, want 1 vote for option 1", got.Counts)
	}

	stored, err := s.Get(t.Context(), p.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if stored.Counts[1] != 1 {
		t.Errorf("stored Counts = %v, want 1 vote for option 1", stored.Counts)
	}
}

func TestMemoryStoreCastVoteErrors(t *testing.T) {
	tests := []struct {
		name        string
		pollID      string
		participant string
		wantErr     error
	}{
		{name: "missing poll", pollID: "missing", participant: "alice", wantErr: ErrNotFound},
		{name: "not a participant", participant: "mallory", wantErr: ErrNotParticipant},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, p := newStoreWithPoll(t)
			pollID := tt.pollID
			if pollID == "" {
				pollID = p.ID
			}

			_, err := s.CastVote(t.Context(), pollID, tt.participant, 0)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("CastVote() error = %v, want %v", err, tt.wantErr)
			}

			stored, err := s.Get(t.Context(), p.ID)
			if err != nil {
				t.Fatalf("Get() error = %v", err)
			}
			if !reflect.DeepEqual(stored, p) {
				t.Errorf("stored poll changed after a rejected vote: %+v", stored)
			}
		})
	}
}

func TestMemoryStoreConcurrentVotes(t *testing.T) {
	const n = 50
	participants := make([]string, n)
	for i := range participants {
		participants[i] = fmt.Sprintf("voter-%d", i)
	}
	p, err := New("Lunch?", []string{"Tacos", "Pizza"}, participants)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	s := NewMemoryStore()
	if err := s.Create(t.Context(), p); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	var wg sync.WaitGroup
	for i, participant := range participants {
		wg.Go(func() {
			if _, err := s.CastVote(t.Context(), p.ID, participant, i%2); err != nil {
				t.Errorf("CastVote(%s) error = %v", participant, err)
			}
		})
	}
	wg.Wait()

	got, err := s.Get(t.Context(), p.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Status != StatusClosed {
		t.Errorf("Status = %s, want %s", got.Status, StatusClosed)
	}
	if got.Counts[0]+got.Counts[1] != n {
		t.Errorf("Counts = %v, want %d votes in total", got.Counts, n)
	}
}
