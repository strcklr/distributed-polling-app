package poll

import (
	"errors"
	"slices"
	"testing"
)

func newTestPoll(t *testing.T) Poll {
	t.Helper()
	poll, err := New("Lunch Plans", []string{"Tacos", "Pizza", "Gyros"}, []string{"alice", "bob", "billy"})
	if err != nil {
		t.Fatalf("error creating poll: %v", err)
	}
	return poll
}

func TestVote(t *testing.T) {
	tests := []struct {
		name        string
		priorVotes  []string
		participant string
		option      int
		wantErr     error
	}{
		{name: "valid vote", participant: "bob", option: 0, wantErr: nil},
		{name: "invalid option, lower bound", participant: "bob", option: -1, wantErr: ErrInvalidOption},
		{name: "invalid option, upper bound", participant: "bob", option: 3, wantErr: ErrInvalidOption},
		{name: "not a participant", participant: "foo", option: 0, wantErr: ErrNotParticipant},
		{name: "already voted", priorVotes: []string{"bob"}, participant: "bob", option: 0, wantErr: ErrAlreadyVoted},
		{name: "poll closed", priorVotes: []string{"bob", "alice", "billy"}, participant: "bob", option: 0, wantErr: ErrClosed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			poll := newTestPoll(t)
			for _, pv := range tt.priorVotes {
				if err := poll.Vote(pv, 0); err != nil {
					t.Fatalf("error setting prior vote %v", err)
				}
			}
			err := poll.Vote(tt.participant, tt.option)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Vote() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestVoteClosesPollAfterLastVote(t *testing.T) {
	poll := newTestPoll(t)
	votes := []struct {
		participant string
		option      int
	}{
		{participant: "bob", option: 0},
		{participant: "alice", option: 0},
		{participant: "billy", option: 1},
	}

	for i, tt := range votes {
		err := poll.Vote(tt.participant, tt.option)
		if err != nil {
			t.Fatalf("Vote() error = %v", err)
		}
		if i < len(votes)-1 && poll.Status != StatusOpen {
			t.Fatalf("poll closed prematurely, i: %d, vote: %v", i, tt)
		}
	}
	if poll.Status != StatusClosed {
		t.Errorf("poll did not close after everyone voted, status: %s", poll.Status)
	}
	want := []int{2, 1, 0}
	if !slices.Equal(poll.Counts, want) {
		t.Errorf("incorrect vote counts, counts = %v, want %v", poll.Counts, want)
	}
}
