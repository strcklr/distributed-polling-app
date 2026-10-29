package poll

import (
	"errors"
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
		name        string
		participant string
		option      int
	}{
		{name: "first vote", participant: "bob", option: 0},
		{name: "second vote", participant: "alice", option: 0},
		{name: "final vote", participant: "billy", option: 1},
	}

	for _, tt := range votes {
		err := poll.Vote(tt.participant, tt.option)
		if err != nil {
			t.Errorf("Vote() error = %v", err)
		}
	}
	if poll.Status != StatusClosed {
		t.Errorf("poll did not close after everyone voted, status: %s", poll.Status)
	}
	if poll.Counts[0] != 2 {
		t.Errorf("votes didn't get counted correctly")
	}
}
