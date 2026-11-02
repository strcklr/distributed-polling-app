package poll

import (
	"errors"
	"slices"
	"testing"
)

func TestNormalizeAddress(t *testing.T) {
	tests := []struct {
		name    string
		addr    string
		want    string
		wantErr bool
	}{
		{name: "bare address", addr: "alice@example.com", want: "alice@example.com"},
		{name: "mixed case", addr: "Alice@Example.COM", want: "alice@example.com"},
		{name: "surrounding space", addr: "  alice@example.com\t", want: "alice@example.com"},
		{name: "plain name", addr: "alice", wantErr: true},
		{name: "empty", addr: "", wantErr: true},
		{name: "missing domain", addr: "alice@", wantErr: true},
		{name: "display name", addr: "Alice <alice@example.com>", wantErr: true},
		{name: "angle brackets", addr: "<alice@example.com>", wantErr: true},
		{name: "two addresses", addr: "alice@example.com, bob@example.com", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NormalizeAddress(tt.addr)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NormalizeAddress(%q) = %q, want an error", tt.addr, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("NormalizeAddress(%q) error = %v", tt.addr, err)
			}
			if got != tt.want {
				t.Errorf("NormalizeAddress(%q) = %q, want %q", tt.addr, got, tt.want)
			}
		})
	}
}

func TestNewRejectsInvalidParticipants(t *testing.T) {
	tests := []struct {
		name         string
		participants []string
	}{
		{name: "plain name", participants: []string{"alice@example.com", "bob"}},
		{name: "display name", participants: []string{"Bob <bob@example.com>"}},
		{name: "duplicate after normalizing", participants: []string{"alice@example.com", "Alice@Example.com"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New("Lunch?", []string{"Tacos", "Pizza"}, tt.participants)
			if !errors.Is(err, ErrInvalidPoll) {
				t.Errorf("New() error = %v, want %v", err, ErrInvalidPoll)
			}
		})
	}
}

func TestNewNormalizesParticipants(t *testing.T) {
	p, err := New("Lunch?", []string{"Tacos", "Pizza"}, []string{" Alice@Example.com ", "BOB@example.com"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if want := []string{"alice@example.com", "bob@example.com"}; !slices.Equal(p.Participants, want) {
		t.Errorf("Participants = %q, want %q", p.Participants, want)
	}
}

func TestVoteNormalizesParticipant(t *testing.T) {
	p := newTestPoll(t)

	if err := p.Vote(" Alice@Example.COM", 1); err != nil {
		t.Fatalf("Vote() error = %v", err)
	}

	if got, ok := p.Votes["alice@example.com"]; !ok || got != 1 {
		t.Errorf("Votes = %v, want alice@example.com: 1", p.Votes)
	}
}
