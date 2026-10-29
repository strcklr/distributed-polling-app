// Package poll holds the poll domain model and the rules for voting on it.
package poll

import (
	"crypto/rand"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Status is where a poll is in its lifecycle.
type Status string

const (
	StatusOpen   Status = "OPEN"
	StatusClosed Status = "CLOSED"
)

var (
	ErrInvalidPoll    = errors.New("invalid poll")
	ErrNotFound       = errors.New("poll not found")
	ErrClosed         = errors.New("poll is closed")
	ErrNotParticipant = errors.New("not a participant")
	ErrAlreadyVoted   = errors.New("participant already voted")
	ErrInvalidOption  = errors.New("invalid option")
)

// Poll is a question with fixed options that each participant answers once.
// It closes when every participant has voted.
type Poll struct {
	ID           string   `json:"id"`
	Question     string   `json:"question"`
	Options      []string `json:"options"`
	Participants []string `json:"participants"`
	Status       Status   `json:"status"`
	Counts       []int    `json:"counts"`

	votes map[string]int
}

// New validates the input and returns an open poll with no votes.
func New(question string, options, participants []string) (Poll, error) {
	question = strings.TrimSpace(question)
	switch {
	case question == "":
		return Poll{}, fmt.Errorf("%w: question is required", ErrInvalidPoll)
	case len(options) < 2:
		return Poll{}, fmt.Errorf("%w: at least 2 options are required", ErrInvalidPoll)
	case len(participants) < 1:
		return Poll{}, fmt.Errorf("%w: at least 1 participant is required", ErrInvalidPoll)
	case hasDuplicates(options):
		return Poll{}, fmt.Errorf("%w: options must be unique", ErrInvalidPoll)
	case hasDuplicates(participants):
		return Poll{}, fmt.Errorf("%w: participants must be unique", ErrInvalidPoll)
	}

	return Poll{
		ID:           rand.Text(),
		Question:     question,
		Options:      slices.Clone(options),
		Participants: slices.Clone(participants),
		Status:       StatusOpen,
		Counts:       make([]int, len(options)),
		votes:        make(map[string]int),
	}, nil
}

// Vote records participant's choice of the option at index option,
// and closes the poll if it was the last outstanding vote.
func (p *Poll) Vote(participant string, option int) error {
	switch {
	case p.Status != StatusOpen:
		return ErrClosed
	case !slices.Contains(p.Participants, participant):
		return ErrNotParticipant
	case option < 0 || option >= len(p.Options):
		return ErrInvalidOption
	}
	if _, voted := p.votes[participant]; voted {
		return ErrAlreadyVoted
	}

	p.votes[participant] = option
	p.Counts[option]++
	if len(p.votes) == len(p.Participants) {
		p.Status = StatusClosed
	}
	return nil
}

func (p Poll) clone() Poll {
	p.Options = slices.Clone(p.Options)
	p.Participants = slices.Clone(p.Participants)
	p.Counts = slices.Clone(p.Counts)
	p.votes = maps.Clone(p.votes)
	return p
}

func hasDuplicates(values []string) bool {
	seen := make(map[string]bool, len(values))
	for _, v := range values {
		if seen[v] {
			return true
		}
		seen[v] = true
	}
	return false
}
