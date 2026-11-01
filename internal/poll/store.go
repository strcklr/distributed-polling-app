package poll

import (
	"context"
	"sync"
)

// Store persists polls. CastVote must apply Poll.Vote atomically,
// so concurrent votes on the same poll can't both count or both close it.
type Store interface {
	Create(ctx context.Context, p Poll) error
	Get(ctx context.Context, id string) (Poll, error)
	CastVote(ctx context.Context, pollID, participant string, option int) (Poll, error)
}

var _ Store = (*MemoryStore)(nil)

// MemoryStore is a Store backed by a map, for tests and local runs.
type MemoryStore struct {
	mu    sync.Mutex
	polls map[string]*Poll
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{polls: make(map[string]*Poll)}
}

// Create stores a copy of p.
func (s *MemoryStore) Create(ctx context.Context, p Poll) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.polls[p.ID]; ok {
		return ErrAlreadyExists
	}

	stored := p.clone()
	s.polls[p.ID] = &stored
	return nil
}

// Get returns a copy of the poll with the given id, or ErrNotFound.
func (s *MemoryStore) Get(ctx context.Context, id string) (Poll, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	p, ok := s.polls[id]
	if !ok {
		return Poll{}, ErrNotFound
	}
	return p.clone(), nil
}

// CastVote applies the vote under the store's lock and returns the updated poll.
func (s *MemoryStore) CastVote(ctx context.Context, pollID, participant string, option int) (Poll, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	p, ok := s.polls[pollID]
	if !ok {
		return Poll{}, ErrNotFound
	}
	if err := p.Vote(participant, option); err != nil {
		return Poll{}, err
	}
	return p.clone(), nil
}
