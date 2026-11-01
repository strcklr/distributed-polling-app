// Package dynamo stores polls in DynamoDB, following the table design in
// docs/adr/0001-dynamodb-table-design.md.
package dynamo

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/strcklr/distributed-polling-app/internal/poll"
)

var _ poll.Store = (*Store)(nil)

// Store is a poll.Store backed by a DynamoDB table.
type Store struct {
	client *dynamodb.Client
	table  string
}

// NewStore returns a Store that reads and writes polls in table.
func NewStore(client *dynamodb.Client, table string) *Store {
	return &Store{client: client, table: table}
}

// TODO(#9): PutItem the poll, failing with poll.ErrAlreadyExists if the ID is taken.
func (s *Store) Create(ctx context.Context, p poll.Poll) error {
	return errors.New("not implemented")
}

// TODO(#9): GetItem with a consistent read, or poll.ErrNotFound.
func (s *Store) Get(ctx context.Context, id string) (poll.Poll, error) {
	return poll.Poll{}, errors.New("not implemented")
}

// TODO(#10): one conditional UpdateItem per vote, then the conditional close (#11).
func (s *Store) CastVote(ctx context.Context, pollID, participant string, option int) (poll.Poll, error) {
	return poll.Poll{}, errors.New("not implemented")
}
