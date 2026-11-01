// Package dynamo stores polls in DynamoDB, following the table design in
// docs/adr/0001-dynamodb-table-design.md.
package dynamo

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

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

// pollItem represents a DynamoDB item
type pollItem struct {
	ID           string         `dynamodbav:"id"`
	Question     string         `dynamodbav:"question"`
	Options      []string       `dynamodbav:"options"`
	Participants []string       `dynamodbav:"participants"`
	Status       string         `dynamodbav:"status"`
	Votes        map[string]int `dynamodbav:"votes"`
	Counts       []int          `dynamodbav:"counts"`
	VotesCast    int            `dynamodbav:"votesCast"`
}

func toItem(p poll.Poll) pollItem {
	votes := p.Votes
	if votes == nil {
		votes = map[string]int{}
	}

	return pollItem{
		ID:           p.ID,
		Question:     p.Question,
		Options:      p.Options,
		Participants: p.Participants,
		Status:       string(p.Status),
		Votes:        votes,
		Counts:       p.Counts,
		VotesCast:    len(votes),
	}
}

func (it pollItem) toPoll() poll.Poll {
	return poll.Poll{
		ID:           it.ID,
		Question:     it.Question,
		Options:      it.Options,
		Participants: it.Participants,
		Status:       poll.Status(it.Status),
		Votes:        it.Votes,
		Counts:       it.Counts,
	}
}

func (s *Store) Create(ctx context.Context, p poll.Poll) error {
	it := toItem(p)

	av, err := attributevalue.MarshalMap(it)
	if err != nil {
		return err
	}

	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(s.table),
		Item:                av,
		ConditionExpression: aws.String("attribute_not_exists(id)"),
	})
	if err != nil {
		if _, ok := errors.AsType[*types.ConditionalCheckFailedException](err); ok {
			return poll.ErrAlreadyExists
		}
	}

	return err
}

func (s *Store) Get(ctx context.Context, id string) (poll.Poll, error) {
	key := map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: id}}

	result, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(s.table),
		Key:            key,
		ConsistentRead: aws.Bool(true),
	})

	if err != nil {
		return poll.Poll{}, err
	}

	if result.Item == nil {
		return poll.Poll{}, poll.ErrNotFound
	}

	var it pollItem
	err = attributevalue.UnmarshalMap(result.Item, &it)
	if err != nil {
		return poll.Poll{}, err
	}

	return it.toPoll(), nil
}

// TODO(#10): implement as one conditional UpdateItem, following the ADR's
// "Casting a vote" section. Things checked against DynamoDB Local:
//   - Reject option < 0 before building the request: counts[-1] isn't valid
//     expression syntax, so it would fail as a ValidationException.
//   - Without the size(options) > :option condition, ADD counts[5] appends to
//     the list instead of failing.
//   - On failure, errors.AsType[*types.ConditionalCheckFailedException] gives
//     the old item in its Item field (nil when the poll doesn't exist).
//
// The ADR reports a poll as CLOSED once votesCast equals the number of
// participants. Putting that rule in toPoll makes Get, CastVote and the
// failure classification agree, and the close tests pass before #11 writes
// the stored status.
func (s *Store) CastVote(ctx context.Context, pollID, participant string, option int) (poll.Poll, error) {
	return poll.Poll{}, errors.New("not implemented")
}
