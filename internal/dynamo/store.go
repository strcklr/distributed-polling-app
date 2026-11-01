// Package dynamo stores polls in DynamoDB, following the table design in
// docs/adr/0001-dynamodb-table-design.md.
package dynamo

import (
	"context"
	"errors"
	"fmt"
	"strconv"

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
	status := poll.Status(it.Status)
	if it.VotesCast >= len(it.Participants) {
		status = poll.StatusClosed
	}

	return poll.Poll{
		ID:           it.ID,
		Question:     it.Question,
		Options:      it.Options,
		Participants: it.Participants,
		Status:       status,
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

func (s *Store) CastVote(ctx context.Context, pollID, participant string, option int) (poll.Poll, error) {
	if option < 0 {
		return poll.Poll{}, poll.ErrInvalidOption
	}

	out, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.table),
		Key:       map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: pollID}},
		UpdateExpression: aws.String(fmt.Sprintf(
			"SET votes.#p = :option, counts[%[1]d] = counts[%[1]d] + :one, votesCast = votesCast + :one",
			option,
		)),
		ConditionExpression: aws.String(`
			attribute_exists(id)
			AND #status = :open
			AND contains(participants, :participant)
			AND size(options) > :option
			AND attribute_not_exists(votes.#p)
		`),
		ExpressionAttributeNames: map[string]string{
			"#p":      participant,
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":one":         &types.AttributeValueMemberN{Value: "1"},
			":open":        &types.AttributeValueMemberS{Value: string(poll.StatusOpen)},
			":participant": &types.AttributeValueMemberS{Value: participant},
			":option":      &types.AttributeValueMemberN{Value: strconv.Itoa(option)},
		},
		ReturnValues:                        types.ReturnValueAllNew,
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})
	if err != nil {
		if ccf, ok := errors.AsType[*types.ConditionalCheckFailedException](err); ok {
			if ccf.Item == nil {
				return poll.Poll{}, poll.ErrNotFound
			}

			var old pollItem
			if e := attributevalue.UnmarshalMap(ccf.Item, &old); e != nil {
				return poll.Poll{}, e
			}

			p := old.toPoll()
			if err := p.Vote(participant, option); err != nil {
				return poll.Poll{}, err
			}
			return poll.Poll{}, fmt.Errorf("vote by %s on poll %s failed its condition but passes Poll.Vote", participant, pollID)
		}
		return poll.Poll{}, err
	}

	var it pollItem
	err = attributevalue.UnmarshalMap(out.Attributes, &it)
	if err != nil {
		return poll.Poll{}, err
	}

	return it.toPoll(), nil
}
