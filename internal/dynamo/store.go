// Package dynamo stores polls in DynamoDB, following the table design in
// docs/adr/0001-dynamodb-table-design.md and the outbox in
// docs/adr/0002-outbox.md.
package dynamo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/strcklr/distributed-polling-app/internal/poll"
)

var _ poll.Store = (*Store)(nil)

// Store is a poll.Store backed by DynamoDB. It writes poll events to an
// outbox table in the same transaction as the change they record.
type Store struct {
	client *dynamodb.Client
	table  string
	outbox string
}

// NewStore returns a Store that keeps polls in table and their events in
// outbox.
func NewStore(client *dynamodb.Client, table, outbox string) *Store {
	return &Store{client: client, table: table, outbox: outbox}
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
	if it.VotesCast == len(it.Participants) {
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

func (it pollItem) needsRepair() bool {
	return poll.Status(it.Status) == poll.StatusOpen && it.VotesCast == len(it.Participants)
}

func (it pollItem) toEvent(eventType string) eventItem {
	var eventID string
	var counts []int
	switch eventType {
	case eventPollCreated:
		eventID = it.ID + "#created"
	case eventPollClosed:
		eventID = it.ID + "#closed"
		counts = it.Counts
	}

	payload := eventPayload{
		Question:   it.Question,
		Options:    it.Options,
		Recipients: it.Participants,
		Counts:     counts,
	}

	return eventItem{
		EventID:    eventID,
		Type:       eventType,
		PollID:     it.ID,
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Payload:    payload,
		Pending:    pendingValue,
	}
}

func marshalError(msg string, pollID string, err error) error {
	return fmt.Errorf("marshal %s for poll %s: %w", msg, pollID, err)
}

func (s *Store) Create(ctx context.Context, p poll.Poll) error {
	it := toItem(p)

	av, err := attributevalue.MarshalMap(it)
	if err != nil {
		return marshalError("poll item", it.ID, err)
	}

	event := it.toEvent(eventPollCreated)
	eav, err := attributevalue.MarshalMap(event)
	if err != nil {
		return marshalError("create event", it.ID, err)
	}

	if _, err = s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Put: &types.Put{
					TableName:           aws.String(s.table),
					Item:                av,
					ConditionExpression: aws.String("attribute_not_exists(id)"),
				},
			},
			{
				Put: &types.Put{
					TableName:           aws.String(s.outbox),
					Item:                eav,
					ConditionExpression: aws.String("attribute_not_exists(eventId)"),
				},
			},
		},
	}); err != nil {
		if tce, ok := errors.AsType[*types.TransactionCanceledException](err); ok {
			if len(tce.CancellationReasons) > 0 &&
				aws.ToString(tce.CancellationReasons[0].Code) == "ConditionalCheckFailed" {
				return poll.ErrAlreadyExists
			}
		}

		return fmt.Errorf("create poll %s: %w", it.ID, err)
	}

	return nil
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

	if it.needsRepair() {
		if err := s.closePoll(context.WithoutCancel(ctx), it); err != nil {
			slog.ErrorContext(ctx, "repairing poll", slog.Any("err", err), slog.String("pollID", it.ID))
		}
	}

	return it.toPoll(), nil
}

func (s *Store) CastVote(ctx context.Context, pollID, participant string, option int) (poll.Poll, error) {
	if option < 0 {
		return poll.Poll{}, poll.ErrInvalidOption
	}

	participant, err := poll.NormalizeAddress(participant)
	if err != nil {
		return poll.Poll{}, poll.ErrNotParticipant
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
				if errors.Is(err, poll.ErrClosed) && old.needsRepair() {
					if err := s.closePoll(context.WithoutCancel(ctx), old); err != nil {
						slog.ErrorContext(ctx, "repairing poll", slog.Any("err", err), slog.String("pollID", old.ID))
					}
				}
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

	if it.VotesCast >= len(it.Participants) {
		if err := s.closePoll(context.WithoutCancel(ctx), it); err != nil {
			slog.ErrorContext(ctx, "closing poll after last vote", slog.Any("err", err), slog.String("pollID", it.ID))
		}
	}

	return it.toPoll(), nil
}

func (s *Store) closePoll(ctx context.Context, it pollItem) error {
	event := it.toEvent(eventPollClosed)
	av, err := attributevalue.MarshalMap(event)
	if err != nil {
		return marshalError("close event", it.ID, err)
	}

	if _, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Update: &types.Update{
					TableName:           aws.String(s.table),
					Key:                 map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: it.ID}},
					UpdateExpression:    aws.String("SET #status = :closed"),
					ConditionExpression: aws.String("#status = :open"),
					ExpressionAttributeNames: map[string]string{
						"#status": "status",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":open":   &types.AttributeValueMemberS{Value: string(poll.StatusOpen)},
						":closed": &types.AttributeValueMemberS{Value: string(poll.StatusClosed)},
					},
				},
			},
			{
				Put: &types.Put{
					TableName:           aws.String(s.outbox),
					Item:                av,
					ConditionExpression: aws.String("attribute_not_exists(eventId)"),
				},
			},
		},
	}); err != nil {
		if tce, ok := errors.AsType[*types.TransactionCanceledException](err); ok {
			if len(tce.CancellationReasons) > 0 &&
				aws.ToString(tce.CancellationReasons[0].Code) == "ConditionalCheckFailed" {
				return nil // already closed
			}
			return fmt.Errorf("close poll %s: %w", it.ID, err)
		}
		return fmt.Errorf("close poll %s: %w", it.ID, err)
	}

	return nil
}
