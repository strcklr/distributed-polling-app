package dynamo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/strcklr/distributed-polling-app/internal/outbox"
)

var _ outbox.Source = (*Outbox)(nil)

var errNotImplemented = errors.New("not implemented")

// Outbox is an outbox.Source backed by the outbox table. A claim is a lease
// recorded on the event as leaseUntil and claimedBy, as in ADR 0002.
type Outbox struct {
	client *dynamodb.Client
	table  string
	now    func() time.Time
}

// NewOutbox returns an Outbox that reads and claims events in table.
func NewOutbox(client *dynamodb.Client, table string) *Outbox {
	return &Outbox{client: client, table: table, now: time.Now}
}

// Pending queries pendingIndex for up to limit events, oldest first.
func (o *Outbox) Pending(ctx context.Context, limit int) ([]outbox.Event, error) {
	result, err := o.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(o.table),
		IndexName:              aws.String(pendingIndex),
		KeyConditionExpression: aws.String("#pending = :pendingValue"),
		ExpressionAttributeNames: map[string]string{
			"#pending": "pending",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pendingValue": &types.AttributeValueMemberS{Value: pendingValue},
		},
		Limit: aws.Int32(int32(limit)),
	})

	if err != nil {
		return nil, fmt.Errorf("query pending events: %w", err)
	}

	var items []eventItem
	err = attributevalue.UnmarshalListOfMaps(result.Items, &items)
	if err != nil {
		return nil, fmt.Errorf("unmarshal pending events: %w", err)
	}

	var events []outbox.Event
	for _, it := range items {
		events = append(events, it.toEvent())
	}

	return events, nil
}

// Claim sets leaseUntil and claimedBy if the event is pending and has no
// lease, or its lease has expired.
func (o *Outbox) Claim(ctx context.Context, eventID, owner string, lease time.Duration) error {
	now := o.now()
	_, err := o.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(o.table),
		Key:              map[string]types.AttributeValue{"eventId": &types.AttributeValueMemberS{Value: eventID}},
		UpdateExpression: aws.String("SET leaseUntil = :until, claimedBy = :me"),
		ConditionExpression: aws.String(`
			attribute_exists(pending)
			AND (attribute_not_exists(leaseUntil) OR leaseUntil < :now)
		`),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":until": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", now.Add(lease).UnixMilli())},
			":me":    &types.AttributeValueMemberS{Value: owner},
			":now":   &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", now.UnixMilli())},
		},
	})
	if err != nil {
		if _, ok := errors.AsType[*types.ConditionalCheckFailedException](err); ok {
			return outbox.ErrNotAvailable
		}
		return fmt.Errorf("claim event: %w", err)
	}
	return nil
}

// Complete removes pending, leaseUntil and claimedBy if owner holds the
// claim, which takes the event out of pendingIndex. The event stays in the
// table as a record.
func (o *Outbox) Complete(ctx context.Context, eventID, owner string) error {
	_, err := o.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(o.table),
		Key:                 map[string]types.AttributeValue{"eventId": &types.AttributeValueMemberS{Value: eventID}},
		UpdateExpression:    aws.String("REMOVE pending, leaseUntil, claimedBy"),
		ConditionExpression: aws.String("#claimedBy = :me"),
		ExpressionAttributeNames: map[string]string{
			"#claimedBy": "claimedBy",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":me": &types.AttributeValueMemberS{Value: owner},
		},
	})
	if err != nil {
		if _, ok := errors.AsType[*types.ConditionalCheckFailedException](err); ok {
			return outbox.ErrLeaseLost
		}
		return fmt.Errorf("complete event: %w", err)
	}

	return nil
}
