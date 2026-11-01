package dynamo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// NewClient returns a DynamoDB client. With an empty endpoint it uses the
// default AWS configuration chain. Otherwise it connects to endpoint, such as
// DynamoDB Local, with placeholder credentials and region.
func NewClient(ctx context.Context, endpoint string) (*dynamodb.Client, error) {
	if endpoint == "" {
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("load AWS config: %w", err)
		}
		return dynamodb.NewFromConfig(cfg), nil
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")),
	)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		// DynamoDB Local's error responses fail the SDK's CRC32 check, which
		// logs a warning on every rejected request.
		o.DisableValidateResponseChecksum = true
	}), nil
}

// CreateTable creates table with the key schema from ADR 0001 and waits until
// it's active. It does nothing if the table already exists.
func CreateTable(ctx context.Context, client *dynamodb.Client, table string) error {
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String(table),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if _, ok := errors.AsType[*types.ResourceInUseException](err); ok {
		return nil
	}
	if err != nil {
		return fmt.Errorf("create table %s: %w", table, err)
	}

	waiter := dynamodb.NewTableExistsWaiter(client)
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)}, time.Minute); err != nil {
		return fmt.Errorf("wait for table %s: %w", table, err)
	}
	return nil
}

// CheckTable returns an error if table doesn't exist or can't be reached.
func CheckTable(ctx context.Context, client *dynamodb.Client, table string) error {
	if _, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)}); err != nil {
		return fmt.Errorf("describe table %s: %w", table, err)
	}
	return nil
}
