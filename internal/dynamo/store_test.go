package dynamo

import (
	"context"
	"crypto/rand"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/strcklr/distributed-polling-app/internal/poll"
	"github.com/strcklr/distributed-polling-app/internal/poll/storetest"
)

// newLocalClient connects to DynamoDB Local at $DYNAMODB_ENDPOINT,
// skipping the test when it isn't set.
func newLocalClient(t *testing.T) *dynamodb.Client {
	t.Helper()
	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
	if endpoint == "" {
		t.Skip("DYNAMODB_ENDPOINT not set; run `docker compose up -d` and set it to http://localhost:8000")
	}
	cfg, err := config.LoadDefaultConfig(t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")),
	)
	if err != nil {
		t.Fatalf("load AWS config: %v", err)
	}
	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// newTestStore creates a table with a unique name, so every test starts empty,
// and deletes it when the test ends.
func newTestStore(t *testing.T) poll.Store {
	t.Helper()
	client := newLocalClient(t)
	table := "polls-test-" + rand.Text()

	_, err := client.CreateTable(t.Context(), &dynamodb.CreateTableInput{
		TableName:            aws.String(table),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("create table %s: %v", table, err)
	}
	t.Cleanup(func() {
		// t.Context() is already canceled when cleanup runs.
		if _, err := client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(table)}); err != nil {
			t.Errorf("delete table %s: %v", table, err)
		}
	})
	waiter := dynamodb.NewTableExistsWaiter(client)
	if err := waiter.Wait(t.Context(), &dynamodb.DescribeTableInput{TableName: aws.String(table)}, time.Minute); err != nil {
		t.Fatalf("wait for table %s: %v", table, err)
	}

	return NewStore(client, table)
}

func TestStoreCreateGet(t *testing.T) {
	storetest.RunCreateGet(t, newTestStore)
}
