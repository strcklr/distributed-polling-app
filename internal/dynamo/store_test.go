package dynamo

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
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

func newTestStore(t *testing.T) poll.Store {
	return newStore(t)
}

// newStore creates a table with a unique name, so every test starts empty,
// and deletes it when the test ends.
func newStore(t *testing.T) *Store {
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

func TestStoreCastVote(t *testing.T) {
	storetest.RunCastVote(t, newTestStore)
}

// The tests below read the stored status directly, because the store reports
// a fully voted poll as closed whatever the stored status says.

func TestStoreLastVoteStoresClosed(t *testing.T) {
	s := newStore(t)
	p := createPoll(t, s, "alice", "bob", "carol")

	for _, participant := range []string{"alice", "bob"} {
		if _, err := s.CastVote(t.Context(), p.ID, participant, 0); err != nil {
			t.Fatalf("CastVote(%s) error = %v", participant, err)
		}
	}
	if got := storedStatus(t, s, p.ID); got != poll.StatusOpen {
		t.Errorf("stored status before the last vote = %s, want %s", got, poll.StatusOpen)
	}

	if _, err := s.CastVote(t.Context(), p.ID, "carol", 1); err != nil {
		t.Fatalf("CastVote(carol) error = %v", err)
	}
	if got := storedStatus(t, s, p.ID); got != poll.StatusClosed {
		t.Errorf("stored status after the last vote = %s, want %s", got, poll.StatusClosed)
	}
}

func TestStoreConcurrentLastVotesStoreClosed(t *testing.T) {
	const n = 50
	participants := make([]string, n)
	for i := range participants {
		participants[i] = fmt.Sprintf("voter-%d", i)
	}
	s := newStore(t)
	p := createPoll(t, s, participants...)

	var wg sync.WaitGroup
	for _, participant := range participants {
		wg.Go(func() {
			if _, err := s.CastVote(t.Context(), p.ID, participant, 0); err != nil {
				t.Errorf("CastVote(%s) error = %v", participant, err)
			}
		})
	}
	wg.Wait()

	if got := storedStatus(t, s, p.ID); got != poll.StatusClosed {
		t.Errorf("stored status = %s, want %s", got, poll.StatusClosed)
	}
}

func TestStoreClosePollIsIdempotent(t *testing.T) {
	s := newStore(t)
	p := createPoll(t, s, "alice")

	for i := range 2 {
		if err := s.closePoll(t.Context(), p.ID); err != nil {
			t.Fatalf("closePoll() call %d error = %v", i+1, err)
		}
	}
	if got := storedStatus(t, s, p.ID); got != poll.StatusClosed {
		t.Errorf("stored status = %s, want %s", got, poll.StatusClosed)
	}
}

func createPoll(t *testing.T, s *Store, participants ...string) poll.Poll {
	t.Helper()
	p, err := poll.New("Lunch?", []string{"Tacos", "Pizza"}, participants)
	if err != nil {
		t.Fatalf("poll.New() error = %v", err)
	}
	if err := s.Create(t.Context(), p); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	return p
}

func storedStatus(t *testing.T, s *Store, id string) poll.Status {
	t.Helper()
	out, err := s.client.GetItem(t.Context(), &dynamodb.GetItemInput{
		TableName:      aws.String(s.table),
		Key:            map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: id}},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		t.Fatalf("GetItem(%s) error = %v", id, err)
	}
	var it pollItem
	if err := attributevalue.UnmarshalMap(out.Item, &it); err != nil {
		t.Fatalf("unmarshal %s: %v", id, err)
	}
	return poll.Status(it.Status)
}
