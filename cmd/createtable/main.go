// Command createtable creates the polls table named by $POLLS_TABLE, for
// local development against DynamoDB Local at $DYNAMODB_ENDPOINT.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/strcklr/distributed-polling-app/internal/dynamo"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	table := os.Getenv("POLLS_TABLE")
	if table == "" {
		logger.Error("POLLS_TABLE not set")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client, err := dynamo.NewClient(ctx, os.Getenv("DYNAMODB_ENDPOINT"))
	if err != nil {
		logger.Error("connecting to DynamoDB", "err", err)
		os.Exit(1)
	}
	if err := dynamo.CreateTable(ctx, client, table); err != nil {
		logger.Error("creating polls table", "err", err)
		os.Exit(1)
	}
	logger.Info("table ready", "table", table)
}
