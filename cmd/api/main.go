// Command api runs the polling HTTP server.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/strcklr/distributed-polling-app/internal/api"
	"github.com/strcklr/distributed-polling-app/internal/dynamo"
)

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	table := os.Getenv("POLLS_TABLE")
	if table == "" {
		logger.Error("POLLS_TABLE not set")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := dynamo.NewClient(ctx, os.Getenv("DYNAMODB_ENDPOINT"))
	if err != nil {
		logger.Error("connecting to DynamoDB", "err", err)
		os.Exit(1)
	}
	if err := dynamo.CheckTable(ctx, client, table); err != nil {
		logger.Error("checking polls table", "err", err)
		os.Exit(1)
	}

	server := api.NewServer(dynamo.NewStore(client, table), logger)

	logger.Info("listening", "addr", addr, "table", table)
	if err := http.ListenAndServe(addr, server); err != nil {
		logger.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
