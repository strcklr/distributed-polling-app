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

	table, outbox := os.Getenv("POLLS_TABLE"), os.Getenv("OUTBOX_TABLE")
	if table == "" || outbox == "" {
		logger.Error("POLLS_TABLE and OUTBOX_TABLE must be set")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := dynamo.NewClient(ctx, os.Getenv("DYNAMODB_ENDPOINT"))
	if err != nil {
		logger.Error("connecting to DynamoDB", "err", err)
		os.Exit(1)
	}
	for _, t := range []string{table, outbox} {
		if err := dynamo.CheckTable(ctx, client, t); err != nil {
			logger.Error("checking table", "err", err)
			os.Exit(1)
		}
	}

	server := api.NewServer(dynamo.NewStore(client, table, outbox), logger)

	logger.Info("listening", "addr", addr, "table", table, "outbox", outbox)
	if err := http.ListenAndServe(addr, server); err != nil {
		logger.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
