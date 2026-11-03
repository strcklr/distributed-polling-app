// Command worker delivers poll events from the outbox to their recipients.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/strcklr/distributed-polling-app/internal/dynamo"
	"github.com/strcklr/distributed-polling-app/internal/email"
	"github.com/strcklr/distributed-polling-app/internal/outbox"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	table := os.Getenv("OUTBOX_TABLE")
	if table == "" {
		logger.Error("OUTBOX_TABLE must be set")
		os.Exit(1)
	}
	id := os.Getenv("WORKER_ID")
	if id == "" {
		host, err := os.Hostname()
		if err != nil {
			host = "worker"
		}
		id = fmt.Sprintf("%s-%d", host, os.Getpid())
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	setupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	client, err := dynamo.NewClient(setupCtx, os.Getenv("DYNAMODB_ENDPOINT"))
	if err != nil {
		logger.Error("connecting to DynamoDB", "err", err)
		os.Exit(1)
	}
	if err := dynamo.CheckTable(setupCtx, client, table); err != nil {
		logger.Error("checking table", "err", err)
		os.Exit(1)
	}

	var sender outbox.Sender = outbox.LogSender{Logger: logger}
	if addr := os.Getenv("SMTP_ADDR"); addr != "" {
		from := os.Getenv("MAIL_FROM")
		if from == "" {
			from = "polls@localhost"
		}
		sender = email.SMTPSender{Addr: addr, From: from}
	}
	sender = outbox.NewRetrySender(sender, outbox.RetryConfig{
		MaxAttempts: 4,
		BaseDelay:   200 * time.Millisecond,
		MaxDelay:    5 * time.Second,
	})

	w := outbox.New(dynamo.NewOutbox(client, table), sender, logger, outbox.Config{
		ID:           id,
		PollInterval: 2 * time.Second,
		Lease:        2 * time.Minute,
		BatchSize:    25,
		Events:       4,
		Sends:        8,
	})

	logger.Info("worker started", "id", id, "outbox", table)
	if err := w.Run(ctx); err != nil {
		logger.Error("worker stopped", "err", err)
		os.Exit(1)
	}
	logger.Info("worker stopped")
}
