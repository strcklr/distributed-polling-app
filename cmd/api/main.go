// Command api runs the polling HTTP server.
package main

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/strcklr/distributed-polling-app/internal/api"
	"github.com/strcklr/distributed-polling-app/internal/poll"
)

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	server := api.NewServer(poll.NewMemoryStore(), logger)

	logger.Info("listening", "addr", addr)
	if err := http.ListenAndServe(addr, server); err != nil {
		logger.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
