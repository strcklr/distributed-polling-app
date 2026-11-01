# Distributed Polling App

A polling service in Go, built to stay correct under failure: conditional state transitions, an outbox for email, and durable timers.

## Requirements

- Go 1.27+
- Docker, for the DynamoDB store's tests

## Setup

After cloning, enable the repo's git hooks:

```bash
git config core.hooksPath .githooks
```

The pre-commit hook runs `gofmt -w` on staged Go files and re-stages them. If a file that needs formatting also has unstaged changes, the commit stops and asks you to format and stage it yourself.

## Run

```bash
go run ./cmd/api
```

The server listens on `:8080` by default. Set `ADDR` to change it, for example `ADDR=127.0.0.1:9000`.

To build a binary instead:

```bash
go build -o bin/api ./cmd/api
./bin/api
```

## Test

```bash
go test ./...
```

Useful flags: `-v` for each test's name and output, `-run TestHealth` to run only the matching tests, and `-race` to run with the race detector. `-race` needs cgo, so on Windows it also needs a C compiler such as MinGW-w64 and `CGO_ENABLED=1`. On Linux and macOS it works out of the box.

The DynamoDB store's tests are skipped unless `DYNAMODB_ENDPOINT` is set. To run them against DynamoDB Local:

```bash
docker compose up -d
DYNAMODB_ENDPOINT=http://localhost:8000 go test ./...
```

On PowerShell, set the variable first with `$env:DYNAMODB_ENDPOINT = "http://localhost:8000"`. Each test creates its own table and deletes it afterwards. CI runs these tests against a DynamoDB Local service container.

Before committing:

```bash
gofmt -l .
go vet ./...
```

`gofmt -l` lists files that aren't formatted; empty output means everything is clean.

## Try it with HTTP

With the server running:

```bash
curl -i http://localhost:8080/health
```

Expected response:

```
HTTP/1.1 200 OK
Content-Type: application/json

{"status":"ok"}
```

Other HTTP methods return `405 Method Not Allowed`:

```bash
curl -i -X POST http://localhost:8080/health
```

On Windows PowerShell 5.1, `curl` is an alias for `Invoke-WebRequest`. Type `curl.exe` to get real curl.

## Layout

| Path | What's in it |
|---|---|
| `cmd/api` | Entry point for the HTTP server |
| `internal/api` | HTTP handlers and routing |
| `internal/poll` | Poll model, voting rules, `Store` interface and in-memory store |
| `internal/poll/storetest` | Test suite every `Store` implementation must pass |
| `internal/dynamo` | DynamoDB-backed `Store` |
| `docs/adr` | Architecture decision records |
