# Distributed Polling App

A polling service in Go, built to stay correct under failure: conditional state transitions, an outbox for email, and durable timers.

## Requirements

- Go 1.27+

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
