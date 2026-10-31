package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/strcklr/distributed-polling-app/internal/poll"
)

func newLoggedServer(t *testing.T) (*Server, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	return NewServer(poll.NewMemoryStore(), logger), &buf
}

func logLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("log line %q is not JSON: %v", line, err)
		}
		lines = append(lines, entry)
	}
	return lines
}

func logLinesWithMsg(t *testing.T, buf *bytes.Buffer, msg string) []map[string]any {
	t.Helper()
	var matched []map[string]any
	for _, entry := range logLines(t, buf) {
		if entry["msg"] == msg {
			matched = append(matched, entry)
		}
	}
	return matched
}

func assertRequestLogged(t *testing.T, buf *bytes.Buffer, method, path string, status int) {
	t.Helper()
	lines := logLinesWithMsg(t, buf, "request")
	if len(lines) != 1 {
		t.Fatalf("got %d request log lines, want 1: %v", len(lines), logLines(t, buf))
	}
	entry := lines[0]

	wantLevel := "INFO"
	if status >= 500 {
		wantLevel = "ERROR"
	}
	if entry["level"] != wantLevel {
		t.Errorf("level = %v, want %s for status %d", entry["level"], wantLevel, status)
	}
	if entry["method"] != method || entry["path"] != path {
		t.Errorf("method = %v, path = %v, want %s %s", entry["method"], entry["path"], method, path)
	}
	if got, ok := entry["status"].(float64); !ok || int(got) != status {
		t.Errorf("status = %v, want %d", entry["status"], status)
	}
	if got, ok := entry["duration"].(float64); !ok || got < 0 {
		t.Errorf("duration = %v, want a non-negative number", entry["duration"])
	}
}

func TestLogRequestsRecordsStatus(t *testing.T) {
	tests := []struct {
		name       string
		handler    http.HandlerFunc
		wantStatus int
	}{
		{
			name:       "explicit status",
			handler:    func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusCreated) },
			wantStatus: http.StatusCreated,
		},
		{
			name: "status from http.Error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "nope", http.StatusTeapot)
			},
			wantStatus: http.StatusTeapot,
		},
		{
			name: "server error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "down", http.StatusServiceUnavailable)
			},
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:       "implicit 200 from Write",
			handler:    func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("hello")) },
			wantStatus: http.StatusOK,
		},
		{
			name:       "implicit 200 with no write",
			handler:    func(w http.ResponseWriter, r *http.Request) {},
			wantStatus: http.StatusOK,
		},
		{
			name: "WriteHeader after Write keeps the implicit 200",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte("hello"))
				w.WriteHeader(http.StatusInternalServerError)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "second WriteHeader keeps the first status",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusCreated)
				w.WriteHeader(http.StatusInternalServerError)
			},
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, buf := newLoggedServer(t)
			recorder := httptest.NewRecorder()

			s.logRequests(tt.handler).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/things?debug=1", nil))

			if recorder.Code != tt.wantStatus {
				t.Errorf("response status = %d, want %d", recorder.Code, tt.wantStatus)
			}
			assertRequestLogged(t, buf, http.MethodPost, "/things", tt.wantStatus)
		})
	}
}

func TestLogRequestsPassesBodyThrough(t *testing.T) {
	s, _ := newLoggedServer(t)
	handler := func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("hello")) }
	recorder := httptest.NewRecorder()

	s.logRequests(http.HandlerFunc(handler)).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))

	if got := recorder.Body.String(); got != "hello" {
		t.Errorf("body = %q, want %q", got, "hello")
	}
}

func TestServerLogsEveryRequest(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{name: "matched route", path: "/health", wantStatus: http.StatusOK},
		{name: "unknown route", path: "/nope", wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, buf := newLoggedServer(t)
			doRequest(t, s, http.MethodGet, tt.path, "")
			assertRequestLogged(t, buf, http.MethodGet, tt.path, tt.wantStatus)
		})
	}
}

func TestServerRecoversPanics(t *testing.T) {
	tests := []struct {
		name       string
		value      any
		wantLogged string
	}{
		{name: "string panic", value: "boom", wantLogged: "boom"},
		{name: "error panic", value: errors.New("store exploded"), wantLogged: "store exploded"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, buf := newLoggedServer(t)
			s.mux.HandleFunc("GET /panic", func(w http.ResponseWriter, r *http.Request) { panic(tt.value) })

			recorder := doRequest(t, s, http.MethodGet, "/panic", "")

			if recorder.Code != http.StatusInternalServerError {
				t.Errorf("response status = %d, want %d", recorder.Code, http.StatusInternalServerError)
			}
			assertRequestLogged(t, buf, http.MethodGet, "/panic", http.StatusInternalServerError)

			panics := logLinesWithMsg(t, buf, "panic")
			if len(panics) != 1 {
				t.Fatalf("got %d panic log lines, want 1: %v", len(panics), logLines(t, buf))
			}
			entry := panics[0]
			if entry["level"] != "ERROR" {
				t.Errorf("panic level = %v, want ERROR", entry["level"])
			}
			if got, _ := entry["panic"].(string); !strings.Contains(got, tt.wantLogged) {
				t.Errorf("panic = %v, want it to contain %q", entry["panic"], tt.wantLogged)
			}
			if stack, _ := entry["stack"].(string); !strings.Contains(stack, "goroutine") {
				t.Errorf("stack = %q, want a goroutine stack trace", stack)
			}
		})
	}
}

func TestServerKeepsStatusSentBeforePanic(t *testing.T) {
	s, buf := newLoggedServer(t)
	s.mux.HandleFunc("GET /panic", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		panic("boom")
	})

	recorder := doRequest(t, s, http.MethodGet, "/panic", "")

	if recorder.Code != http.StatusAccepted {
		t.Errorf("response status = %d, want %d", recorder.Code, http.StatusAccepted)
	}
	assertRequestLogged(t, buf, http.MethodGet, "/panic", http.StatusAccepted)
	if panics := logLinesWithMsg(t, buf, "panic"); len(panics) != 1 {
		t.Errorf("got %d panic log lines, want 1", len(panics))
	}
}

func TestRecoverPanicsLetsErrAbortHandlerThrough(t *testing.T) {
	s, buf := newLoggedServer(t)
	handler := s.recoverPanics(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic(http.ErrAbortHandler)
	}))

	defer func() {
		if rec := recover(); rec != http.ErrAbortHandler {
			t.Errorf("recovered %v, want http.ErrAbortHandler to be re-panicked", rec)
		}
		if panics := logLinesWithMsg(t, buf, "panic"); len(panics) != 0 {
			t.Errorf("ErrAbortHandler was logged as a panic: %v", panics)
		}
	}()
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
}
