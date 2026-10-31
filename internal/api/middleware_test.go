package api

import (
	"bytes"
	"encoding/json"
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

func assertRequestLogged(t *testing.T, buf *bytes.Buffer, method, path string, status int) {
	t.Helper()
	lines := logLines(t, buf)
	if len(lines) != 1 {
		t.Fatalf("got %d log lines, want 1: %v", len(lines), lines)
	}
	entry := lines[0]

	if entry["msg"] != "request" || entry["level"] != "INFO" {
		t.Errorf("msg = %v, level = %v, want msg %q at level INFO", entry["msg"], entry["level"], "request")
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
			name:       "implicit 200 from Write",
			handler:    func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("hello")) },
			wantStatus: http.StatusOK,
		},
		{
			name:       "implicit 200 with no write",
			handler:    func(w http.ResponseWriter, r *http.Request) {},
			wantStatus: http.StatusOK,
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
