package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/strcklr/distributed-polling-app/internal/poll"
)

func newTestServer(t *testing.T) (*Server, *poll.MemoryStore) {
	t.Helper()
	store := poll.NewMemoryStore()
	return NewServer(store, slog.New(slog.DiscardHandler)), store
}

func doRequest(t *testing.T, s *Server, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	recorder := httptest.NewRecorder()
	s.ServeHTTP(recorder, httptest.NewRequest(method, path, r))
	return recorder
}

func decodeBody[T any](t *testing.T, recorder *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(recorder.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode body %q: %v", recorder.Body.String(), err)
	}
	return v
}

type failingStore struct {
	err error
}

func (f failingStore) Create(ctx context.Context, p poll.Poll) error {
	return f.err
}

func (f failingStore) Get(ctx context.Context, id string) (poll.Poll, error) {
	return poll.Poll{}, f.err
}

func (f failingStore) CastVote(ctx context.Context, pollID, participant string, option int) (poll.Poll, error) {
	return poll.Poll{}, f.err
}

func TestHealth(t *testing.T) {
	s, _ := newTestServer(t)
	recorder := doRequest(t, s, http.MethodGet, "/health", "")
	if recorder.Code != http.StatusOK {
		t.Errorf("status = %d, want = %d", recorder.Code, http.StatusOK)
	}

	got := decodeBody[map[string]string](t, recorder)
	if status := got["status"]; status != "ok" {
		t.Errorf(`status = %q, want "ok"`, status)
	}
}

func TestWriteJSONMarshalError(t *testing.T) {
	s, _ := newTestServer(t)
	recorder := httptest.NewRecorder()
	s.writeJSON(recorder, http.StatusOK, make(chan int))
	if recorder.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", recorder.Code, http.StatusInternalServerError)
	}
}
