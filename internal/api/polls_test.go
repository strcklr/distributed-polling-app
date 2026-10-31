package api

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/strcklr/distributed-polling-app/internal/poll"
)

func createStoredPoll(t *testing.T, store *poll.MemoryStore) poll.Poll {
	t.Helper()
	p, err := poll.New("Lunch?", []string{"Tacos", "Pizza"}, []string{"alice", "bob"})
	if err != nil {
		t.Fatalf("poll.New() error = %v", err)
	}
	if err := store.Create(t.Context(), p); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	return p
}

func assertJSONContentType(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want %q", got, "application/json")
	}
}

func assertErrorResponse(t *testing.T, recorder *httptest.ResponseRecorder, wantStatus int) {
	t.Helper()
	if recorder.Code != wantStatus {
		t.Fatalf("status = %d, want %d (body %q)", recorder.Code, wantStatus, recorder.Body.String())
	}
	assertJSONContentType(t, recorder)
	if body := decodeBody[map[string]string](t, recorder); body["error"] == "" {
		t.Errorf(`body has no "error" message: %v`, body)
	}
}

func TestCreatePoll(t *testing.T) {
	s, store := newTestServer(t)

	recorder := doRequest(t, s, http.MethodPost, "/polls",
		`{"question": "Lunch?", "options": ["Tacos", "Pizza"], "participants": ["alice", "bob"]}`)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d (body %q)", recorder.Code, http.StatusCreated, recorder.Body.String())
	}
	assertJSONContentType(t, recorder)
	location := recorder.Header().Get("Location")

	got := decodeBody[poll.Poll](t, recorder)
	if got.ID == "" {
		t.Fatal("response poll has no ID")
	}
	if want := "/polls/" + got.ID; location != want {
		t.Errorf("Location = %q, want %q", location, want)
	}
	if got.Question != "Lunch?" || got.Status != poll.StatusOpen || !slices.Equal(got.Counts, []int{0, 0}) {
		t.Errorf("response poll = %+v, want an open poll for %q with no votes", got, "Lunch?")
	}
	if _, err := store.Get(t.Context(), got.ID); err != nil {
		t.Errorf("created poll is not in the store: %v", err)
	}
}

func TestCreatePollErrors(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "empty body", body: ""},
		{name: "malformed JSON", body: `{"question": "Lunch?"`},
		{name: "wrong field type", body: `{"question": 5, "options": ["Tacos", "Pizza"], "participants": ["alice"]}`},
		{name: "missing question", body: `{"options": ["Tacos", "Pizza"], "participants": ["alice"]}`},
		{name: "one option", body: `{"question": "Lunch?", "options": ["Tacos"], "participants": ["alice"]}`},
		{name: "no participants", body: `{"question": "Lunch?", "options": ["Tacos", "Pizza"]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestServer(t)
			recorder := doRequest(t, s, http.MethodPost, "/polls", tt.body)
			assertErrorResponse(t, recorder, http.StatusBadRequest)
		})
	}
}

func TestCreatePollBodyTooLarge(t *testing.T) {
	s, _ := newTestServer(t)
	body := `{"question": "` + strings.Repeat("a", maxBodyBytes) + `", "options": ["Tacos", "Pizza"], "participants": ["alice"]}`

	recorder := doRequest(t, s, http.MethodPost, "/polls", body)

	assertErrorResponse(t, recorder, http.StatusRequestEntityTooLarge)
}

func TestGetPoll(t *testing.T) {
	s, store := newTestServer(t)
	p := createStoredPoll(t, store)

	recorder := doRequest(t, s, http.MethodGet, "/polls/"+p.ID, "")

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body %q)", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	assertJSONContentType(t, recorder)
	got := decodeBody[poll.Poll](t, recorder)
	if got.ID != p.ID || got.Question != p.Question || got.Status != p.Status ||
		!slices.Equal(got.Options, p.Options) || !slices.Equal(got.Counts, p.Counts) {
		t.Errorf("GET poll = %+v, want %+v", got, p)
	}
}

func TestGetPollNotFound(t *testing.T) {
	s, _ := newTestServer(t)
	recorder := doRequest(t, s, http.MethodGet, "/polls/missing", "")
	assertErrorResponse(t, recorder, http.StatusNotFound)
}

func TestStoreErrorsReturn500(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{name: "create poll", method: http.MethodPost, path: "/polls", body: `{"question": "Lunch?", "options": ["Tacos", "Pizza"], "participants": ["alice"]}`},
		{name: "get poll", method: http.MethodGet, path: "/polls/abc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storeErr := errors.New("store is down")
			var buf bytes.Buffer
			s := NewServer(failingStore{err: storeErr}, slog.New(slog.NewJSONHandler(&buf, nil)))

			recorder := doRequest(t, s, tt.method, tt.path, tt.body)

			assertErrorResponse(t, recorder, http.StatusInternalServerError)
			if body := recorder.Body.String(); strings.Contains(body, storeErr.Error()) {
				t.Errorf("response %q leaks the internal error", body)
			}
			logged := slices.ContainsFunc(logLines(t, &buf), func(entry map[string]any) bool {
				return entry["level"] == "ERROR" && entry["err"] == storeErr.Error()
			})
			if !logged {
				t.Errorf("store error was not logged at ERROR: %v", logLines(t, &buf))
			}
		})
	}
}

func TestCastVote(t *testing.T) {
	s, store := newTestServer(t)
	p := createStoredPoll(t, store)

	recorder := doRequest(t, s, http.MethodPost, "/polls/"+p.ID+"/votes", `{"participant": "alice", "option": 1}`)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body %q)", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	assertJSONContentType(t, recorder)
	got := decodeBody[poll.Poll](t, recorder)
	if !slices.Equal(got.Counts, []int{0, 1}) || got.Status != poll.StatusOpen {
		t.Errorf("response poll = %+v, want Counts [0 1] and status OPEN", got)
	}

	stored, err := store.Get(t.Context(), p.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !slices.Equal(stored.Counts, []int{0, 1}) {
		t.Errorf("stored Counts = %v, want [0 1]", stored.Counts)
	}
}

func TestCastVoteLastVoteClosesPoll(t *testing.T) {
	s, store := newTestServer(t)
	p := createStoredPoll(t, store)
	if _, err := store.CastVote(t.Context(), p.ID, "alice", 0); err != nil {
		t.Fatalf("CastVote() error = %v", err)
	}

	recorder := doRequest(t, s, http.MethodPost, "/polls/"+p.ID+"/votes", `{"participant": "bob", "option": 0}`)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body %q)", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if got := decodeBody[poll.Poll](t, recorder); got.Status != poll.StatusClosed {
		t.Errorf("Status = %s, want %s", got.Status, poll.StatusClosed)
	}
}

func TestCastVoteErrors(t *testing.T) {
	tests := []struct {
		name       string
		missing    bool
		priorVotes []string
		body       string
		wantStatus int
	}{
		{name: "poll not found", missing: true, body: `{"participant": "alice", "option": 0}`, wantStatus: http.StatusNotFound},
		{name: "not a participant", body: `{"participant": "mallory", "option": 0}`, wantStatus: http.StatusForbidden},
		{name: "option out of range", body: `{"participant": "alice", "option": 2}`, wantStatus: http.StatusBadRequest},
		{name: "option missing", body: `{"participant": "alice"}`, wantStatus: http.StatusBadRequest},
		{name: "malformed JSON", body: `{"participant": `, wantStatus: http.StatusBadRequest},
		{name: "empty body", body: "", wantStatus: http.StatusBadRequest},
		{name: "already voted", priorVotes: []string{"alice"}, body: `{"participant": "alice", "option": 1}`, wantStatus: http.StatusConflict},
		{name: "poll closed", priorVotes: []string{"alice", "bob"}, body: `{"participant": "bob", "option": 1}`, wantStatus: http.StatusConflict},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, store := newTestServer(t)
			p := createStoredPoll(t, store)
			for _, participant := range tt.priorVotes {
				if _, err := store.CastVote(t.Context(), p.ID, participant, 0); err != nil {
					t.Fatalf("prior CastVote(%s) error = %v", participant, err)
				}
			}
			before, err := store.Get(t.Context(), p.ID)
			if err != nil {
				t.Fatalf("Get() error = %v", err)
			}

			pollID := p.ID
			if tt.missing {
				pollID = "missing"
			}
			recorder := doRequest(t, s, http.MethodPost, "/polls/"+pollID+"/votes", tt.body)
			assertErrorResponse(t, recorder, tt.wantStatus)

			after, err := store.Get(t.Context(), p.ID)
			if err != nil {
				t.Fatalf("Get() error = %v", err)
			}
			if !slices.Equal(after.Counts, before.Counts) || after.Status != before.Status {
				t.Errorf("rejected vote changed the poll: Counts %v -> %v, Status %s -> %s",
					before.Counts, after.Counts, before.Status, after.Status)
			}
		})
	}
}

func TestErrorStatus(t *testing.T) {
	tests := []struct {
		err  error
		want int
	}{
		{err: poll.ErrInvalidPoll, want: http.StatusBadRequest},
		{err: fmt.Errorf("%w: question is required", poll.ErrInvalidPoll), want: http.StatusBadRequest},
		{err: poll.ErrInvalidOption, want: http.StatusBadRequest},
		{err: poll.ErrNotParticipant, want: http.StatusForbidden},
		{err: poll.ErrNotFound, want: http.StatusNotFound},
		{err: poll.ErrAlreadyVoted, want: http.StatusConflict},
		{err: poll.ErrClosed, want: http.StatusConflict},
		{err: errors.New("database is on fire"), want: http.StatusInternalServerError},
	}

	for _, tt := range tests {
		t.Run(tt.err.Error(), func(t *testing.T) {
			if got := errorStatus(tt.err); got != tt.want {
				t.Errorf("errorStatus(%v) = %d, want %d", tt.err, got, tt.want)
			}
		})
	}
}
