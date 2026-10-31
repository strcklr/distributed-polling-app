package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/strcklr/distributed-polling-app/internal/poll"
)

type createPollRequest struct {
	Question     string   `json:"question"`
	Options      []string `json:"options"`
	Participants []string `json:"participants"`
}

type castVoteRequest struct {
	Participant string `json:"participant"`
	Option      *int   `json:"option"`
}

const maxBodyBytes = 1 << 20 // 1MiB

func (s *Server) handleCreatePoll(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	var req createPollRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var maxErr *http.MaxBytesError

		if errors.As(err, &maxErr) {
			s.writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}

		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	p, err := poll.New(req.Question, req.Options, req.Participants)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.store.Create(r.Context(), p); err != nil {
		s.logger.Error("create poll",
			slog.Any("err", err),
			slog.String("pollID", p.ID),
		)
		s.writeError(w, http.StatusInternalServerError, http.StatusText(http.StatusInternalServerError))
		return
	}

	w.Header().Set("Location", "/polls/"+p.ID)
	s.writeJSON(w, http.StatusCreated, p)
}

// TODO(#6): respond 200 with the poll for the {id} path value.
func (s *Server) handleGetPoll(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}

// TODO(#7): decode castVoteRequest and cast the vote through Store.CastVote only.
// Respond 200 with the updated poll.
func (s *Server) handleCastVote(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}

func (s *Server) writeError(w http.ResponseWriter, status int, message string) {
	s.writeJSON(w, status, map[string]string{"error": message})
}

// TODO(#7): map poll's sentinel errors to HTTP status codes.
func errorStatus(err error) int {
	return http.StatusInternalServerError
}
