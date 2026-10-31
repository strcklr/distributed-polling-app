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

const maxBodyBytes = 1 << 20

func (s *Server) handleCreatePoll(w http.ResponseWriter, r *http.Request) {
	var req createPollRequest
	if err := s.decodeRequest(w, r, &req); err != nil {
		return
	}

	p, err := poll.New(req.Question, req.Options, req.Participants)
	if err != nil {
		s.writeError(w, errorStatus(err), err.Error())
		return
	}

	if err := s.store.Create(r.Context(), p); err != nil {
		s.writeStoreError(w, err, "error creating poll", p.ID)
		return
	}

	w.Header().Set("Location", "/polls/"+p.ID)
	s.writeJSON(w, http.StatusCreated, p)
}

func (s *Server) handleGetPoll(w http.ResponseWriter, r *http.Request) {
	pollID := r.PathValue("id")
	p, err := s.store.Get(r.Context(), pollID)
	if err != nil {
		s.writeStoreError(w, err, "error fetching poll", pollID)
		return
	}
	s.writeJSON(w, http.StatusOK, p)
}

func (s *Server) handleCastVote(w http.ResponseWriter, r *http.Request) {
	var req castVoteRequest
	if err := s.decodeRequest(w, r, &req); err != nil {
		return
	}
	if req.Option == nil {
		s.writeError(w, http.StatusBadRequest, "option is required")
		return
	}

	pollID := r.PathValue("id")

	p, err := s.store.CastVote(r.Context(), pollID, req.Participant, *req.Option)
	if err != nil {
		s.writeStoreError(w, err, "error casting vote", pollID)
		return
	}

	s.writeJSON(w, http.StatusOK, p)
}

func (s *Server) writeStoreError(w http.ResponseWriter, err error, msg string, pollID string) {
	status := errorStatus(err)
	if status == http.StatusInternalServerError {
		s.logger.Error(msg,
			slog.Any("err", err),
			slog.String("pollID", pollID),
		)
		s.writeError(w, http.StatusInternalServerError, msg)
		return
	}
	s.writeError(w, status, err.Error())
}

func (s *Server) decodeRequest(w http.ResponseWriter, r *http.Request, req any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)

	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			s.writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return err
		}

		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return err
	}

	return nil
}

func (s *Server) writeError(w http.ResponseWriter, status int, message string) {
	s.writeJSON(w, status, map[string]string{"error": message})
}

func errorStatus(err error) int {
	switch {
	case errors.Is(err, poll.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, poll.ErrNotParticipant):
		return http.StatusForbidden
	case errors.Is(err, poll.ErrInvalidOption), errors.Is(err, poll.ErrInvalidPoll):
		return http.StatusBadRequest
	case errors.Is(err, poll.ErrAlreadyVoted), errors.Is(err, poll.ErrClosed):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}
