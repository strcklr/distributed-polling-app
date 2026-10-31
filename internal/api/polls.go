package api

import (
	"net/http"
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

// TODO(#5): decode createPollRequest, build the poll with poll.New, store it,
// and respond 201 with the poll and a Location header.
func (s *Server) handleCreatePoll(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
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
