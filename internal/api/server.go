// Package api holds the HTTP layer of the polling service.
package api

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/strcklr/distributed-polling-app/internal/poll"
)

// Server is the HTTP API. It implements http.Handler.
type Server struct {
	store   poll.Store
	logger  *slog.Logger
	mux     *http.ServeMux
	handler http.Handler
}

// NewServer returns a Server with all routes registered and requests logged.
func NewServer(store poll.Store, logger *slog.Logger) *Server {
	s := &Server{store: store, logger: logger, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.handler = s.logRequests(s.mux)
	return s
}

// ServeHTTP routes the request to the matching handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, body any) {
	data, err := json.Marshal(body)
	if err != nil {
		s.logger.Error("marshal response", "err", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(data); err != nil {
		s.logger.Error("write response", "err", err)
	}
}
