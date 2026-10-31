package api

import (
	"net/http"
)

// TODO(#4): log one "request" line per request at Info level with the
// method, path, status and duration attributes.
func (s *Server) logRequests(next http.Handler) http.Handler {
	return next
}
