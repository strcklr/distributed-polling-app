package api

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"
)

type httpStatusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *httpStatusRecorder) Write(b []byte) (int, error) {
	r.wroteHeader = true
	return r.ResponseWriter.Write(b)
}

func (r *httpStatusRecorder) WriteHeader(statusCode int) {
	// Ensures we only capture the first call to WriteHeader, which is what is actually returned to the client
	if !r.wroteHeader {
		r.status = statusCode
		r.wroteHeader = true
	}

	r.ResponseWriter.WriteHeader(statusCode)
}

func (r *httpStatusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		statusRecorder := &httpStatusRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(statusRecorder, r)

		logLevel := slog.LevelInfo
		switch {
		case statusRecorder.status >= 500:
			logLevel = slog.LevelError
		case statusRecorder.status >= 400:
			logLevel = slog.LevelWarn
		}

		s.logger.Log(r.Context(), logLevel, "request",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", statusRecorder.status),
			slog.Duration("duration", time.Since(start)),
		)
	})
}

func (s *Server) recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			rec := recover()
			if rec == nil {
				return
			}
			if rec == http.ErrAbortHandler {
				panic(rec)
			}
			s.logger.Error("panic", slog.Any("panic", rec), slog.String("stack", string(debug.Stack())))
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		}()
		next.ServeHTTP(w, r)
	})
}
