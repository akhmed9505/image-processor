package http

import (
	"log/slog"
	"net/http"
	"time"
)

type responseWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (w *responseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *responseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	if status >= 200 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}
func (w *responseWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(data)
	w.bytes += int64(n)
	return n, err
}

func middleware(next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state := &responseWriter{ResponseWriter: w}
		state.Header().Set("X-Content-Type-Options", "nosniff")
		started := time.Now()
		defer func() {
			status := state.status
			if status == 0 {
				status = 200
			}
			logger.InfoContext(r.Context(), "HTTP request completed", "method", r.Method, "route", r.Pattern, "status", status, "bytes", state.bytes, "duration", time.Since(started))
		}()
		defer func() {
			if p := recover(); p != nil {
				if p == http.ErrAbortHandler {
					panic(p)
				}
				logger.ErrorContext(r.Context(), "HTTP handler panicked")
				if state.status != 0 {
					panic(http.ErrAbortHandler)
				}
				state.Header().Del("Content-Length")
				writeError(state, 500, "internal_error", "Internal server error")
			}
		}()
		next.ServeHTTP(state, r)
	})
}
