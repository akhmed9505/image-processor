// Package http exposes image services through net/http.
package http

import (
	"log/slog"
	"net/http"
	"strings"
)

// NewRouter returns the API handler with request logging and panic recovery.
func NewRouter(upload Uploader, images Images, logger *slog.Logger) http.Handler {
	h := handlers{upload, images, logger}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /upload", h.uploadImage)
	mux.HandleFunc("GET /images", h.list)
	mux.HandleFunc("GET /image/{id}", h.content)
	mux.HandleFunc("GET /image/{id}/original", h.original)
	mux.HandleFunc("GET /image/{id}/metadata", h.metadata)
	mux.HandleFunc("GET /image/{id}/status", h.metadata)
	mux.HandleFunc("DELETE /image/{id}", h.delete)
	return middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, pattern := mux.Handler(r); pattern == "" {
			var allowed []string
			for _, method := range []string{"GET", "HEAD", "POST", "DELETE"} {
				copy := r.Clone(r.Context())
				copy.Method = method
				if _, p := mux.Handler(copy); p != "" {
					allowed = append(allowed, method)
				}
			}
			if len(allowed) > 0 {
				w.Header().Set("Allow", strings.Join(allowed, ", "))
				writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed")
			} else {
				writeError(w, http.StatusNotFound, "not_found", "Endpoint not found")
			}
			return
		}
		mux.ServeHTTP(w, r)
	}), logger)
}
