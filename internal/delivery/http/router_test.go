package http

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/akhmed9505/image-processor/internal/domain"
	service "github.com/akhmed9505/image-processor/internal/service/image"
	"github.com/google/uuid"
)

func TestRouterWeb(t *testing.T) {
	t.Chdir(filepath.Join("..", "..", ".."))
	router := NewRouter(nil, nil, testLogger())
	for _, tc := range []struct {
		path, contentType, content string
	}{
		{"/", "text/html", "<html lang=\"ru\">"},
		{"/styles.css", "text/css", ".workspace"},
		{"/app.js", "javascript", "FormData"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			w := call(router, "GET", tc.path)
			if w.Code != 200 || !strings.Contains(w.Header().Get("Content-Type"), tc.contentType) || !strings.Contains(w.Body.String(), tc.content) {
				t.Fatal(w.Code, w.Header(), w.Body.String())
			}
			if w := call(router, "HEAD", tc.path); w.Code != 200 || w.Body.Len() != 0 {
				t.Fatal("HEAD", w.Code, w.Body.String())
			}
		})
	}
	for _, path := range []string{"/.env", "/AGENTS.md", "/.git/config", "/web/index.html", "/internal/app/app.go", "/index.html", "/app.js/extra"} {
		if w := call(router, "GET", path); w.Code != 404 || !strings.Contains(w.Body.String(), `"error"`) {
			t.Fatal(path, w.Code, w.Body.String())
		}
	}
	if w := call(router, "POST", "/"); w.Code != 405 {
		t.Fatal(w.Code)
	}
}

func TestRouterRoutingErrors(t *testing.T) {
	router := NewRouter(nil, nil, testLogger())
	for _, tc := range []struct {
		method, url string
		status      int
	}{{"GET", "/not-found", 404}, {"POST", "/images", 405}, {"GET", "/upload", 405}} {
		w := call(router, tc.method, tc.url)
		if w.Code != tc.status || !strings.Contains(w.Body.String(), `"error"`) {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}

func TestRouterMetadataRoutes(t *testing.T) {
	id := uuid.NewString()
	router := NewRouter(nil, imagesMock{get: func(context.Context, string) (service.ImageInfo, error) {
		return service.ImageInfo{ID: id, Status: domain.ImageStatusCompleted}, nil
	}}, testLogger())
	for _, suffix := range []string{"/status", "/metadata"} {
		if w := call(router, "GET", "/image/"+id+suffix); w.Code != 200 || !strings.Contains(w.Body.String(), "completed") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}

func TestRequestLoggingOmitsQuery(t *testing.T) {
	var output bytes.Buffer
	router := NewRouter(nil, nil, slog.New(slog.NewJSONHandler(&output, nil)))
	response := call(router, "GET", "/image/"+uuid.NewString()+"/status?secret=private-query")
	if response.Code != 400 || strings.Contains(output.String(), "private-query") || !strings.Contains(output.String(), `"route":"GET /image/{id}/status"`) || !strings.Contains(output.String(), `"status":400`) {
		t.Fatal(response.Code, output.String())
	}
}
