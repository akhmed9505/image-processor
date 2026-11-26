//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"image"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	delivery "github.com/akhmed9505/image-processor/internal/delivery/http"
	repository "github.com/akhmed9505/image-processor/internal/repository/postgres"
	"github.com/akhmed9505/image-processor/internal/service/processor"
)

func apiMultipart(t *testing.T, data []byte, operations string) (*bytes.Buffer, string) {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	file, e := writer.CreateFormFile("file", "photo.wrong")
	if e != nil {
		t.Fatal(e)
	}
	_, _ = file.Write(data)
	_ = writer.WriteField("operations", operations)
	_ = writer.Close()
	return &body, writer.FormDataContentType()
}
func apiRequest(t *testing.T, server *httptest.Server, method, path string, body io.Reader, contentType string) (*http.Response, []byte) {
	t.Helper()
	request, e := http.NewRequest(method, server.URL+path, body)
	if e != nil {
		t.Fatal(e)
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response, e := server.Client().Do(request)
	if e != nil {
		t.Fatal(e)
	}
	data, e := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if e != nil || closeErr != nil {
		t.Fatal(e, closeErr)
	}
	return response, data
}

func TestHTTPImageLifecycle(t *testing.T) {
	for _, format := range []string{"png", "jpeg"} {
		t.Run(format, func(t *testing.T) {
			p := migratedReadPool(t)
			storage := uploadStorage(t)
			router := delivery.NewRouter(imageUpload(t, p, storage), readDeleteService(p, storage), slog.New(slog.NewTextHandler(io.Discard, nil)))
			server := httptest.NewServer(router)
			defer server.Close()
			original := uploadBytes(t, format)
			body, mime := apiMultipart(t, original, `[{"type":"resize","width":60,"height":40},{"type":"thumbnail","size":20},{"type":"watermark"}]`)
			response, data := apiRequest(t, server, "POST", "/upload", body, mime)
			if response.StatusCode != 202 || bytes.Contains(data, []byte("original_key")) || bytes.Contains(data, []byte("attempt_token")) {
				t.Fatal(response.StatusCode, string(data))
			}
			var uploaded struct {
				ID     string `json:"id"`
				Status string `json:"status"`
				Jobs   []struct {
					ID string `json:"id"`
				} `json:"jobs"`
			}
			if e := json.Unmarshal(data, &uploaded); e != nil || uploaded.ID == "" || uploaded.Status != "pending" || len(uploaded.Jobs) != 3 {
				t.Fatal(string(data), e)
			}
			id := uploaded.ID
			for _, path := range []string{"/image/" + id + "/status", "/image/" + id + "/metadata", "/images?limit=1"} {
				r, b := apiRequest(t, server, "GET", path, nil, "")
				if r.StatusCode != 200 || !strings.Contains(string(b), id) {
					t.Fatal(path, r.StatusCode, string(b))
				}
			}
			for _, path := range []string{"/image/" + id, "/image/" + id + "/original"} {
				r, b := apiRequest(t, server, "GET", path, nil, "")
				if r.StatusCode != 200 || !bytes.Equal(b, original) || r.ContentLength != int64(len(original)) || r.Header.Get("Content-Type") != "image/"+format {
					t.Fatal("original HTTP stream", r.StatusCode)
				}
			}
			if r, _ := apiRequest(t, server, "GET", "/image/"+id+"?operation=resize", nil, ""); r.StatusCode != 409 {
				t.Fatal("pending result", r.StatusCode)
			}
			jobs := processor.NewJobService(processingRepo(t, p), storage)
			for _, job := range uploaded.Jobs {
				a, e := jobs.Admit(context.Background(), job.ID)
				if e != nil {
					t.Fatal(e)
				}
				if _, e := jobs.ProcessAttempt(context.Background(), a); e != nil {
					t.Fatal(e)
				}
			}
			for _, op := range []string{"resize", "thumbnail", "watermark"} {
				r, b := apiRequest(t, server, "GET", "/image/"+id+"?operation="+op, nil, "")
				if r.StatusCode != 200 || r.Header.Get("Content-Type") != "image/"+format || r.ContentLength != int64(len(b)) {
					t.Fatal("result stream", r.StatusCode)
				}
				_, detected, e := image.Decode(bytes.NewReader(b))
				if e != nil || detected != format {
					t.Fatal(detected, e)
				}
			}
			image, e := repository.NewImageRepository(p).GetByID(context.Background(), id)
			if e != nil {
				t.Fatal(e)
			}
			for range 2 {
				r, _ := apiRequest(t, server, "DELETE", "/image/"+id, nil, "")
				if r.StatusCode != 202 {
					t.Fatal(r.StatusCode)
				}
			}
			if r, _ := apiRequest(t, server, "GET", "/image/"+id+"/status", nil, ""); r.StatusCode != 404 {
				t.Fatal(r.StatusCode)
			}
			if r, b := apiRequest(t, server, "GET", "/images", nil, ""); r.StatusCode != 200 || strings.Contains(string(b), id) {
				t.Fatal(r.StatusCode, string(b))
			}
			if _, e := storage.Stat(context.Background(), image.OriginalKey); e != nil {
				t.Fatal("HTTP delete removed physical content", e)
			}
		})
	}
}

func TestHTTPUploadFailures(t *testing.T) {
	p := migratedReadPool(t)
	storage := uploadStorage(t)
	server := httptest.NewServer(delivery.NewRouter(imageUpload(t, p, storage), readDeleteService(p, storage), slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer server.Close()
	for _, tc := range []struct {
		data       []byte
		operations string
		status     int
	}{
		{[]byte("invalid image"), `[{"type":"watermark"}]`, 415},
		{uploadBytes(t, "png"), `[{"type":"watermark"},{"type":"watermark"}]`, 400},
		{uploadBytes(t, "png"), `[{"type":"resize","width":0,"height":1}]`, 400},
		{uploadBytes(t, "png"), `[{"type":"thumbnail","size":2049}]`, 413},
	} {
		body, mime := apiMultipart(t, tc.data, tc.operations)
		r, b := apiRequest(t, server, "POST", "/upload", body, mime)
		if r.StatusCode != tc.status || !strings.Contains(string(b), `"error"`) {
			t.Fatal(r.StatusCode, string(b))
		}
	}
	assertUploadCounts(t, p, 0, 0, 0)
	if _, e := p.Exec(context.Background(), `CREATE FUNCTION reject_http_upload() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'secret-db-credentials'; END $$; CREATE TRIGGER reject_http_upload BEFORE INSERT ON images FOR EACH ROW EXECUTE FUNCTION reject_http_upload()`); e != nil {
		t.Fatal(e)
	}
	body, mime := apiMultipart(t, uploadBytes(t, "png"), `[{"type":"watermark"}]`)
	r, b := apiRequest(t, server, "POST", "/upload", body, mime)
	if r.StatusCode != 500 || strings.Contains(string(b), "secret") || strings.Contains(string(b), "SQL") {
		t.Fatal("infrastructure details exposed", r.StatusCode, string(b))
	}
}
