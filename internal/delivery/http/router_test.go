package http

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
	service "github.com/akhmed9505/image-processor/internal/service/image"
	"github.com/akhmed9505/image-processor/internal/service/processor"
	"github.com/google/uuid"
)

type uploadMock func(context.Context, string, io.Reader, []service.OperationRequest) (domain.Image, error)

func (f uploadMock) Upload(c context.Context, n string, r io.Reader, ops []service.OperationRequest) (domain.Image, error) {
	return f(c, n, r, ops)
}

type imagesMock struct {
	get      func(context.Context, string) (service.ImageInfo, error)
	list     func(context.Context, int, *service.ImageCursor) (service.ImageList, error)
	original func(context.Context, string) (service.Content, error)
	result   func(context.Context, string, string) (service.Content, error)
	delete   func(context.Context, string) error
}

func (m imagesMock) Get(c context.Context, id string) (service.ImageInfo, error) { return m.get(c, id) }
func (m imagesMock) List(c context.Context, n int, cur *service.ImageCursor) (service.ImageList, error) {
	return m.list(c, n, cur)
}
func (m imagesMock) OpenOriginal(c context.Context, id string) (service.Content, error) {
	return m.original(c, id)
}
func (m imagesMock) OpenResult(c context.Context, id, j string) (service.Content, error) {
	return m.result(c, id, j)
}
func (m imagesMock) Delete(c context.Context, id string) error { return m.delete(c, id) }
func testLogger() *slog.Logger                                 { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
func multipartRequest(t *testing.T, data []byte, operations string) *http.Request {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	file, e := writer.CreateFormFile("file", "untrusted.png")
	if e != nil {
		t.Fatal(e)
	}
	_, _ = file.Write(data)
	_ = writer.WriteField("operations", operations)
	_ = writer.Close()
	r := httptest.NewRequest("POST", "/upload", &body)
	r.Header.Set("Content-Type", writer.FormDataContentType())
	return r
}
func call(router http.Handler, method, url string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(method, url, nil))
	return w
}
func TestUploadAndTemporaryFiles(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	id := uuid.NewString()
	jobID := uuid.NewString()
	data := bytes.Repeat([]byte{1}, 2<<20)
	router := NewRouter(uploadMock(func(ctx context.Context, name string, r io.Reader, ops []service.OperationRequest) (domain.Image, error) {
		got, e := io.ReadAll(r)
		if e != nil || !bytes.Equal(got, data) || name != "untrusted.png" || len(ops) != 1 || ops[0].Params.Resize.Width != 60 {
			t.Fatal("upload mapping", e, ops)
		}
		return domain.Image{ID: id, OriginalName: name, OriginalKey: "secret/key", ContentType: "image/png", Width: 120, Height: 80, SizeBytes: int64(len(data)), Jobs: []domain.Job{{ID: jobID, Operation: domain.OperationResize, Params: ops[0].Params, Status: domain.JobStatusPending, ResultKey: "private/result"}}}, nil
	}), nil, testLogger())
	w := httptest.NewRecorder()
	router.ServeHTTP(w, multipartRequest(t, data, `[{"type":"resize","width":60,"height":40}]`))
	if w.Code != 202 || !strings.Contains(w.Body.String(), `"status":"pending"`) || strings.Contains(w.Body.String(), "secret/") || strings.Contains(w.Body.String(), "private/") {
		t.Fatal(w.Code, w.Body.String())
	}
	files, e := os.ReadDir(dir)
	if e != nil || len(files) != 0 {
		t.Fatal("multipart files leaked", files, e)
	}
}
func TestUploadValidation(t *testing.T) {
	router := NewRouter(uploadMock(func(context.Context, string, io.Reader, []service.OperationRequest) (domain.Image, error) {
		t.Fatal("invalid transport input reached service")
		return domain.Image{}, nil
	}), nil, testLogger())
	for _, operations := range []string{`{}`, `[{"type":"resize","extra":1}]`, `[{"type":"watermark","width":0}]`, `[] {}`, strings.Repeat(" ", maxOperationsBytes+1)} {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, multipartRequest(t, []byte("x"), operations))
		if w.Code != 400 {
			t.Fatal(w.Code)
		}
	}
	for _, size := range []int{processor.MaxInputBytes + 1, MaxBodyBytes + 1} {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, multipartRequest(t, make([]byte, size), `[]`))
		if w.Code != 413 {
			t.Fatal("size accepted", size, w.Code)
		}
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	_ = writer.WriteField("operations", "[]")
	_ = writer.Close()
	r := httptest.NewRequest("POST", "/upload", &body)
	r.Header.Set("Content-Type", writer.FormDataContentType())
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
}
func TestPaginationAndMetadata(t *testing.T) {
	id := uuid.NewString()
	created := time.Now().UTC()
	var cur *service.ImageCursor
	mock := imagesMock{get: func(context.Context, string) (service.ImageInfo, error) {
		return service.ImageInfo{ID: id, Status: domain.ImageStatusCompleted}, nil
	}, list: func(_ context.Context, n int, cursor *service.ImageCursor) (service.ImageList, error) {
		if n != 1 {
			t.Fatal(n)
		}
		cur = cursor
		return service.ImageList{Images: []service.ImageInfo{{ID: id}}, NextCursor: &service.ImageCursor{ID: id, CreatedAt: created}}, nil
	}}
	router := NewRouter(nil, mock, testLogger())
	w := call(router, "GET", "/images?limit=1")
	var page struct {
		NextCursor string `json:"next_cursor"`
	}
	if e := json.Unmarshal(w.Body.Bytes(), &page); e != nil {
		t.Fatal(e)
	}
	w = call(router, "GET", "/images?limit=1&cursor="+page.NextCursor)
	if w.Code != 200 || cur == nil || cur.ID != id || !cur.CreatedAt.Equal(created) {
		t.Fatal(w.Code, cur)
	}
	for _, suffix := range []string{"/status", "/metadata"} {
		if w := call(router, "GET", "/image/"+id+suffix); w.Code != 200 || !strings.Contains(w.Body.String(), "completed") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	badCursor := base64.RawURLEncoding.EncodeToString([]byte(`{"id":"bad","created_at":"2020-01-01T00:00:00Z"}`))
	for _, url := range []string{"/images?limit=0", "/images?limit=101", "/images?cursor=", "/images?cursor=" + badCursor, "/images?limit=1&limit=2", "/images?x=1", "/images?limit=%zz", "/image/bad/status", "/image/" + id + "?operation=no", "/image/" + id + "?operation=resize&operation=resize"} {
		if w := call(router, "GET", url); w.Code != 400 {
			t.Fatal(url, w.Code)
		}
	}
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

type trackedReader struct {
	io.Reader
	closed bool
}

func (r *trackedReader) Close() error { r.closed = true; return nil }
func TestStreamAndDelete(t *testing.T) {
	id, jid := uuid.NewString(), uuid.NewString()
	var body *trackedReader
	content := func() (service.Content, error) {
		body = &trackedReader{Reader: strings.NewReader("data")}
		return service.Content{Reader: body, ContentType: "image/png", SizeBytes: 4}, nil
	}
	router := NewRouter(nil, imagesMock{original: func(context.Context, string) (service.Content, error) { return content() }, result: func(_ context.Context, image, job string) (service.Content, error) {
		if image != id || job != jid {
			t.Fatal(image, job)
		}
		return content()
	}, get: func(context.Context, string) (service.ImageInfo, error) {
		return service.ImageInfo{Jobs: []service.JobInfo{{ID: jid, Operation: domain.OperationResize}}}, nil
	}, delete: func(context.Context, string) error { return nil }}, testLogger())
	for _, suffix := range []string{"", "/original", "?operation=resize"} {
		w := call(router, "GET", "/image/"+id+suffix)
		if w.Code != 200 || w.Body.String() != "data" || w.Header().Get("Content-Type") != "image/png" || w.Header().Get("Content-Length") != "4" || !body.closed {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if w := call(router, "DELETE", "/image/"+id); w.Code != 202 {
		t.Fatal(w.Code)
	}
}
func TestSafeErrorMapping(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
	}{{domain.ErrImageNotFound, 404}, {service.ErrContentNotFound, 404}, {service.ErrResultNotReady, 409}, {processor.ErrUnsupportedFormat, 415}, {processor.ErrInvalidImage, 400}, {processor.ErrLimitExceeded, 413}, {service.ErrDuplicateOperation, 400}, {context.Canceled, 408}, {context.DeadlineExceeded, 504}, {errors.New("secret/password/storage/key"), 500}} {
		router := NewRouter(nil, imagesMock{get: func(context.Context, string) (service.ImageInfo, error) { return service.ImageInfo{}, tc.err }}, testLogger())
		w := call(router, "GET", "/image/"+uuid.NewString()+"/status")
		if w.Code != tc.status || !strings.Contains(w.Body.String(), `"error"`) || strings.Contains(w.Body.String(), "secret/") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}

type failingReader struct {
	sent    bool
	partial bool
	panics  bool
}

func (r *failingReader) Read(p []byte) (int, error) {
	if r.panics {
		panic("secret panic")
	}
	if r.partial && !r.sent {
		r.sent = true
		return copy(p, "x"), nil
	}
	return 0, io.ErrUnexpectedEOF
}
func TestStreamingFailureAndRecovery(t *testing.T) {
	for _, partial := range []bool{false, true} {
		for _, panics := range []bool{false, true} {
			body := &trackedReader{Reader: &failingReader{partial: partial, panics: panics}}
			router := NewRouter(nil, imagesMock{original: func(context.Context, string) (service.Content, error) {
				return service.Content{Reader: body, ContentType: "image/png", SizeBytes: 4}, nil
			}}, testLogger())
			w := httptest.NewRecorder()
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				router.ServeHTTP(w, httptest.NewRequest("GET", "/image/"+uuid.NewString(), nil))
			}()
			if partial && !panics {
				if recovered != http.ErrAbortHandler || w.Body.String() != "x" {
					t.Fatal("partial response rewritten", recovered, w.Body.String())
				}
			} else if w.Code != 500 || strings.Contains(w.Body.String(), "secret") || w.Header().Get("Content-Length") != "" {
				t.Fatal(w.Code, w.Body.String())
			}
			if !body.closed {
				t.Fatal("reader leaked")
			}
		}
	}
}

func TestChunkedMultipartEpilogueLimit(t *testing.T) {
	request := multipartRequest(t, []byte("small"), `[{"type":"watermark"}]`)
	request.Body = io.NopCloser(io.MultiReader(request.Body, strings.NewReader(strings.Repeat("x", MaxBodyBytes))))
	request.ContentLength = -1
	router := NewRouter(uploadMock(func(context.Context, string, io.Reader, []service.OperationRequest) (domain.Image, error) {
		t.Fatal("oversized multipart epilogue accepted")
		return domain.Image{}, nil
	}), nil, testLogger())
	w := httptest.NewRecorder()
	router.ServeHTTP(w, request)
	if w.Code != 413 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestExactFileLimitReachesService(t *testing.T) {
	called := false
	router := NewRouter(uploadMock(func(_ context.Context, _ string, r io.Reader, _ []service.OperationRequest) (domain.Image, error) {
		called = true
		n, e := io.Copy(io.Discard, r)
		if e != nil || n != processor.MaxInputBytes {
			t.Fatal(n, e)
		}
		return domain.Image{}, processor.ErrInvalidImage
	}), nil, testLogger())
	w := httptest.NewRecorder()
	router.ServeHTTP(w, multipartRequest(t, make([]byte, processor.MaxInputBytes), `[{"type":"watermark"}]`))
	if !called || w.Code != 400 {
		t.Fatal("exact file boundary rejected by transport", w.Code)
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

func TestOperationsSizeBoundary(t *testing.T) {
	for _, tc := range []struct {
		name         string
		size, status int
		accepted     bool
	}{
		{"4096 bytes", 4096, http.StatusAccepted, true},
		{"4097 bytes", 4097, http.StatusBadRequest, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			router := NewRouter(uploadMock(func(_ context.Context, _ string, _ io.Reader, operations []service.OperationRequest) (domain.Image, error) {
				called = true
				if len(operations) != 1 || operations[0].Type != domain.OperationWatermark {
					t.Fatal("operations were not parsed correctly", operations)
				}
				return domain.Image{ID: uuid.NewString(), Jobs: []domain.Job{{ID: uuid.NewString(), Operation: domain.OperationWatermark, Status: domain.JobStatusPending}}}, nil
			}), nil, testLogger())
			raw := `[{"type":"watermark"}]`
			raw += strings.Repeat(" ", tc.size-len(raw))
			w := httptest.NewRecorder()
			router.ServeHTTP(w, multipartRequest(t, []byte("image bytes"), raw))
			if w.Code != tc.status || called != tc.accepted {
				t.Fatal("incorrect operations boundary handling", w.Code, called, w.Body.String())
			}
		})
	}
}
