// Package http exposes image services through net/http.
package http

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/akhmed9505/image-processor/internal/domain"
	service "github.com/akhmed9505/image-processor/internal/service/image"
	"github.com/akhmed9505/image-processor/internal/service/processor"
	"github.com/google/uuid"
)

// MaxBodyBytes allows multipart overhead above the 20 MiB file limit.
const (
	MaxBodyBytes       = 21 << 20
	maxOperationsBytes = 4 << 10
)

// Uploader returns only durably committed uploads.
type Uploader interface {
	Upload(context.Context, string, io.Reader, []service.OperationRequest) (domain.Image, error)
}

// Images provides metadata, owned content streams, and logical deletion.
type Images interface {
	Get(context.Context, string) (service.ImageInfo, error)
	List(context.Context, int, *service.ImageCursor) (service.ImageList, error)
	OpenOriginal(context.Context, string) (service.Content, error)
	OpenResult(context.Context, string, string) (service.Content, error)
	Delete(context.Context, string) error
}

type handlers struct {
	upload Uploader
	images Images
	logger *slog.Logger
}

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

func (h handlers) uploadImage(w http.ResponseWriter, r *http.Request) {
	if !validQuery(r, nil) {
		badRequest(w)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
	defer r.Body.Close()
	if r.ContentLength > MaxBodyBytes {
		writeError(w, 413, "body_too_large", "Request body exceeds 21 MiB")
		return
	}
	err := r.ParseMultipartForm(1 << 20)
	if r.MultipartForm != nil {
		defer r.MultipartForm.RemoveAll()
	}
	if err == nil {
		_, err = io.Copy(io.Discard, r.Body)
	}
	if err != nil {
		if r.Context().Err() != nil {
			h.fail(w, r, r.Context().Err())
			return
		}
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, 413, "body_too_large", "Request body exceeds 21 MiB")
		} else {
			badRequest(w)
		}
		return
	}
	form := r.MultipartForm
	if form == nil || len(form.File) != 1 || len(form.File["file"]) != 1 || len(form.Value) != 1 || len(form.Value["operations"]) != 1 {
		badRequest(w)
		return
	}
	file := form.File["file"][0]
	if file.Size > processor.MaxInputBytes {
		writeError(w, 413, "file_too_large", "Image exceeds 20 MiB")
		return
	}
	raw := form.Value["operations"][0]
	if len(raw) > maxOperationsBytes {
		badRequest(w)
		return
	}
	operations, err := parseOperations(raw)
	if err != nil {
		badRequest(w)
		return
	}
	body, err := file.Open()
	if err != nil {
		h.fail(w, r, err)
		return
	}
	defer body.Close()
	name := path.Base(strings.ReplaceAll(file.Filename, "\\", "/"))
	image, err := h.upload.Upload(r.Context(), name, body, operations)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	status, err := image.ProcessingStatus()
	if err != nil {
		h.fail(w, r, err)
		return
	}
	jobs := make([]service.JobInfo, len(image.Jobs))
	for i, j := range image.Jobs {
		jobs[i] = service.JobInfo{ID: j.ID, Operation: j.Operation, Params: j.Params, Status: j.Status, CreatedAt: j.CreatedAt, UpdatedAt: j.UpdatedAt}
	}
	info := service.ImageInfo{ID: image.ID, OriginalName: image.OriginalName, ContentType: image.ContentType, SizeBytes: image.SizeBytes, Width: image.Width, Height: image.Height, CreatedAt: image.CreatedAt, Status: status, Jobs: jobs}
	w.Header().Set("Location", "/image/"+image.ID+"/status")
	writeJSON(w, 202, toImageDTO(info))
}

func (h handlers) metadata(w http.ResponseWriter, r *http.Request) {
	id, ok := requestID(w, r)
	if !ok {
		return
	}
	if !validQuery(r, nil) {
		badRequest(w)
		return
	}
	info, err := h.images.Get(r.Context(), id)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, 200, toImageDTO(info))
}
func (h handlers) list(w http.ResponseWriter, r *http.Request) {
	if !validQuery(r, []string{"limit", "cursor"}) {
		badRequest(w)
		return
	}
	limit := 0
	if values, ok := r.URL.Query()["limit"]; ok {
		n, err := strconv.Atoi(values[0])
		if err != nil || n < 1 || n > 100 {
			badRequest(w)
			return
		}
		limit = n
	}
	var cursor *service.ImageCursor
	if values, ok := r.URL.Query()["cursor"]; ok {
		raw := values[0]
		if len(raw) > 512 {
			badRequest(w)
			return
		}
		data, err := base64.RawURLEncoding.DecodeString(raw)
		var wire cursorDTO
		if err != nil || decodeJSON(string(data), &wire) != nil || wire.CreatedAt.IsZero() {
			badRequest(w)
			return
		}
		id, ok := validID(wire.ID)
		if !ok {
			badRequest(w)
			return
		}
		cursor = &service.ImageCursor{ID: id, CreatedAt: wire.CreatedAt}
	}
	page, err := h.images.List(r.Context(), limit, cursor)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	images := make([]imageDTO, len(page.Images))
	for i, image := range page.Images {
		images[i] = toImageDTO(image)
	}
	next := ""
	if page.NextCursor != nil {
		raw, _ := json.Marshal(cursorDTO{page.NextCursor.CreatedAt, page.NextCursor.ID})
		next = base64.RawURLEncoding.EncodeToString(raw)
	}
	writeJSON(w, 200, struct {
		Images     []imageDTO `json:"images"`
		NextCursor string     `json:"next_cursor,omitempty"`
	}{images, next})
}
func (h handlers) delete(w http.ResponseWriter, r *http.Request) {
	id, ok := requestID(w, r)
	if !ok {
		return
	}
	if !validQuery(r, nil) {
		badRequest(w)
		return
	}
	if err := h.images.Delete(r.Context(), id); err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, 202, struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}{id, "deleted"})
}
func (h handlers) original(w http.ResponseWriter, r *http.Request) {
	id, ok := requestID(w, r)
	if !ok {
		return
	}
	if !validQuery(r, nil) {
		badRequest(w)
		return
	}
	content, err := h.images.OpenOriginal(r.Context(), id)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	h.stream(w, r, content)
}
func (h handlers) content(w http.ResponseWriter, r *http.Request) {
	id, ok := requestID(w, r)
	if !ok {
		return
	}
	if !validQuery(r, []string{"operation"}) {
		badRequest(w)
		return
	}
	operation, provided := r.URL.Query()["operation"]
	if !provided {
		content, err := h.images.OpenOriginal(r.Context(), id)
		if err != nil {
			h.fail(w, r, err)
			return
		}
		h.stream(w, r, content)
		return
	}
	op := domain.Operation(operation[0])
	if op != domain.OperationResize && op != domain.OperationThumbnail && op != domain.OperationWatermark {
		badRequest(w)
		return
	}
	info, err := h.images.Get(r.Context(), id)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	for _, job := range info.Jobs {
		if job.Operation == op {
			content, err := h.images.OpenResult(r.Context(), id, job.ID)
			if err != nil {
				h.fail(w, r, err)
				return
			}
			h.stream(w, r, content)
			return
		}
	}
	writeError(w, 404, "job_not_found", "Requested operation not found")
}
func (h handlers) stream(w http.ResponseWriter, r *http.Request, content service.Content) {
	defer func() {
		if err := content.Reader.Close(); err != nil {
			h.logger.WarnContext(r.Context(), "Content close failed")
		}
	}()
	w.Header().Set("Content-Type", content.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(content.SizeBytes, 10))
	if r.Method == http.MethodHead {
		w.WriteHeader(200)
		return
	}
	n, err := io.Copy(w, content.Reader)
	if err == nil && n != content.SizeBytes {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		h.logger.WarnContext(r.Context(), "Content streaming interrupted", "bytes", n)
		state := w.(*responseWriter)
		if state.status != 0 {
			panic(http.ErrAbortHandler)
		}
		w.Header().Del("Content-Length")
		w.Header().Del("Content-Type")
		h.fail(w, r, err)
	}
}
func (h handlers) fail(w http.ResponseWriter, r *http.Request, err error) {
	status, code, message := 500, "internal_error", "Internal server error"
	switch {
	case errors.Is(err, context.Canceled):
		status, code, message = 408, "request_cancelled", "Request cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		status, code, message = 504, "timeout", "Operation timed out"
	case errors.Is(err, domain.ErrImageNotFound), errors.Is(err, domain.ErrImageDeleted), errors.Is(err, service.ErrJobNotFound):
		status, code, message = 404, "not_found", "Image or job not found"
	case errors.Is(err, service.ErrContentNotFound):
		status, code, message = 404, "content_not_found", "Image content not found"
	case errors.Is(err, service.ErrResultNotReady):
		status, code, message = 409, "result_not_ready", "Result is not completed"
	case errors.Is(err, processor.ErrUnsupportedFormat):
		status, code, message = 415, "unsupported_format", "Only JPEG and PNG are supported"
	case errors.Is(err, processor.ErrLimitExceeded):
		status, code, message = 413, "image_limit_exceeded", "Image exceeds processing limits"
	case errors.Is(err, processor.ErrInvalidImage), errors.Is(err, domain.ErrInvalidOperation), errors.Is(err, domain.ErrInvalidOperationParams), (errors.Is(err, domain.ErrNoJobs) && r.Method == http.MethodPost), errors.Is(err, service.ErrDuplicateOperation):
		status, code, message = 400, "invalid_image_request", "Invalid image or operations"
	}
	if status >= 500 {
		h.logger.ErrorContext(r.Context(), "Image API operation failed", "status", status)
	}
	writeError(w, status, code, message)
}
func requestID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id, ok := validID(r.PathValue("id"))
	if !ok {
		badRequest(w)
	}
	return id, ok
}
func validID(raw string) (string, bool) {
	id, err := uuid.Parse(raw)
	return id.String(), err == nil && id != uuid.Nil && len(raw) == 36
}
func validQuery(r *http.Request, allowed []string) bool {
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return false
	}
	for key, values := range query {
		found := false
		for _, name := range allowed {
			if name == key {
				found = true
			}
		}
		if !found || len(values) != 1 {
			return false
		}
	}
	return true
}
func badRequest(w http.ResponseWriter) {
	writeError(w, 400, "invalid_request", "Invalid request parameters")
}
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}{struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}{code, message}})
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func decodeJSON(raw string, value any) error {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("multiple JSON values")
	}
	return nil
}
