package image

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
)

const (
	imageDatabaseTimeout = 5 * time.Second
	imageStorageTimeout  = 30 * time.Second
)

// Read errors distinguish unavailable results from absent stored content.
var (
	ErrJobNotFound     = errors.New("image job not found")
	ErrResultNotReady  = errors.New("job result is not completed")
	ErrContentNotFound = errors.New("storage object not found")
)

// ImageCursor identifies the last image in a page ordered by creation time and ID.
type ImageCursor struct {
	CreatedAt time.Time
	ID        string
}

// ImageRecords is the repository snapshot, including internal object references.
type ImageRecords struct {
	Images     []domain.Image
	NextCursor *ImageCursor
}

// ImageReader reads active images and consistent job sets from PostgreSQL.
type ImageReader interface {
	GetByID(context.Context, string) (domain.Image, error)
	List(context.Context, int, *ImageCursor) (ImageRecords, error)
}

// ImageDeleter owns tombstoning, cancellation, and cleanup scheduling transactions.
type ImageDeleter interface {
	Delete(context.Context, string) error
}

// ContentStorage opens streams; missing objects wrap ErrContentNotFound.
// Context cancellation and Close must interrupt blocked reads.
type ContentStorage interface {
	Open(context.Context, string) (io.ReadCloser, error)
}

// JobInfo exposes processing metadata without storage keys or attempt credentials.
type JobInfo struct {
	ID                                string
	Operation                         domain.Operation
	Params                            domain.OperationParams
	Status                            domain.JobStatus
	Attempts                          int
	NextRetryAt                       time.Time
	ResultWidth, ResultHeight         int
	ResultSizeBytes                   int64
	LastErrorCode                     string
	CreatedAt, UpdatedAt, CompletedAt time.Time
}

// ImageInfo is the public metadata and aggregated processing state of an active image.
type ImageInfo struct {
	ID, OriginalName, ContentType string
	SizeBytes                     int64
	Width, Height                 int
	CreatedAt                     time.Time
	Status                        domain.ImageStatus
	Jobs                          []JobInfo
}

// ImageList carries public metadata and the next page's cursor, if any.
type ImageList struct {
	Images     []ImageInfo
	NextCursor *ImageCursor
}

// Content owns a stream whose context remains live until Close or its deadline.
// Callers must close Reader even after EOF; it may be read concurrently with Close.
type Content struct {
	Reader        io.ReadCloser
	ContentType   string
	SizeBytes     int64
	Width, Height int
}

// ReadDeleteService reads active images and delegates logical deletion.
type ReadDeleteService struct {
	reader  ImageReader
	deleter ImageDeleter
	storage ContentStorage
}

// NewReadDeleteService leaves dependency ownership with the caller.
func NewReadDeleteService(reader ImageReader, deleter ImageDeleter, storage ContentStorage) *ReadDeleteService {
	return &ReadDeleteService{reader, deleter, storage}
}

// Get returns public metadata with status derived by domain.ProcessingStatus.
func (s *ReadDeleteService) Get(ctx context.Context, id string) (ImageInfo, error) {
	image, err := s.get(ctx, id)
	if err != nil {
		return ImageInfo{}, err
	}
	return imageInfo(image)
}

// List preserves the repository's page limits, cursor, ordering, and snapshot consistency.
func (s *ReadDeleteService) List(ctx context.Context, limit int, cursor *ImageCursor) (ImageList, error) {
	dbCtx, cancel := context.WithTimeout(ctx, imageDatabaseTimeout)
	defer cancel()
	records, err := s.reader.List(dbCtx, limit, cursor)
	if err != nil {
		return ImageList{}, readError{"list images", err}
	}
	result := ImageList{Images: make([]ImageInfo, 0, len(records.Images)), NextCursor: records.NextCursor}
	for _, image := range records.Images {
		info, err := imageInfo(image)
		if err != nil {
			return ImageList{}, err
		}
		result.Images = append(result.Images, info)
	}
	return result, nil
}

// OpenOriginal returns the encoded original without buffering its contents.
func (s *ReadDeleteService) OpenOriginal(ctx context.Context, id string) (Content, error) {
	image, err := s.get(ctx, id)
	if err != nil {
		return Content{}, err
	}
	return s.open(ctx, image.OriginalKey, image.ContentType, image.SizeBytes, image.Width, image.Height)
}

// OpenResult accepts only a completed job belonging to the requested active image.
func (s *ReadDeleteService) OpenResult(ctx context.Context, imageID, jobID string) (Content, error) {
	image, err := s.get(ctx, imageID)
	if err != nil {
		return Content{}, err
	}
	for _, job := range image.Jobs {
		if job.ID != jobID {
			continue
		}
		if job.Status != domain.JobStatusCompleted {
			return Content{}, ErrResultNotReady
		}
		return s.open(ctx, job.ResultKey, image.ContentType, job.ResultSizeBytes, job.ResultWidth, job.ResultHeight)
	}
	return Content{}, ErrJobNotFound
}

// Delete is idempotent for existing tombstones and never physically removes objects.
// Uncertain commits are returned unchanged through wrapping for caller reconciliation.
func (s *ReadDeleteService) Delete(ctx context.Context, id string) error {
	dbCtx, cancel := context.WithTimeout(ctx, imageDatabaseTimeout)
	defer cancel()
	if err := s.deleter.Delete(dbCtx, id); err != nil {
		return readError{"delete image", err}
	}
	return nil
}

func (s *ReadDeleteService) get(ctx context.Context, id string) (domain.Image, error) {
	dbCtx, cancel := context.WithTimeout(ctx, imageDatabaseTimeout)
	defer cancel()
	image, err := s.reader.GetByID(dbCtx, id)
	if err != nil {
		return domain.Image{}, readError{"get image", err}
	}
	if !image.DeletedAt.IsZero() {
		return domain.Image{}, domain.ErrImageNotFound
	}
	return image, nil
}

func imageInfo(image domain.Image) (ImageInfo, error) {
	status, err := image.ProcessingStatus()
	if err != nil {
		return ImageInfo{}, readError{"derive image status", err}
	}
	result := ImageInfo{ID: image.ID, OriginalName: image.OriginalName, ContentType: image.ContentType, SizeBytes: image.SizeBytes, Width: image.Width, Height: image.Height, CreatedAt: image.CreatedAt, Status: status, Jobs: make([]JobInfo, len(image.Jobs))}
	for i, job := range image.Jobs {
		result.Jobs[i] = JobInfo{ID: job.ID, Operation: job.Operation, Params: job.Params, Status: job.Status, Attempts: job.Attempts, NextRetryAt: job.NextRetryAt, ResultWidth: job.ResultWidth, ResultHeight: job.ResultHeight, ResultSizeBytes: job.ResultSizeBytes, LastErrorCode: job.LastErrorCode, CreatedAt: job.CreatedAt, UpdatedAt: job.UpdatedAt, CompletedAt: job.CompletedAt}
	}
	return result, nil
}

func (s *ReadDeleteService) open(ctx context.Context, key, mime string, size int64, w, h int) (Content, error) {
	storageCtx, cancel := context.WithTimeout(ctx, imageStorageTimeout)
	body, err := s.storage.Open(storageCtx, key)
	if err != nil {
		cancel()
		return Content{}, readError{"open image content", err}
	}
	if err := storageCtx.Err(); err != nil {
		body.Close()
		cancel()
		return Content{}, err
	}
	return Content{Reader: &contentReader{body: body, ctx: storageCtx, cancel: cancel}, ContentType: mime, SizeBytes: size, Width: w, Height: h}, nil
}

type contentReader struct {
	body     io.ReadCloser
	ctx      context.Context
	cancel   context.CancelFunc
	once     sync.Once
	closeErr error
}

func (r *contentReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.body.Read(p)
	if ctxErr := r.ctx.Err(); ctxErr != nil {
		return n, ctxErr
	}
	if err != nil && err != io.EOF {
		return n, readError{"read image content", err}
	}
	return n, err
}
func (r *contentReader) Close() error {
	r.once.Do(func() {
		r.cancel()
		if err := r.body.Close(); err != nil {
			r.closeErr = readError{"close image content", err}
		}
	})
	return r.closeErr
}

// readError preserves causes for classification without exposing storage paths in messages.
type readError struct {
	operation string
	cause     error
}

func (e readError) Error() string { return e.operation + " failed" }
func (e readError) Unwrap() error { return e.cause }
