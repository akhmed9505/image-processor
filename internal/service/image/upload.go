// Package image coordinates image uploads.
package image

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
	"github.com/akhmed9505/image-processor/internal/service/processor"
	"github.com/google/uuid"
)

// OriginalIntentTTL bounds the original object write window.
const OriginalIntentTTL = 120 * time.Second

// ErrDuplicateOperation identifies repeated operation types.
var ErrDuplicateOperation = errors.New("duplicate image operation")

// UploadRepository atomically retains the intent and creates the image, jobs, and events.
type UploadRepository interface {
	Create(context.Context, domain.Image) (domain.Image, error)
}

// OriginalRegistry reserves immutable original keys before any object write.
type OriginalRegistry interface {
	ReserveOriginal(context.Context, string, string, time.Duration) error
}

// ObjectStorage stores the original stream without changing its encoding.
type ObjectStorage interface {
	Put(context.Context, string, io.Reader, int64, string) error
}

// OperationRequest specifies one requested transformation using domain parameters.
type OperationRequest struct {
	Type   domain.Operation
	Params domain.OperationParams
}

// UploadService owns validation slots; its dependencies remain caller-owned.
type UploadService struct {
	repository      UploadRepository
	registry        OriginalRegistry
	storage         ObjectStorage
	validationSlots chan struct{}
}

// NewUploadService defaults validationConcurrency to one when zero is supplied.
func NewUploadService(repository UploadRepository, registry OriginalRegistry, storage ObjectStorage, validationConcurrency int) (*UploadService, error) {
	if validationConcurrency < 0 {
		return nil, errors.New("validation concurrency must be nonnegative")
	}
	if validationConcurrency == 0 {
		validationConcurrency = 1
	}
	return &UploadService{repository, registry, storage, make(chan struct{}, validationConcurrency)}, nil
}

// Upload preserves the original bytes and returns the committed image and jobs.
// Failed or uncertain writes stay registered for cleanup; blocking input I/O must be cancellable.
func (s *UploadService) Upload(ctx context.Context, name string, input io.Reader, operations []OperationRequest) (domain.Image, error) {
	file, info, err := s.validateOriginal(ctx, input)
	if err != nil {
		return domain.Image{}, err
	}
	defer func() { file.Close(); os.Remove(file.Name()) }()
	if len(operations) == 0 {
		return domain.Image{}, domain.ErrNoJobs
	}
	seen := make(map[domain.Operation]bool, 3)
	for _, operation := range operations {
		if err := processor.ValidateOperation(operation.Type, operation.Params); err != nil {
			return domain.Image{}, err
		}
		if seen[operation.Type] {
			return domain.Image{}, ErrDuplicateOperation
		}
		seen[operation.Type] = true

	}
	id, err := uuid.NewRandom()
	if err != nil {
		return domain.Image{}, fmt.Errorf("generate image ID: %w", err)
	}
	ext := "png"
	if info.ContentType == "image/jpeg" {
		ext = "jpg"
	}
	image := domain.Image{ID: id.String(), OriginalName: name, OriginalKey: id.String() + "/original." + ext, ContentType: info.ContentType, SizeBytes: info.SizeBytes, Width: info.Width, Height: info.Height, Jobs: make([]domain.Job, len(operations))}
	for i, operation := range operations {
		jobID, err := uuid.NewRandom()
		if err != nil {
			return domain.Image{}, fmt.Errorf("generate upload job ID: %w", err)
		}
		image.Jobs[i] = domain.Job{ID: jobID.String(), ImageID: image.ID, Operation: operation.Type, Params: operation.Params}
	}
	if err := ctx.Err(); err != nil {
		return domain.Image{}, err
	}
	dbCtx, cancel := context.WithTimeout(ctx, imageDatabaseTimeout)
	err = s.registry.ReserveOriginal(dbCtx, image.OriginalKey, image.ID, OriginalIntentTTL)
	cancel()
	if err != nil {
		return domain.Image{}, fmt.Errorf("reserve upload intent: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return domain.Image{}, err
	}
	storageCtx, cancel := context.WithTimeout(ctx, imageStorageTimeout)
	err = s.storage.Put(storageCtx, image.OriginalKey, file, image.SizeBytes, image.ContentType)
	cancel()
	if err != nil {
		return domain.Image{}, fmt.Errorf("store original: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return domain.Image{}, err
	}
	dbCtx, cancel = context.WithTimeout(ctx, imageDatabaseTimeout)
	created, err := s.repository.Create(dbCtx, image)
	cancel()
	if err != nil {
		return domain.Image{}, fmt.Errorf("finalize upload %s: %w", image.ID, err)
	}
	return created, nil
}

func (s *UploadService) validateOriginal(ctx context.Context, input io.Reader) (*os.File, processor.OriginalInfo, error) {
	select {
	case s.validationSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, processor.OriginalInfo{}, ctx.Err()
	}
	defer func() { <-s.validationSlots }()
	if err := ctx.Err(); err != nil {
		return nil, processor.OriginalInfo{}, err
	}
	file, err := os.CreateTemp("", "imageprocessor-upload-*")
	if err != nil {
		return nil, processor.OriginalInfo{}, fmt.Errorf("create upload spool: %w", err)
	}
	success := false
	defer func() {
		if !success {
			file.Close()
			os.Remove(file.Name())
		}
	}()
	info, err := processor.ValidateOriginal(ctx, io.TeeReader(input, file))
	if err != nil {
		return nil, processor.OriginalInfo{}, fmt.Errorf("validate original: %w", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, processor.OriginalInfo{}, fmt.Errorf("rewind original: %w", err)
	}
	success = true
	return file, info, nil
}
