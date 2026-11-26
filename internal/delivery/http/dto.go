package http

import (
	"errors"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
	service "github.com/akhmed9505/image-processor/internal/service/image"
)

type operationDTO struct {
	Type   domain.Operation `json:"type"`
	Width  *int             `json:"width,omitempty"`
	Height *int             `json:"height,omitempty"`
	Size   *int             `json:"size,omitempty"`
}

func parseOperations(raw string) ([]service.OperationRequest, error) {
	var wire []operationDTO
	if err := decodeJSON(raw, &wire); err != nil {
		return nil, err
	}
	operations := make([]service.OperationRequest, len(wire))
	for i, op := range wire {
		params := domain.OperationParams{}
		switch op.Type {
		case domain.OperationResize:
			if op.Size != nil {
				return nil, errors.New("unexpected resize field")
			}
			if op.Width != nil {
				params.Resize.Width = *op.Width
			}
			if op.Height != nil {
				params.Resize.Height = *op.Height
			}
		case domain.OperationThumbnail:
			if op.Width != nil || op.Height != nil {
				return nil, errors.New("unexpected thumbnail field")
			}
			if op.Size != nil {
				params.Thumbnail.Size = *op.Size
			}
		case domain.OperationWatermark:
			if op.Width != nil || op.Height != nil || op.Size != nil {
				return nil, errors.New("unexpected watermark field")
			}
		}
		operations[i] = service.OperationRequest{Type: op.Type, Params: params}
	}
	return operations, nil
}
func operationParams(job service.JobInfo) operationDTO {
	result := operationDTO{Type: job.Operation}
	switch job.Operation {
	case domain.OperationResize:
		result.Width = &job.Params.Resize.Width
		result.Height = &job.Params.Resize.Height
	case domain.OperationThumbnail:
		result.Size = &job.Params.Thumbnail.Size
	}
	return result
}

type jobDTO struct {
	ID              string           `json:"id"`
	Operation       operationDTO     `json:"operation"`
	Status          domain.JobStatus `json:"status"`
	Attempts        int              `json:"attempts"`
	ResultWidth     int              `json:"result_width,omitempty"`
	ResultHeight    int              `json:"result_height,omitempty"`
	ResultSizeBytes int64            `json:"result_size_bytes,omitempty"`
	ErrorCode       string           `json:"error_code,omitempty"`
}
type imageDTO struct {
	ID           string             `json:"id"`
	OriginalName string             `json:"original_name"`
	ContentType  string             `json:"content_type"`
	SizeBytes    int64              `json:"size_bytes"`
	Width        int                `json:"width"`
	Height       int                `json:"height"`
	CreatedAt    time.Time          `json:"created_at"`
	Status       domain.ImageStatus `json:"status"`
	Jobs         []jobDTO           `json:"jobs"`
}

func toImageDTO(image service.ImageInfo) imageDTO {
	jobs := make([]jobDTO, len(image.Jobs))
	for i, j := range image.Jobs {
		jobs[i] = jobDTO{ID: j.ID, Operation: operationParams(j), Status: j.Status, Attempts: j.Attempts, ResultWidth: j.ResultWidth, ResultHeight: j.ResultHeight, ResultSizeBytes: j.ResultSizeBytes, ErrorCode: j.LastErrorCode}
	}
	return imageDTO{ID: image.ID, OriginalName: image.OriginalName, ContentType: image.ContentType, SizeBytes: image.SizeBytes, Width: image.Width, Height: image.Height, CreatedAt: image.CreatedAt, Status: image.Status, Jobs: jobs}
}

type cursorDTO struct {
	CreatedAt time.Time `json:"created_at"`
	ID        string    `json:"id"`
}
