package domain

import "fmt"

// Operation identifies an independent transformation of the original image.
type Operation string

// Supported image operations.
const (
	OperationResize    Operation = "resize"
	OperationThumbnail Operation = "thumbnail"
	OperationWatermark Operation = "watermark"
)

// ResizeParams specifies the bounding box without cropping or upscaling.
type ResizeParams struct{ Width, Height int }

// ThumbnailParams specifies the side of a square center-cropped thumbnail.
type ThumbnailParams struct{ Size int }

// OperationParams contains only the parameters for the selected operation.
// Watermark uses no parameters; both fields must be zero for it.
type OperationParams struct {
	Resize    ResizeParams
	Thumbnail ThumbnailParams
}

// Validate checks the selected operation and its parameters.
// Configurable resource limits are enforced by the consuming service.
func (p OperationParams) Validate(operation Operation) error {
	switch operation {
	case OperationResize:
		if p.Resize.Width > 0 && p.Resize.Height > 0 && p.Thumbnail == (ThumbnailParams{}) {
			return nil
		}
	case OperationThumbnail:
		if p.Thumbnail.Size > 0 && p.Resize == (ResizeParams{}) {
			return nil
		}
	case OperationWatermark:
		if p == (OperationParams{}) {
			return nil
		}
	default:
		return fmt.Errorf("%w: %q", ErrInvalidOperation, operation)
	}
	return fmt.Errorf("%w: %s", ErrInvalidOperationParams, operation)
}
