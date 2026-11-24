package domain

import (
	"errors"
	"testing"
)

func TestOperationParamsValidate(t *testing.T) {
	for _, tc := range []struct {
		name      string
		operation Operation
		params    OperationParams
		wantErr   error
	}{
		{"resize", OperationResize, OperationParams{Resize: ResizeParams{600, 600}}, nil},
		{"thumbnail", OperationThumbnail, OperationParams{Thumbnail: ThumbnailParams{200}}, nil},
		{"watermark", OperationWatermark, OperationParams{}, nil},
		{"missing resize params", OperationResize, OperationParams{}, ErrInvalidOperationParams},
		{"zero width", OperationResize, OperationParams{Resize: ResizeParams{0, 100}}, ErrInvalidOperationParams},
		{"negative height", OperationResize, OperationParams{Resize: ResizeParams{100, -1}}, ErrInvalidOperationParams},
		{"missing thumbnail params", OperationThumbnail, OperationParams{}, ErrInvalidOperationParams},
		{"negative thumbnail", OperationThumbnail, OperationParams{Thumbnail: ThumbnailParams{-1}}, ErrInvalidOperationParams},
		{"extra thumbnail", OperationResize, OperationParams{Resize: ResizeParams{100, 100}, Thumbnail: ThumbnailParams{50}}, ErrInvalidOperationParams},
		{"extra resize", OperationThumbnail, OperationParams{Resize: ResizeParams{1, 0}, Thumbnail: ThumbnailParams{50}}, ErrInvalidOperationParams},
		{"watermark with resize", OperationWatermark, OperationParams{Resize: ResizeParams{1, 1}}, ErrInvalidOperationParams},
		{"watermark with thumbnail", OperationWatermark, OperationParams{Thumbnail: ThumbnailParams{1}}, ErrInvalidOperationParams},
		{"unknown", Operation("rotate"), OperationParams{}, ErrInvalidOperation},
		{"empty operation", Operation(""), OperationParams{}, ErrInvalidOperation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.params.Validate(tc.operation); !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
		})
	}
}
