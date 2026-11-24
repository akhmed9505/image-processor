package processor

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/color"
	"io"
)

// OriginalInfo describes the encoded original and its orientation-normalized dimensions.
type OriginalInfo struct {
	Width, Height int
	ContentType   string
	SizeBytes     int64
	Orientation   int
}

// ValidateOriginal fully decodes an original without transforming or encoding it.
func ValidateOriginal(ctx context.Context, input io.Reader) (OriginalInfo, error) {
	o, err := readOriginal(ctx, input)
	if err != nil {
		return OriginalInfo{}, err
	}
	if _, err := o.decode(ctx); err != nil {
		return OriginalInfo{}, err
	}
	return o.info, nil
}

type originalImage struct {
	data        []byte
	cfg         image.Config
	format      string
	orientation int
	info        OriginalInfo
}

func readOriginal(ctx context.Context, input io.Reader) (originalImage, error) {
	if err := ctx.Err(); err != nil {
		return originalImage{}, err
	}
	data, err := io.ReadAll(io.LimitReader(contextReader{ctx, input}, MaxInputBytes+1))
	if err != nil {
		return originalImage{}, fmt.Errorf("read image: %w", err)
	}
	if len(data) > MaxInputBytes {
		return originalImage{}, ErrLimitExceeded
	}
	format := ""
	switch {
	case bytes.HasPrefix(data, []byte{0xff, 0xd8}):
		format = "jpeg"
	case bytes.HasPrefix(data, []byte("\x89PNG\r\n\x1a\n")):
		format = "png"
	default:
		return originalImage{}, ErrUnsupportedFormat
	}
	cfg, detected, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return originalImage{}, fmt.Errorf("%w: %w", ErrInvalidImage, err)
	}
	if detected != format {
		return originalImage{}, ErrUnsupportedFormat
	}
	if !validSize(cfg.Width, cfg.Height) {
		return originalImage{}, ErrLimitExceeded
	}
	orientation, err := readOrientation(ctx, data, format)
	if err != nil {
		return originalImage{}, fmt.Errorf("%w: %w", ErrInvalidImage, err)
	}
	w, h := cfg.Width, cfg.Height
	if orientation >= 5 {
		w, h = h, w
	}
	o := originalImage{data: data, cfg: cfg, format: format, orientation: orientation, info: OriginalInfo{Width: w, Height: h, ContentType: "image/" + format, SizeBytes: int64(len(data)), Orientation: orientation}}
	if o.workingBytes(0, 0) > MaxWorkingBytes {
		return originalImage{}, ErrLimitExceeded
	}
	return o, nil
}

func (o originalImage) workingBytes(outW, outH int) int64 {
	// Account for PNG16 scanlines and progressive JPEG coefficient buffers.
	bytesPerPixel := int64(8)
	if o.format == "jpeg" {
		bytesPerPixel = 15
		if _, cmyk := o.cfg.ColorModel.Convert(color.RGBA{}).(color.CMYK); cmyk {
			bytesPerPixel = 20
		}
	}
	return int64(o.cfg.Width)*int64(o.cfg.Height)*bytesPerPixel + int64(o.cfg.Width)*16 + int64(outW)*int64(outH)*4 + int64(outW)*8 + int64(len(o.data))*2 + (16 << 20)
}

func (o originalImage) decode(ctx context.Context) (image.Image, error) {
	decoded, _, err := image.Decode(contextReader{ctx, bytes.NewReader(o.data)})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidImage, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if decoded.Bounds().Dx() != o.cfg.Width || decoded.Bounds().Dy() != o.cfg.Height {
		return nil, ErrInvalidImage
	}
	if o.orientation != 1 {
		return orientedImage{decoded, o.orientation, image.Rect(0, 0, o.info.Width, o.info.Height)}, nil
	}
	return decoded, nil
}
