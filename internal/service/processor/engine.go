// Package processor transforms JPEG and PNG images independently of services and storage.
package processor

import (
	"context"
	"errors"
	"fmt"
	"image"
	"image/color"
	stddraw "image/draw"
	"image/jpeg"
	"image/png"
	"io"
	"math"

	"github.com/akhmed9505/image-processor/internal/domain"
	"golang.org/x/image/draw"
	"golang.org/x/image/font"
	"golang.org/x/image/font/gofont/goregular"
	"golang.org/x/image/font/opentype"
	"golang.org/x/image/math/fixed"
)

// Limits cover encoded files, pixel counts, and estimated working buffers.
const (
	MaxInputBytes    = 20 << 20
	MaxOutputBytes   = 128 << 20
	MaxDimension     = 10_000
	MaxThumbnailSize = 2048
	MaxPixels        = 25_000_000
	MaxWorkingBytes  = 512 << 20
)

var (
	ErrUnsupportedFormat = errors.New("unsupported image format")
	ErrInvalidImage      = errors.New("invalid image")
	ErrLimitExceeded     = errors.New("image resource limit exceeded")
)

// Result describes the encoded output.
type Result struct {
	Width, Height int
	ContentType   string
	SizeBytes     int64
}

// Process writes one result; callers must discard partial output on error.
// Reader and writer must support interruption themselves if their I/O can block.
func Process(ctx context.Context, input io.Reader, output io.Writer, operation domain.Operation, params domain.OperationParams) (Result, error) {
	if err := params.Validate(operation); err != nil {
		return Result{}, err
	}
	if operation == domain.OperationThumbnail && params.Thumbnail.Size > MaxThumbnailSize {
		return Result{}, ErrLimitExceeded
	}
	if operation == domain.OperationResize && (params.Resize.Width > MaxDimension || params.Resize.Height > MaxDimension) {
		return Result{}, ErrLimitExceeded
	}
	original, err := readOriginal(ctx, input)
	if err != nil {
		return Result{}, err
	}
	w, h := original.info.Width, original.info.Height
	crop := image.Rect(0, 0, w, h)
	outW, outH := w, h
	switch operation {
	case domain.OperationResize:
		ratio := math.Min(1, math.Min(float64(params.Resize.Width)/float64(w), float64(params.Resize.Height)/float64(h)))
		outW = max(1, int(math.Floor(float64(w)*ratio)))
		outH = max(1, int(math.Floor(float64(h)*ratio)))
	case domain.OperationThumbnail:
		outW = params.Thumbnail.Size
		outH = outW
		side := min(w, h)
		x, y := (w-side)/2, (h-side)/2
		crop = image.Rect(x, y, x+side, y+side)
	}
	if !validSize(outW, outH) {
		return Result{}, ErrLimitExceeded
	}
	if original.workingBytes(outW, outH) > MaxWorkingBytes {
		return Result{}, ErrLimitExceeded
	}
	source, err := original.decode(ctx)
	if err != nil {
		return Result{}, err
	}
	dst := image.NewNRGBA(image.Rect(0, 0, outW, outH))
	// Clipping the destination preserves one scale transform without a full-size intermediate.
	for y := 0; y < outH; y += 32 {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		band := image.Rect(0, y, outW, min(y+32, outH))
		if outW == crop.Dx() && outH == crop.Dy() {
			stddraw.Draw(dst, band, source, crop.Min.Add(band.Min), stddraw.Src)
		} else {
			draw.ApproxBiLinear.Scale(dst.SubImage(band).(*image.NRGBA), dst.Bounds(), source, crop, draw.Src, nil)
		}
	}
	if operation == domain.OperationWatermark {
		if err := watermark(dst); err != nil {
			return Result{}, err
		}
	}
	writer := &contextWriter{ctx: ctx, dst: output}
	if original.format == "jpeg" {
		err = jpeg.Encode(writer, dst, &jpeg.Options{Quality: 90})
	} else {
		err = png.Encode(writer, dst)
	}
	if err != nil {
		return Result{}, fmt.Errorf("encode image: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	contentType := original.info.ContentType
	return Result{outW, outH, contentType, writer.n}, nil
}

func validSize(w, h int) bool {
	return w > 0 && h > 0 && w <= MaxDimension && h <= MaxDimension && w <= MaxPixels/h
}

type contextReader struct {
	ctx context.Context
	src io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if e := r.ctx.Err(); e != nil {
		return 0, e
	}
	return r.src.Read(p)
}

type contextWriter struct {
	ctx context.Context
	dst io.Writer
	n   int64
}

func (w *contextWriter) Write(p []byte) (int, error) {
	if e := w.ctx.Err(); e != nil {
		return 0, e
	}
	if int64(len(p)) > MaxOutputBytes-w.n {
		return 0, ErrLimitExceeded
	}
	n, e := w.dst.Write(p)
	w.n += int64(n)
	if e == nil && n != len(p) {
		e = io.ErrShortWrite
	}
	return n, e
}

type orientedImage struct {
	source      image.Image
	orientation int
	bounds      image.Rectangle
}

func (o orientedImage) ColorModel() color.Model { return o.source.ColorModel() }
func (o orientedImage) Bounds() image.Rectangle { return o.bounds }
func (o orientedImage) At(x, y int) color.Color { x, y = o.point(x, y); return o.source.At(x, y) }

// RGBA64At avoids allocating a boxed color for every oriented pixel.
func (o orientedImage) RGBA64At(x, y int) color.RGBA64 {
	x, y = o.point(x, y)
	if src, ok := o.source.(image.RGBA64Image); ok {
		return src.RGBA64At(x, y)
	}
	r, g, b, a := o.source.At(x, y).RGBA()
	return color.RGBA64{R: uint16(r), G: uint16(g), B: uint16(b), A: uint16(a)}
}
func (o orientedImage) point(x, y int) (int, int) {
	w, h := o.source.Bounds().Dx(), o.source.Bounds().Dy()
	switch o.orientation {
	case 2:
		x = w - 1 - x
	case 3:
		x, y = w-1-x, h-1-y
	case 4:
		y = h - 1 - y
	case 5:
		x, y = y, x
	case 6:
		x, y = y, h-1-x
	case 7:
		x, y = w-1-y, h-1-x
	case 8:
		x, y = w-1-y, x
	}
	return x + o.source.Bounds().Min.X, y + o.source.Bounds().Min.Y
}

func watermark(dst *image.NRGBA) error {
	f, err := opentype.Parse(goregular.TTF)
	if err != nil {
		return err
	}
	w, h := dst.Bounds().Dx(), dst.Bounds().Dy()
	margin := max(1, min(w, h)/40)
	for size := min(max(1, h/8), max(1, w/30)); size >= 1; size-- {
		face, err := opentype.NewFace(f, &opentype.FaceOptions{Size: float64(size), DPI: 72, Hinting: font.HintingFull})
		if err != nil {
			return err
		}
		bounds, advance := font.BoundString(face, "ImageProcessor")
		x := w - margin - advance.Ceil() - 1
		y := h - margin - bounds.Max.Y.Ceil() - 1
		if x+bounds.Min.X.Floor() < margin || y+bounds.Min.Y.Floor() < margin {
			face.Close()
			continue
		}
		d := font.Drawer{Dst: dst, Face: face, Src: image.NewUniform(color.NRGBA{A: 128}), Dot: fixed.P(x+1, y+1)}
		d.DrawString("ImageProcessor")
		d.Src = image.NewUniform(color.NRGBA{R: 255, G: 255, B: 255, A: 128})
		d.Dot = fixed.P(x, y)
		d.DrawString("ImageProcessor")
		face.Close()
		break
	}
	return nil
}
