package processor

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"image"
	"io"
	"testing"

	"github.com/akhmed9505/image-processor/internal/domain"
)

func TestValidateOriginalMetadata(t *testing.T) {
	for _, format := range []string{"jpeg", "png"} {
		for orientation := 1; orientation <= 8; orientation++ {
			data := withOrientation(encode(t, stripes(), format), format, orientation, false)
			got, e := ValidateOriginal(context.Background(), bytes.NewReader(data))
			if e != nil {
				t.Fatal(e)
			}
			w, h := 120, 80
			if orientation >= 5 {
				w, h = h, w
			}
			if got.Width != w || got.Height != h || got.ContentType != "image/"+format || got.Orientation != orientation || got.SizeBytes != int64(len(data)) {
				t.Fatalf("incorrect original metadata: %+v", got)
			}
		}
	}
}

func TestOriginalInputByteBoundary(t *testing.T) {
	data := encode(t, stripes(), "jpeg")
	exact := io.MultiReader(bytes.NewReader(data), io.LimitReader(zeroReader{}, MaxInputBytes-int64(len(data))))
	info, e := ValidateOriginal(context.Background(), exact)
	if e != nil || info.SizeBytes != MaxInputBytes {
		t.Fatalf("20 MiB input rejected: %+v %v", info, e)
	}
	for _, n := range []int64{MaxInputBytes + 1, MaxInputBytes + 1024} {
		input := io.MultiReader(bytes.NewReader(data), io.LimitReader(zeroReader{}, n-int64(len(data))))
		if info, e := ValidateOriginal(context.Background(), input); !errors.Is(e, ErrLimitExceeded) || info != (OriginalInfo{}) {
			t.Fatalf("oversize original accepted: %+v %v", info, e)
		}
	}
}

func TestOriginalDimensionBoundaries(t *testing.T) {
	for _, size := range []image.Point{{MaxDimension, 1}, {1, MaxDimension}} {
		data := encode(t, image.NewNRGBA(image.Rect(0, 0, size.X, size.Y)), "png")
		info, e := ValidateOriginal(context.Background(), bytes.NewReader(data))
		if e != nil || info.Width != size.X || info.Height != size.Y {
			t.Fatalf("boundary dimensions: %+v %v", info, e)
		}
	}
	for _, size := range []image.Point{{MaxDimension + 1, 1}, {1, MaxDimension + 1}, {5001, 5000}, {5000, 5001}} {
		data := encode(t, stripes(), "png")
		binary.BigEndian.PutUint32(data[16:], uint32(size.X))
		binary.BigEndian.PutUint32(data[20:], uint32(size.Y))
		binary.BigEndian.PutUint32(data[29:], crc32.ChecksumIEEE(data[12:29]))
		if _, e := ValidateOriginal(context.Background(), bytes.NewReader(data)); !errors.Is(e, ErrLimitExceeded) {
			t.Fatalf("bad dimension %v: %v", size, e)
		}
	}
	if !validSize(5000, 5000) || !validSize(10000, 2500) || validSize(10000, 2501) {
		t.Fatal("25 MP boundary incorrect")
	}
}

func TestThumbnailBoundaries(t *testing.T) {
	data := encode(t, stripes(), "png")
	for _, size := range []int{1, MaxThumbnailSize} {
		img, info, _ := run(t, data, domain.OperationThumbnail, domain.OperationParams{Thumbnail: domain.ThumbnailParams{Size: size}})
		if info.Width != size || info.Height != size || img.Bounds().Dx() != size {
			t.Fatal("thumbnail boundary")
		}
	}
	for _, size := range []int{-1, 0, MaxThumbnailSize + 1} {
		var output bytes.Buffer
		_, e := Process(context.Background(), bytes.NewReader(data), &output, domain.OperationThumbnail, domain.OperationParams{Thumbnail: domain.ThumbnailParams{Size: size}})
		if e == nil || output.Len() != 0 {
			t.Fatalf("thumbnail size %d accepted", size)
		}
	}
	for _, size := range []domain.ResizeParams{{Width: MaxDimension + 1, Height: 1}, {Width: 1, Height: MaxDimension + 1}} {
		if _, e := Process(context.Background(), bytes.NewReader(data), io.Discard, domain.OperationResize, domain.OperationParams{Resize: size}); !errors.Is(e, ErrLimitExceeded) {
			t.Fatal("resize bounds accepted", e)
		}
	}
}

func TestOutputByteBoundary(t *testing.T) {
	w := &contextWriter{ctx: context.Background(), dst: io.Discard}
	chunk := make([]byte, 64<<10)
	for w.n < MaxOutputBytes {
		if _, e := w.Write(chunk); e != nil {
			t.Fatalf("output stopped at %d bytes: %v", w.n, e)
		}
	}
	if w.n != 128<<20 {
		t.Fatal("incorrect output limit")
	}
	if _, e := w.Write([]byte{1}); !errors.Is(e, ErrLimitExceeded) || w.n != MaxOutputBytes {
		t.Fatal("output limit not enforced", e)
	}
}

func TestValidateOriginalErrors(t *testing.T) {
	valid := encode(t, stripes(), "png")
	for _, tc := range []struct {
		data []byte
		want error
	}{{nil, ErrUnsupportedFormat}, {[]byte("GIF89a"), ErrUnsupportedFormat}, {valid[:len(valid)/2], ErrInvalidImage}, {withOrientation(valid, "png", 9, false), ErrInvalidImage}} {
		if info, e := ValidateOriginal(context.Background(), bytes.NewReader(tc.data)); !errors.Is(e, tc.want) || info != (OriginalInfo{}) {
			t.Fatalf("bad original: %+v %v", info, e)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := ValidateOriginal(ctx, bytes.NewReader(valid)); !errors.Is(e, context.Canceled) {
		t.Fatal("original validation ignored cancellation", e)
	}
	cause := errors.New("source failure")
	if _, e := ValidateOriginal(context.Background(), errorReader{cause}); !errors.Is(e, cause) {
		t.Fatal("source error lost", e)
	}
}
