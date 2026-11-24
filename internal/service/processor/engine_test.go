package processor

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"io"
	"math"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
)

func encode(t testing.TB, img image.Image, format string) []byte {
	t.Helper()
	var b bytes.Buffer
	var e error
	if format == "jpeg" {
		e = jpeg.Encode(&b, img, &jpeg.Options{Quality: 100})
	} else {
		e = png.Encode(&b, img)
	}
	if e != nil {
		t.Fatal(e)
	}
	return b.Bytes()
}
func run(t *testing.T, data []byte, op domain.Operation, p domain.OperationParams) (image.Image, Result, []byte) {
	t.Helper()
	var b bytes.Buffer
	r, e := Process(context.Background(), bytes.NewReader(data), &b, op, p)
	if e != nil {
		t.Fatal(e)
	}
	img, format, e := image.Decode(bytes.NewReader(b.Bytes()))
	if e != nil || "image/"+format != r.ContentType || r.Width != img.Bounds().Dx() || r.Height != img.Bounds().Dy() || r.SizeBytes != int64(b.Len()) {
		t.Fatal("incorrect output metadata", r, e)
	}
	return img, r, b.Bytes()
}
func stripes() image.Image {
	img := image.NewNRGBA(image.Rect(0, 0, 120, 80))
	for y := 0; y < 80; y++ {
		for x := 0; x < 120; x++ {
			c := color.NRGBA{G: 255, A: 255}
			if x < 20 {
				c = color.NRGBA{R: 255, A: 255}
			}
			if x >= 100 {
				c = color.NRGBA{B: 255, A: 255}
			}
			img.SetNRGBA(x, y, c)
		}
	}
	return img
}
func TestOperations(t *testing.T) {
	for _, format := range []string{"png", "jpeg"} {
		t.Run(format, func(t *testing.T) {
			data := encode(t, stripes(), format)
			_, r, _ := run(t, data, domain.OperationResize, domain.OperationParams{Resize: domain.ResizeParams{Width: 60, Height: 60}})
			if r.Width != 60 || r.Height != 40 {
				t.Fatal("resize did not fit", r)
			}
			_, r, _ = run(t, data, domain.OperationResize, domain.OperationParams{Resize: domain.ResizeParams{Width: MaxDimension, Height: MaxDimension}})
			if r.Width != 120 || r.Height != 80 {
				t.Fatal("resize upscaled")
			}
			for _, size := range []int{1, 40, 160} {
				img, r, _ := run(t, data, domain.OperationThumbnail, domain.OperationParams{Thumbnail: domain.ThumbnailParams{Size: size}})
				if r.Width != size || r.Height != size {
					t.Fatal("thumbnail dimensions")
				}
				red, green, blue, _ := img.At(0, 0).RGBA()
				if green < 60000 || red > 4000 || blue > 4000 {
					t.Fatal("thumbnail was not center-cropped")
				}
			}
			_, r, _ = run(t, data, domain.OperationWatermark, domain.OperationParams{})
			if r.Width != 120 || r.Height != 80 {
				t.Fatal("watermark changed dimensions")
			}
		})
	}
}
func TestWatermarkAndAlpha(t *testing.T) {
	img := image.NewNRGBA(image.Rect(0, 0, 600, 300))
	for y := 0; y < 300; y++ {
		for x := 0; x < 600; x++ {
			img.SetNRGBA(x, y, color.NRGBA{R: 40, G: 80, B: 120, A: 100})
		}
	}
	got, _, _ := run(t, encode(t, img, "png"), domain.OperationWatermark, domain.OperationParams{})
	if color.NRGBAModel.Convert(got.At(0, 0)).(color.NRGBA) != img.NRGBAAt(0, 0) {
		t.Fatal("untouched alpha changed")
	}
	white, shadow := 0, 0
	for y := 0; y < 300; y++ {
		for x := 0; x < 600; x++ {
			c := color.NRGBAModel.Convert(got.At(x, y)).(color.NRGBA)
			if c == img.NRGBAAt(x, y) {
				continue
			}
			if x < 300 || y < 225 {
				t.Fatal("watermark not in bottom right")
			}
			if c.R > 40 && c.G > 80 && c.A > 100 && c.A < 255 {
				white++
			}
			if c.B < 120 && c.A > 100 {
				shadow++
			}
		}
	}
	if white == 0 || shadow == 0 {
		t.Fatal("missing translucent white text or shadow")
	}
	for _, op := range []domain.Operation{domain.OperationResize, domain.OperationThumbnail} {
		p := domain.OperationParams{}
		if op == domain.OperationResize {
			p.Resize = domain.ResizeParams{Width: 300, Height: 200}
		} else {
			p.Thumbnail = domain.ThumbnailParams{Size: 80}
		}
		out, _, _ := run(t, encode(t, img, "png"), op, p)
		_, _, _, alpha := out.At(0, 0).RGBA()
		if alpha != 100*257 {
			t.Fatal("PNG alpha lost")
		}
	}
	transparent := image.NewNRGBA(image.Rect(0, 0, 2, 1))
	transparent.SetNRGBA(1, 0, color.NRGBA{R: 255, A: 255})
	out, _, _ := run(t, encode(t, transparent, "png"), domain.OperationThumbnail, domain.OperationParams{Thumbnail: domain.ThumbnailParams{Size: 4}})
	_, _, _, a := out.At(0, 0).RGBA()
	if a != 0 {
		t.Fatal("transparent pixel became opaque")
	}
	tiny := image.NewNRGBA(image.Rect(0, 0, 1, 1))
	out, _, _ = run(t, encode(t, tiny, "png"), domain.OperationWatermark, domain.OperationParams{})
	if _, _, _, a := out.At(0, 0).RGBA(); a != 0 {
		t.Fatal("tiny watermark must be skipped")
	}
}
func exifData(value int, big bool) []byte {
	data := make([]byte, 26)
	var order binary.ByteOrder = binary.LittleEndian
	copy(data, "II")
	if big {
		order = binary.BigEndian
		copy(data, "MM")
	}
	order.PutUint16(data[2:], 42)
	order.PutUint32(data[4:], 8)
	order.PutUint16(data[8:], 1)
	order.PutUint16(data[10:], 0x112)
	order.PutUint16(data[12:], 3)
	order.PutUint32(data[14:], 1)
	order.PutUint16(data[18:], uint16(value))
	return data
}
func withOrientation(data []byte, format string, value int, big bool) []byte {
	meta := exifData(value, big)
	if format == "jpeg" {
		meta = append([]byte("Exif\x00\x00"), meta...)
		segment := []byte{0xff, 0xe1, 0, 0}
		binary.BigEndian.PutUint16(segment[2:], uint16(len(meta)+2))
		segment = append(segment, meta...)
		return append(append(append([]byte{}, data[:2]...), segment...), data[2:]...)
	}
	chunk := make([]byte, len(meta)+12)
	binary.BigEndian.PutUint32(chunk, uint32(len(meta)))
	copy(chunk[4:], "eXIf")
	copy(chunk[8:], meta)
	binary.BigEndian.PutUint32(chunk[len(chunk)-4:], crc32.ChecksumIEEE(chunk[4:len(chunk)-4]))
	return append(append(append([]byte{}, data[:33]...), chunk...), data[33:]...)
}
func TestEXIFOrientation(t *testing.T) {
	src := image.NewNRGBA(image.Rect(0, 0, 80, 40))
	colors := []color.NRGBA{{R: 255, A: 255}, {G: 255, A: 255}, {B: 255, A: 255}, {R: 255, G: 255, A: 255}}
	for y := 0; y < 40; y++ {
		for x := 0; x < 80; x++ {
			idx := 0
			if x >= 40 {
				idx++
			}
			if y >= 20 {
				idx += 2
			}
			src.SetNRGBA(x, y, colors[idx])
		}
	}
	// Corner order: upper-left, upper-right, lower-left, lower-right.
	want := [][]int{{0, 1, 2, 3}, {1, 0, 3, 2}, {3, 2, 1, 0}, {2, 3, 0, 1}, {0, 2, 1, 3}, {2, 0, 3, 1}, {3, 1, 2, 0}, {1, 3, 0, 2}}
	for _, format := range []string{"png", "jpeg"} {
		for value := 1; value <= 8; value++ {
			for _, big := range []bool{false, true} {
				out, r, encoded := run(t, withOrientation(encode(t, src, format), format, value, big), domain.OperationResize, domain.OperationParams{Resize: domain.ResizeParams{Width: 100, Height: 100}})
				w, h := 80, 40
				if value >= 5 {
					w, h = h, w
				}
				if r.Width != w || r.Height != h {
					t.Fatal("orientation dimensions")
				}
				points := []image.Point{{5, 5}, {w - 6, 5}, {5, h - 6}, {w - 6, h - 6}}
				for i, p := range points {
					c := color.NRGBAModel.Convert(out.At(p.X, p.Y)).(color.NRGBA)
					expected := colors[want[value-1][i]]
					if abs(int(c.R)-int(expected.R)) > 5 || abs(int(c.G)-int(expected.G)) > 5 || abs(int(c.B)-int(expected.B)) > 5 {
						t.Fatalf("format=%s orientation=%d corner=%d color=%v", format, value, i, c)
					}
				}
				orientation, e := readOrientation(context.Background(), encoded, format)
				if e != nil || orientation != 1 {
					t.Fatal("EXIF emitted again", e)
				}
			}
		}
	}
}
func abs(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
func TestJPEGQuality(t *testing.T) {
	src := stripes()
	data := encode(t, src, "jpeg")
	decoded, _, _ := image.Decode(bytes.NewReader(data))
	dst := image.NewNRGBA(decoded.Bounds())
	for y := 0; y < 80; y++ {
		for x := 0; x < 120; x++ {
			dst.Set(x, y, decoded.At(x, y))
		}
	}
	var expected bytes.Buffer
	jpeg.Encode(&expected, dst, &jpeg.Options{Quality: 90})
	_, _, encoded := run(t, data, domain.OperationResize, domain.OperationParams{Resize: domain.ResizeParams{Width: 120, Height: 80}})
	if !bytes.Equal(encoded, expected.Bytes()) {
		t.Fatal("JPEG quality is not 90")
	}
}
func TestInvalidImagesAndLimits(t *testing.T) {
	valid := encode(t, stripes(), "png")
	huge := append([]byte{}, valid...)
	binary.BigEndian.PutUint32(huge[16:], 5001)
	binary.BigEndian.PutUint32(huge[20:], 5000)
	binary.BigEndian.PutUint32(huge[29:], crc32.ChecksumIEEE(huge[12:29]))
	wide := append([]byte{}, valid...)
	binary.BigEndian.PutUint32(wide[16:], MaxPixels)
	binary.BigEndian.PutUint32(wide[20:], 1)
	binary.BigEndian.PutUint32(wide[29:], crc32.ChecksumIEEE(wide[12:29]))
	for _, tc := range []struct {
		data []byte
		want error
	}{{nil, ErrUnsupportedFormat}, {[]byte("GIF89a"), ErrUnsupportedFormat}, {valid[:20], ErrInvalidImage}, {valid[:len(valid)/2], ErrInvalidImage}, {huge, ErrLimitExceeded}, {wide, ErrLimitExceeded}, {encode(t, stripes(), "jpeg")[:30], ErrInvalidImage}, {withOrientation(valid, "png", 9, false), ErrInvalidImage}} {
		var b bytes.Buffer
		_, e := Process(context.Background(), bytes.NewReader(tc.data), &b, domain.OperationResize, domain.OperationParams{Resize: domain.ResizeParams{Width: 100, Height: 100}})
		if !errors.Is(e, tc.want) || b.Len() != 0 {
			t.Fatalf("got %v want %v", e, tc.want)
		}
	}
	if !validSize(5000, 5000) || validSize(math.MaxInt, math.MaxInt) || validSize(0, 1) {
		t.Fatal("pixel boundary")
	}
	for _, size := range []int{5001, math.MaxInt} {
		_, e := Process(context.Background(), bytes.NewReader(valid), io.Discard, domain.OperationThumbnail, domain.OperationParams{Thumbnail: domain.ThumbnailParams{Size: size}})
		if !errors.Is(e, ErrLimitExceeded) {
			t.Fatal("oversize output accepted", e)
		}
	}
	_, e := Process(context.Background(), io.LimitReader(zeroReader{}, MaxInputBytes+1), io.Discard, domain.OperationWatermark, domain.OperationParams{})
	if !errors.Is(e, ErrLimitExceeded) {
		t.Fatal("input byte limit", e)
	}
	_, e = Process(context.Background(), bytes.NewReader(valid), io.Discard, domain.Operation("other"), domain.OperationParams{})
	if !errors.Is(e, domain.ErrInvalidOperation) {
		t.Fatal(e)
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) { clear(p); return len(p), nil }

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

type cancellingReader struct {
	src    io.Reader
	cancel context.CancelFunc
}

func (r cancellingReader) Read(p []byte) (int, error) { n, e := r.src.Read(p); r.cancel(); return n, e }
func TestContextAndIOErrors(t *testing.T) {
	data := encode(t, stripes(), "png")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, e := Process(ctx, bytes.NewReader(data), io.Discard, domain.OperationWatermark, domain.OperationParams{})
	if !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	_, e = Process(ctx, cancellingReader{bytes.NewReader(data), cancel}, io.Discard, domain.OperationWatermark, domain.OperationParams{})
	if !errors.Is(e, context.Canceled) {
		t.Fatal("mid-read cancellation", e)
	}
	cause := errors.New("write failure")
	_, e = Process(context.Background(), bytes.NewReader(data), failingWriter{cause}, domain.OperationWatermark, domain.OperationParams{})
	if !errors.Is(e, cause) {
		t.Fatal("write cause lost", e)
	}
	writer := &contextWriter{ctx: context.Background(), dst: io.Discard, n: MaxOutputBytes}
	if _, e := writer.Write([]byte{1}); !errors.Is(e, ErrLimitExceeded) {
		t.Fatal(e)
	}
}

func TestProcess25MPMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("25 MP memory check")
	}
	file, e := os.CreateTemp(t.TempDir(), "original-*.png")
	if e != nil {
		t.Fatal(e)
	}
	defer file.Close()
	src := image.NewNRGBA64(image.Rect(0, 0, 5000, 5000))
	if e := png.Encode(file, src); e != nil {
		t.Fatal(e)
	}
	src = nil
	data, e := os.ReadFile(file.Name())
	if e != nil {
		t.Fatal(e)
	}
	data = withOrientation(data, "png", 6, false)
	if e := file.Truncate(0); e != nil {
		t.Fatal(e)
	}
	if _, e := file.Seek(0, 0); e != nil {
		t.Fatal(e)
	}
	if _, e := file.Write(data); e != nil {
		t.Fatal(e)
	}
	data = nil
	runtime.GC()
	var start runtime.MemStats
	runtime.ReadMemStats(&start)
	var peak atomic.Uint64
	peak.Store(start.HeapAlloc)
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				var stats runtime.MemStats
				runtime.ReadMemStats(&stats)
				if stats.HeapAlloc > peak.Load() {
					peak.Store(stats.HeapAlloc)
				}
			case <-done:
				return
			}
		}
	}()
	_, e = file.Seek(0, 0)
	if e == nil {
		_, e = Process(context.Background(), file, io.Discard, domain.OperationWatermark, domain.OperationParams{})
	}
	close(done)
	<-stopped
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("25 MP PNG16 orientation 6 peak live heap: %.1f MiB (baseline %.1f MiB)", float64(peak.Load())/(1<<20), float64(start.HeapAlloc)/(1<<20))
	if peak.Load() > 512<<20 {
		t.Fatal("25 MP processing exceeded 512 MiB heap budget")
	}
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

type cancellingWriter struct{ cancel context.CancelFunc }

func (w cancellingWriter) Write(p []byte) (int, error) { w.cancel(); return len(p), nil }
func TestReadErrorsAndEncodingCancellation(t *testing.T) {
	cause := errors.New("read failure")
	if _, e := Process(context.Background(), errorReader{cause}, io.Discard, domain.OperationWatermark, domain.OperationParams{}); !errors.Is(e, cause) {
		t.Fatal("read cause lost", e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, e := Process(ctx, bytes.NewReader(encode(t, stripes(), "png")), cancellingWriter{cancel}, domain.OperationWatermark, domain.OperationParams{})
	if !errors.Is(e, context.Canceled) {
		t.Fatal("encoding did not honor cancellation", e)
	}
}
