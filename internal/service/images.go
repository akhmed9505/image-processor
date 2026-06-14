package service

import (
	"fmt"
	"image"
	"image/draw"
	"os"

	"github.com/akhmed9505/image-processor/internal/dto"
)

const (
	Resize    = "resize"
	Watermark = "watermark"
	Thumbnail = "miniature generating"
)

func (s *Service) ProcessImage(config dto.Message) error {
	format, err := parseFormat(config.ContentType)
	if err != nil {
		return err
	}

	switch config.Task {
	case Resize:
		return s.resizeImage(config.FileName, format, config.Resize.Width, config.Resize.Height)
	case Watermark:
		return s.addWatermark(config.FileName, format, config.WatermarkText)
	case Thumbnail:
		return s.createThumbnail(config.FileName, format)
	}

	return fmt.Errorf("invalid task")
}

func (s *Service) addWatermark(fileName, format, watermarkText string) error {
	input, err := os.Open(originDirName + "/" + fileName)
	if err != nil {
		return fmt.Errorf("no file with name: %s", fileName)
	}

	img, err := decode(format, input)
	if err != nil {
		return fmt.Errorf("could not read image: %w", err)
	}

	rect := img.Bounds()
	x := rect.Min.X + rect.Dx()/2
	y := rect.Min.Y + rect.Dy() - 30

	rgbaImg := image.NewRGBA(img.Bounds())
	draw.Draw(rgbaImg, rgbaImg.Bounds(), img, image.Point{0, 0}, draw.Src)

	if err := addLabel(rgbaImg, x, y, watermarkText, 60); err != nil {
		return fmt.Errorf("could not draw watermark: %w", err)
	}

	output, err := os.OpenFile(processedDirName+"/"+fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("could not open file to store processed image")
	}

	return encode(format, rgbaImg, output)
}

func (s *Service) createThumbnail(fileName, format string) error {
	return s.resizeImage(fileName, format, 200, 200)
}

func (s *Service) resizeImage(fileName, format string, width, height int) error {
	input, err := os.Open(originDirName + "/" + fileName)
	if err != nil {
		return fmt.Errorf("no file with name: %s", fileName)
	}

	output, err := os.OpenFile(processedDirName+"/"+fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("could not open file to store processed image")
	}

	return resize(format, input, output, width, height)
}
