package service

import (
	"fmt"
	"os"

	"github.com/akhmed9505/image-processor/internal/dto"
	"github.com/akhmed9505/image-processor/internal/model"
	"github.com/google/uuid"
)

func (s *Service) CreateImage(data []byte, imageData dto.Message) (*uuid.UUID, error) {
	format, err := parseFormat(imageData.ContentType)
	if err != nil {
		return nil, err
	}

	if !isCorrectTask(imageData.Task) {
		return nil, ErrInvalidTask
	}

	id := uuid.New()
	image := model.Image{
		ID:     id,
		Format: format,
		Status: "in progress",
	}

	fileName := id.String() + "." + format
	imageData.ID = id
	imageData.FileName = fileName

	if err := os.WriteFile(originDirName+"/"+fileName, data, 0666); err != nil {
		return nil, fmt.Errorf("could not save image: %w", err)
	}

	filePath := originDirName + "/" + fileName
	if err := s.fileStorage.SaveImage(fileName, filePath, "images"); err != nil {
		return nil, err
	}

	if err := s.queue.ProduceMessage(imageData); err != nil {
		return nil, err
	}

	if err := s.storage.CreateImage(image); err != nil {
		return nil, err
	}

	return &id, nil
}
