package service

import (
	"fmt"

	"github.com/akhmed9505/image-processor/internal/model"
	"github.com/google/uuid"
)

func (s *Service) GetImageStatus(id uuid.UUID) (*model.Image, error) {
	return s.storage.GetImageInfo(id)
}

func (s *Service) GetImageById(id uuid.UUID) (string, error) {
	imageInfo, err := s.storage.GetImageInfo(id)
	if err != nil {
		return "", err
	}

	if imageInfo.Status == "in progress" {
		return "", ErrNotProcessdYet
	}

	fileName := id.String() + "." + imageInfo.Format
	filePath := processedDirName + "/" + fileName
	if err := s.fileStorage.GetImage(fileName, filePath, "processed"); err != nil {
		return "", fmt.Errorf("could not get image from file storage: %w", err)
	}

	return filePath, nil
}
