package handler

import (
	"github.com/akhmed9505/image-processor/internal/dto"
	"github.com/akhmed9505/image-processor/internal/model"
	"github.com/google/uuid"
)

type ImageProcessorService interface {
	ProcessImage(dto.Message) error
	GetImageStatus(uuid.UUID) (*model.Image, error)
	GetImageById(uuid.UUID) (string, error)
	CreateImage([]byte, dto.Message) (*uuid.UUID, error)
	DeleteImage(uuid.UUID) error
}

type Handler struct {
	service ImageProcessorService
}

func New(service ImageProcessorService) *Handler {
	return &Handler{
		service: service,
	}
}
