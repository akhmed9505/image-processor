package service

import (
	"errors"
	"log"
	"os"

	"github.com/akhmed9505/image-processor/internal/dto"
	"github.com/akhmed9505/image-processor/internal/model"
	"github.com/google/uuid"
)

var (
	ErrInvalidImageFormat = errors.New("invalid image format, must be in (jpg, png, gif)")
	ErrInvalidTask        = errors.New("invalid task, must be in(resize, watermark, miniature generating)")
	ErrNotProcessdYet     = errors.New("image is not ready yet")
)

var (
	originDirName    string
	processedDirName string
)

type Storage interface {
	CreateImage(model.Image) error
	GetImageInfo(uuid.UUID) (*model.Image, error)
	DeleteImage(uuid.UUID) error
	UpdateImageStatus(uuid.UUID, string) error
}

type FileStorage interface {
	SaveImage(string, string, string) error
	GetImage(string, string, string) error
	DeleteImages(string, ...string) error
}

type Queue interface {
	ProduceMessage(dto.Message) error
	ConsumeMessage() (*dto.Message, error)
}

type Service struct {
	storage     Storage
	fileStorage FileStorage
	queue       Queue
}

func New(storage Storage, fileStorage FileStorage, queue Queue) *Service {
	name, err := os.MkdirTemp(".", "images")
	if err != nil {
		log.Fatal("could not create temporary directory to store images: ", err)
	}

	processed, err := os.MkdirTemp(".", "processed")
	if err != nil {
		log.Fatal("could not create temporary directory to store processed images: ", err)
	}

	originDirName = name
	processedDirName = processed

	return &Service{
		storage:     storage,
		fileStorage: fileStorage,
		queue:       queue,
	}
}
