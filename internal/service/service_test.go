package service

import (
	"errors"
	"image"
	"image/color"
	"os"
	"testing"

	"github.com/akhmed9505/image-processor/internal/dto"
	"github.com/akhmed9505/image-processor/internal/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

type mockStorage struct {
	createImageFunc       func(model.Image) error
	getImageInfoFunc      func(uuid.UUID) (*model.Image, error)
	deleteImageFunc       func(uuid.UUID) error
	updateImageStatusFunc func(uuid.UUID, string) error
}

func (m *mockStorage) CreateImage(img model.Image) error {
	if m.createImageFunc != nil {
		return m.createImageFunc(img)
	}
	return nil
}

func (m *mockStorage) GetImageInfo(id uuid.UUID) (*model.Image, error) {
	if m.getImageInfoFunc != nil {
		return m.getImageInfoFunc(id)
	}
	return nil, nil
}

func (m *mockStorage) DeleteImage(id uuid.UUID) error {
	if m.deleteImageFunc != nil {
		return m.deleteImageFunc(id)
	}
	return nil
}

func (m *mockStorage) UpdateImageStatus(id uuid.UUID, status string) error {
	if m.updateImageStatusFunc != nil {
		return m.updateImageStatusFunc(id, status)
	}
	return nil
}

type mockFileStorage struct {
	saveImageFunc    func(string, string, string) error
	getImageFunc     func(string, string, string) error
	deleteImagesFunc func(string, ...string) error
}

func (m *mockFileStorage) SaveImage(fileName, filePath, storageType string) error {
	if m.saveImageFunc != nil {
		return m.saveImageFunc(fileName, filePath, storageType)
	}
	return nil
}

func (m *mockFileStorage) GetImage(fileName, filePath, storageType string) error {
	if m.getImageFunc != nil {
		return m.getImageFunc(fileName, filePath, storageType)
	}
	return nil
}

func (m *mockFileStorage) DeleteImages(storageType string, fileNames ...string) error {
	if m.deleteImagesFunc != nil {
		return m.deleteImagesFunc(storageType, fileNames...)
	}
	return nil
}

type mockQueue struct {
	produceMessageFunc func(dto.Message) error
	consumeMessageFunc func() (*dto.Message, error)
}

func (m *mockQueue) ProduceMessage(msg dto.Message) error {
	if m.produceMessageFunc != nil {
		return m.produceMessageFunc(msg)
	}
	return nil
}

func (m *mockQueue) ConsumeMessage() (*dto.Message, error) {
	if m.consumeMessageFunc != nil {
		return m.consumeMessageFunc()
	}
	return nil, nil
}

func createTestService() (*Service, *mockStorage, *mockFileStorage, *mockQueue) {
	mockStorage := &mockStorage{}
	mockFileStorage := &mockFileStorage{}
	mockQueue := &mockQueue{}

	originDir, _ := os.MkdirTemp("", "test_images")
	processedDir, _ := os.MkdirTemp("", "test_processed")

	originDirName = originDir
	processedDirName = processedDir

	service := &Service{
		storage:     mockStorage,
		fileStorage: mockFileStorage,
		queue:       mockQueue,
	}

	return service, mockStorage, mockFileStorage, mockQueue
}

func createTestServiceWithoutFileStorage() (*Service, *mockStorage, *mockQueue) {
	mockStorage := &mockStorage{}
	mockQueue := &mockQueue{}

	originDir, _ := os.MkdirTemp("", "test_images")
	processedDir, _ := os.MkdirTemp("", "test_processed")

	originDirName = originDir
	processedDirName = processedDir

	service := &Service{
		storage: mockStorage,
		queue:   mockQueue,
	}

	return service, mockStorage, mockQueue
}

func createTestImageData() dto.Message {
	return dto.Message{
		ID:          uuid.New(),
		FileName:    "test.jpg",
		ContentType: "image/jpeg",
		Task:        "resize",
		Resize: struct {
			Width  int `json:"width"`
			Height int `json:"height"`
		}{
			Width:  100,
			Height: 100,
		},
	}
}

func cleanupTestDirs() {
	if originDirName != "" {
		os.RemoveAll(originDirName)
	}
	if processedDirName != "" {
		os.RemoveAll(processedDirName)
	}
}

func TestNew(t *testing.T) {
	defer cleanupTestDirs()

	t.Run("successful creation", func(t *testing.T) {
		mockStorage := &mockStorage{}
		mockFileStorage := &mockFileStorage{}
		mockQueue := &mockQueue{}

		service := New(mockStorage, mockFileStorage, mockQueue)

		assert.NotNil(t, service)
		assert.Equal(t, mockStorage, service.storage)
		assert.Equal(t, mockFileStorage, service.fileStorage)
		assert.Equal(t, mockQueue, service.queue)
	})
}

func TestService_CreateImage(t *testing.T) {
	defer cleanupTestDirs()

	t.Run("successful creation", func(t *testing.T) {
		service, mockStorage, _, mockQueue := createTestService()
		defer cleanupTestDirs()

		imageData := createTestImageData()
		testData := []byte("fake image data")
		mockQueue.produceMessageFunc = func(msg dto.Message) error {
			return nil
		}
		mockStorage.createImageFunc = func(img model.Image) error {
			return nil
		}

		id, err := service.CreateImage(testData, imageData)

		assert.NoError(t, err)
		assert.NotNil(t, id)
	})

	t.Run("invalid format", func(t *testing.T) {
		service, _, _, _ := createTestService()
		defer cleanupTestDirs()

		imageData := createTestImageData()
		imageData.ContentType = "invalid/type"
		testData := []byte("fake image data")

		id, err := service.CreateImage(testData, imageData)

		assert.Error(t, err)
		assert.Equal(t, ErrInvalidImageFormat, err)
		assert.Nil(t, id)
	})

	t.Run("invalid task", func(t *testing.T) {
		service, _, _, _ := createTestService()
		defer cleanupTestDirs()

		imageData := createTestImageData()
		imageData.Task = "invalid_task"
		testData := []byte("fake image data")

		id, err := service.CreateImage(testData, imageData)

		assert.Error(t, err)
		assert.Equal(t, ErrInvalidTask, err)
		assert.Nil(t, id)
	})

	t.Run("queue error", func(t *testing.T) {
		service, mockStorage, _, mockQueue := createTestService()
		defer cleanupTestDirs()

		imageData := createTestImageData()
		testData := []byte("fake image data")

		mockQueue.produceMessageFunc = func(msg dto.Message) error {
			return errors.New("queue error")
		}
		mockStorage.createImageFunc = func(img model.Image) error {
			return nil
		}

		id, err := service.CreateImage(testData, imageData)

		assert.Error(t, err)
		assert.Nil(t, id)
	})

	t.Run("storage error", func(t *testing.T) {
		service, mockStorage, _, mockQueue := createTestService()
		defer cleanupTestDirs()

		imageData := createTestImageData()
		testData := []byte("fake image data")

		mockQueue.produceMessageFunc = func(msg dto.Message) error {
			return nil
		}
		mockStorage.createImageFunc = func(img model.Image) error {
			return errors.New("storage error")
		}

		id, err := service.CreateImage(testData, imageData)

		assert.Error(t, err)
		assert.Nil(t, id)
	})
}

func TestService_GetImageStatus(t *testing.T) {
	t.Run("successful get status", func(t *testing.T) {
		service, mockStorage, _, _ := createTestService()
		defer cleanupTestDirs()

		testID := uuid.New()
		expectedImage := &model.Image{
			ID:     testID,
			Format: "jpg",
			Status: "finished",
		}

		mockStorage.getImageInfoFunc = func(id uuid.UUID) (*model.Image, error) {
			return expectedImage, nil
		}

		result, err := service.GetImageStatus(testID)

		assert.NoError(t, err)
		assert.Equal(t, expectedImage, result)
	})

	t.Run("storage error", func(t *testing.T) {
		service, mockStorage, _, _ := createTestService()
		defer cleanupTestDirs()

		testID := uuid.New()

		mockStorage.getImageInfoFunc = func(id uuid.UUID) (*model.Image, error) {
			return nil, errors.New("storage error")
		}

		result, err := service.GetImageStatus(testID)

		assert.Error(t, err)
		assert.Nil(t, result)
	})
}

func TestService_GetImageById(t *testing.T) {
	defer cleanupTestDirs()

	t.Run("successful get processed image", func(t *testing.T) {
		service, mockStorage, mockFileStorage, _ := createTestService()
		defer cleanupTestDirs()

		testID := uuid.New()
		expectedImage := &model.Image{
			ID:     testID,
			Format: "jpg",
			Status: "finished",
		}

		mockStorage.getImageInfoFunc = func(id uuid.UUID) (*model.Image, error) {
			return expectedImage, nil
		}
		mockFileStorage.getImageFunc = func(fileName, filePath, storageType string) error {
			return nil
		}

		result, err := service.GetImageById(testID)

		assert.NoError(t, err)
		assert.NotEmpty(t, result)
	})

	t.Run("image not processed yet", func(t *testing.T) {
		service, mockStorage, _ := createTestServiceWithoutFileStorage()
		defer cleanupTestDirs()

		testID := uuid.New()
		expectedImage := &model.Image{
			ID:     testID,
			Format: "jpg",
			Status: "in progress",
		}

		mockStorage.getImageInfoFunc = func(id uuid.UUID) (*model.Image, error) {
			return expectedImage, nil
		}

		result, err := service.GetImageById(testID)

		assert.Error(t, err)
		assert.Equal(t, ErrNotProcessdYet, err)
		assert.Empty(t, result)
	})

	t.Run("storage error", func(t *testing.T) {
		service, mockStorage, _ := createTestServiceWithoutFileStorage()
		defer cleanupTestDirs()

		testID := uuid.New()

		mockStorage.getImageInfoFunc = func(id uuid.UUID) (*model.Image, error) {
			return nil, errors.New("storage error")
		}

		result, err := service.GetImageById(testID)

		assert.Error(t, err)
		assert.Empty(t, result)
	})
}

func TestService_DeleteImage(t *testing.T) {
	t.Run("successful deletion", func(t *testing.T) {
		service, mockStorage, mockFileStorage, _ := createTestService()
		defer cleanupTestDirs()

		testID := uuid.New()

		mockStorage.deleteImageFunc = func(id uuid.UUID) error {
			return nil
		}
		mockFileStorage.deleteImagesFunc = func(storageType string, fileNames ...string) error {
			return nil
		}

		err := service.DeleteImage(testID)

		assert.NoError(t, err)
	})

	t.Run("storage deletion error", func(t *testing.T) {
		service, mockStorage, _ := createTestServiceWithoutFileStorage()
		defer cleanupTestDirs()

		testID := uuid.New()

		mockStorage.deleteImageFunc = func(id uuid.UUID) error {
			return errors.New("storage error")
		}

		err := service.DeleteImage(testID)

		assert.Error(t, err)
	})
}

func TestIsCorrectFormat(t *testing.T) {
	tests := []struct {
		format   string
		expected bool
	}{
		{"jpeg", true},
		{"jpg", true},
		{"png", true},
		{"gif", true},
		{"bmp", false},
		{"tiff", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.format, func(t *testing.T) {
			result := isCorrectFormat(tt.format)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseFormat(t *testing.T) {
	tests := []struct {
		contentType string
		expected    string
		expectError bool
	}{
		{"image/jpeg", "jpeg", false},
		{"image/jpg", "jpeg", false},
		{"image/png", "png", false},
		{"image/gif", "gif", false},
		{"invalid/type", "", true},
		{"", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.contentType, func(t *testing.T) {
			result, err := parseFormat(tt.contentType)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestIsCorrectTask(t *testing.T) {
	tests := []struct {
		task     string
		expected bool
	}{
		{"resize", true},
		{"watermark", true},
		{"minature generating", true},
		{"invalid_task", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.task, func(t *testing.T) {
			result := isCorrectTask(tt.task)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAddWatermark(t *testing.T) {
	defer cleanupTestDirs()

	t.Run("successful watermark", func(t *testing.T) {
		service, _, _, _ := createTestService()
		defer cleanupTestDirs()

		testImage := image.NewRGBA(image.Rect(0, 0, 100, 100))
		for y := 0; y < 100; y++ {
			for x := 0; x < 100; x++ {
				testImage.Set(x, y, color.RGBA{255, 255, 255, 255})
			}
		}

		err := service.addWatermark("test.jpg", "jpeg", "Test Watermark")

		assert.Error(t, err)
	})
}

func TestCreateThumbnail(t *testing.T) {
	defer cleanupTestDirs()

	t.Run("successful thumbnail", func(t *testing.T) {
		service, _, _, _ := createTestService()
		defer cleanupTestDirs()

		err := service.createThumbnail("test.jpg", "jpeg")

		assert.Error(t, err)
	})
}

func TestResizeImage(t *testing.T) {
	defer cleanupTestDirs()

	t.Run("successful resize", func(t *testing.T) {
		service, _, _, _ := createTestService()
		defer cleanupTestDirs()

		err := service.resizeImage("test.jpg", "jpeg", 50, 50)

		assert.Error(t, err)
	})
}
