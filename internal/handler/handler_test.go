package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"mime/multipart"
	"net/http"
	"net/http/httptest"

	"github.com/akhmed9505/image-processor/internal/dto"
	"github.com/akhmed9505/image-processor/internal/model"
	repository "github.com/akhmed9505/image-processor/internal/repository/db"
	"github.com/akhmed9505/image-processor/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type MockImageProcessorService struct {
	mock.Mock
}

func (m *MockImageProcessorService) ProcessImage(message dto.Message) error {
	args := m.Called(message)
	return args.Error(0)
}

func (m *MockImageProcessorService) GetImageStatus(id uuid.UUID) (*model.Image, error) {
	args := m.Called(id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.Image), args.Error(1)
}

func (m *MockImageProcessorService) GetImageById(id uuid.UUID) (string, error) {
	args := m.Called(id)
	return args.String(0), args.Error(1)
}

func (m *MockImageProcessorService) CreateImage(data []byte, message dto.Message) (*uuid.UUID, error) {
	args := m.Called(data, message)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	id := args.Get(0).(*uuid.UUID)
	return id, args.Error(1)
}

func (m *MockImageProcessorService) DeleteImage(id uuid.UUID) error {
	args := m.Called(id)
	return args.Error(0)
}

type HandlerTestSuite struct {
	suite.Suite
	handler     *Handler
	mockService *MockImageProcessorService
}

func (suite *HandlerTestSuite) SetupTest() {
	suite.mockService = new(MockImageProcessorService)
	suite.handler = New(suite.mockService)
}

func (suite *HandlerTestSuite) createMultipartRequest(imageData []byte, metadata dto.Message) (*http.Request, *multipart.Writer) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	filePart, err := writer.CreateFormFile("image", "test.jpg")
	suite.Require().NoError(err)

	_, err = filePart.Write(imageData)
	suite.Require().NoError(err)

	metadataJSON, err := json.Marshal(metadata)
	suite.Require().NoError(err)

	err = writer.WriteField("metadata", string(metadataJSON))
	suite.Require().NoError(err)

	err = writer.Close()
	suite.Require().NoError(err)

	req := httptest.NewRequest("POST", "/upload", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	return req, writer
}

func (suite *HandlerTestSuite) createGinContext(req *http.Request) (*gin.Context, *httptest.ResponseRecorder) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = req
	return c, w
}

func (suite *HandlerTestSuite) TestCreateImage_Success() {
	imageData := []byte("fake image data")
	metadata := dto.Message{
		Task: "resize",
		Resize: struct {
			Width  int `json:"width"`
			Height int `json:"height"`
		}{
			Width:  100,
			Height: 100,
		},
	}

	expectedID := uuid.New()
	suite.mockService.On("CreateImage", imageData, metadata).Return(&expectedID, nil)

	req, _ := suite.createMultipartRequest(imageData, metadata)
	c, w := suite.createGinContext(req)

	suite.handler.CreateImage(c)

	suite.Equal(http.StatusOK, w.Code)
	var response map[string]string
	err := json.Unmarshal(w.Body.Bytes(), &response)
	suite.NoError(err)
	suite.Equal(expectedID.String(), response["id"])
	suite.mockService.AssertExpectations(suite.T())
}

func (suite *HandlerTestSuite) TestCreateImage_InvalidImage() {
	req := httptest.NewRequest("POST", "/upload", bytes.NewBufferString("invalid"))
	req.Header.Set("Content-Type", "multipart/form-data")
	c, w := suite.createGinContext(req)

	suite.handler.CreateImage(c)

	suite.Equal(http.StatusBadRequest, w.Code)
	var response map[string]string
	err := json.Unmarshal(w.Body.Bytes(), &response)
	suite.NoError(err)
	suite.Contains(response["error"], "invalid image")
}

func (suite *HandlerTestSuite) TestCreateImage_InvalidMetadata() {
	imageData := []byte("fake image data")

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	filePart, err := writer.CreateFormFile("image", "test.jpg")
	suite.Require().NoError(err)
	_, err = filePart.Write(imageData)
	suite.Require().NoError(err)

	err = writer.WriteField("metadata", "invalid json")
	suite.Require().NoError(err)
	writer.Close()

	req := httptest.NewRequest("POST", "/upload", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	c, w := suite.createGinContext(req)

	suite.handler.CreateImage(c)

	suite.Equal(http.StatusBadRequest, w.Code)
	var response map[string]string
	err = json.Unmarshal(w.Body.Bytes(), &response)
	suite.NoError(err)
	suite.Equal("invalid request", response["error"])
}

func (suite *HandlerTestSuite) TestCreateImage_ServiceError_InvalidFormat() {
	imageData := []byte("fake image data")
	metadata := dto.Message{
		Task: "resize",
		Resize: struct {
			Width  int `json:"width"`
			Height int `json:"height"`
		}{
			Width:  100,
			Height: 100,
		},
	}

	suite.mockService.On("CreateImage", imageData, metadata).Return(nil, service.ErrInvalidImageFormat)

	req, _ := suite.createMultipartRequest(imageData, metadata)
	c, w := suite.createGinContext(req)

	suite.handler.CreateImage(c)

	suite.Equal(http.StatusBadRequest, w.Code)
	var response map[string]string
	err := json.Unmarshal(w.Body.Bytes(), &response)
	suite.NoError(err)
	suite.Contains(response["error"], "invalid request")
	suite.mockService.AssertExpectations(suite.T())
}

func (suite *HandlerTestSuite) TestCreateImage_ServiceError_Internal() {
	imageData := []byte("fake image data")
	metadata := dto.Message{
		Task: "resize",
		Resize: struct {
			Width  int `json:"width"`
			Height int `json:"height"`
		}{
			Width:  100,
			Height: 100,
		},
	}

	suite.mockService.On("CreateImage", imageData, metadata).Return(nil, errors.New("internal error"))

	req, _ := suite.createMultipartRequest(imageData, metadata)
	c, w := suite.createGinContext(req)

	suite.handler.CreateImage(c)

	suite.Equal(http.StatusInternalServerError, w.Code)
	var response map[string]string
	err := json.Unmarshal(w.Body.Bytes(), &response)
	suite.NoError(err)
	suite.Contains(response["error"], "invalid request")
	suite.mockService.AssertExpectations(suite.T())
}

func (suite *HandlerTestSuite) TestGetImageByID_InvalidUUID() {
	req := httptest.NewRequest("GET", "/image/invalid-uuid", nil)
	c, w := suite.createGinContext(req)
	c.Params = gin.Params{{Key: "id", Value: "invalid-uuid"}}

	suite.handler.GetImageByID(c)

	suite.Equal(http.StatusBadRequest, w.Code)
	var response map[string]string
	err := json.Unmarshal(w.Body.Bytes(), &response)
	suite.NoError(err)
	suite.Equal("invalid id was provided", response["error"])
}

func (suite *HandlerTestSuite) TestGetImageByID_NotProcessedYet() {
	testID := uuid.New()
	suite.mockService.On("GetImageById", testID).Return("", service.ErrNotProcessdYet)

	req := httptest.NewRequest("GET", "/image/"+testID.String(), nil)
	c, w := suite.createGinContext(req)
	c.Params = gin.Params{{Key: "id", Value: testID.String()}}

	suite.handler.GetImageByID(c)

	suite.Equal(http.StatusOK, w.Code)
	var response map[string]string
	err := json.Unmarshal(w.Body.Bytes(), &response)
	suite.NoError(err)
	suite.Equal("in processing, not ready yet", response["status"])
	suite.mockService.AssertExpectations(suite.T())
}

func (suite *HandlerTestSuite) TestGetImageByID_ServiceError() {
	testID := uuid.New()
	suite.mockService.On("GetImageById", testID).Return("", errors.New("service error"))

	req := httptest.NewRequest("GET", "/image/"+testID.String(), nil)
	c, w := suite.createGinContext(req)
	c.Params = gin.Params{{Key: "id", Value: testID.String()}}

	suite.handler.GetImageByID(c)

	suite.Equal(http.StatusInternalServerError, w.Code)
	var response map[string]string
	err := json.Unmarshal(w.Body.Bytes(), &response)
	suite.NoError(err)
	suite.Equal("could not get image", response["error"])
	suite.mockService.AssertExpectations(suite.T())
}

func (suite *HandlerTestSuite) TestGetImageInfo_Success() {
	testID := uuid.New()
	expectedImage := &model.Image{
		ID:     testID,
		Status: "completed",
	}

	suite.mockService.On("GetImageStatus", testID).Return(expectedImage, nil)

	req := httptest.NewRequest("GET", "/image/info/"+testID.String(), nil)
	c, w := suite.createGinContext(req)
	c.Params = gin.Params{{Key: "id", Value: testID.String()}}

	suite.handler.GetImageInfo(c)

	suite.Equal(http.StatusOK, w.Code)
	var response model.Image
	err := json.Unmarshal(w.Body.Bytes(), &response)
	suite.NoError(err)
	suite.Equal(expectedImage.ID, response.ID)
	suite.Equal(expectedImage.Status, response.Status)
	suite.mockService.AssertExpectations(suite.T())
}

func (suite *HandlerTestSuite) TestGetImageInfo_InvalidUUID() {
	req := httptest.NewRequest("GET", "/image/info/invalid-uuid", nil)
	c, w := suite.createGinContext(req)
	c.Params = gin.Params{{Key: "id", Value: "invalid-uuid"}}

	suite.handler.GetImageInfo(c)

	suite.Equal(http.StatusBadRequest, w.Code)
	var response map[string]string
	err := json.Unmarshal(w.Body.Bytes(), &response)
	suite.NoError(err)
	suite.Equal("invalid id was provided", response["error"])
}

func (suite *HandlerTestSuite) TestGetImageInfo_NoSuchImage() {
	testID := uuid.New()
	suite.mockService.On("GetImageStatus", testID).Return(nil, repository.ErrNoSuchImage)

	req := httptest.NewRequest("GET", "/image/info/"+testID.String(), nil)
	c, w := suite.createGinContext(req)
	c.Params = gin.Params{{Key: "id", Value: testID.String()}}

	suite.handler.GetImageInfo(c)

	suite.Equal(http.StatusOK, w.Code)
	var response map[string]string
	err := json.Unmarshal(w.Body.Bytes(), &response)
	suite.NoError(err)
	suite.Equal(repository.ErrNoSuchImage.Error(), response["error"])
	suite.mockService.AssertExpectations(suite.T())
}

func (suite *HandlerTestSuite) TestGetImageInfo_ServiceError() {
	testID := uuid.New()
	suite.mockService.On("GetImageStatus", testID).Return(nil, errors.New("service error"))

	req := httptest.NewRequest("GET", "/image/info/"+testID.String(), nil)
	c, w := suite.createGinContext(req)
	c.Params = gin.Params{{Key: "id", Value: testID.String()}}

	suite.handler.GetImageInfo(c)

	suite.Equal(http.StatusInternalServerError, w.Code)
	var response map[string]string
	err := json.Unmarshal(w.Body.Bytes(), &response)
	suite.NoError(err)
	suite.Equal("could not get image info", response["error"])
	suite.mockService.AssertExpectations(suite.T())
}

func (suite *HandlerTestSuite) TestGetMainPage_Success() {
	gin.SetMode(gin.TestMode)
	req := httptest.NewRequest("GET", "/", nil)
	c, w := suite.createGinContext(req)

	gin.DefaultWriter = w

	suite.handler.GetMainPage(c)

	suite.Equal(http.StatusOK, w.Code)
	suite.Contains(w.Body.String(), "html")
}

func (suite *HandlerTestSuite) TestDeleteImageByID_Success() {
	testID := uuid.New()
	suite.mockService.On("DeleteImage", testID).Return(nil)

	req := httptest.NewRequest("DELETE", "/image/"+testID.String(), nil)
	c, w := suite.createGinContext(req)
	c.Params = gin.Params{{Key: "id", Value: testID.String()}}

	suite.handler.DeleteImageByID(c)

	suite.Equal(http.StatusOK, w.Code)
	var response map[string]string
	err := json.Unmarshal(w.Body.Bytes(), &response)
	suite.NoError(err)
	suite.Equal("successfully deleted image", response["status"])
	suite.mockService.AssertExpectations(suite.T())
}

func (suite *HandlerTestSuite) TestDeleteImageByID_InvalidUUID() {
	req := httptest.NewRequest("DELETE", "/image/invalid-uuid", nil)
	c, w := suite.createGinContext(req)
	c.Params = gin.Params{{Key: "id", Value: "invalid-uuid"}}

	suite.handler.DeleteImageByID(c)

	suite.Equal(http.StatusBadRequest, w.Code)
	var response map[string]string
	err := json.Unmarshal(w.Body.Bytes(), &response)
	suite.NoError(err)
	suite.Equal("invalid id was provided", response["error"])
}

func (suite *HandlerTestSuite) TestDeleteImageByID_ServiceError() {
	testID := uuid.New()
	suite.mockService.On("DeleteImage", testID).Return(errors.New("delete error"))

	req := httptest.NewRequest("DELETE", "/image/"+testID.String(), nil)
	c, w := suite.createGinContext(req)
	c.Params = gin.Params{{Key: "id", Value: testID.String()}}

	suite.handler.DeleteImageByID(c)

	suite.Equal(http.StatusInternalServerError, w.Code)
	var response map[string]string
	err := json.Unmarshal(w.Body.Bytes(), &response)
	suite.NoError(err)
	suite.Equal("could not delete image: delete error", response["error"])
	suite.mockService.AssertExpectations(suite.T())
}
