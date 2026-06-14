package handler

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/akhmed9505/image-processor/internal/dto"
	"github.com/akhmed9505/image-processor/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/wb-go/wbf/ginext"
	"github.com/wb-go/wbf/zlog"
)

// CreateImage godoc
// @Summary      Upload image for processing
// @Description  Upload an image file with metadata for processing
// @Tags         images
// @Accept       multipart/form-data
// @Produce      json
// @Param        image    formData file     true  "Image file to upload"
// @Param        metadata formData string   true  "JSON metadata for image processing"
// @Success      200      {object} map[string]string "id"
// @Failure      400      {object} map[string]string "error"
// @Failure      500      {object} map[string]string "error"
// @Router       /upload [post]
func (h *Handler) CreateImage(c *ginext.Context) {
	fileHeader, err := c.FormFile("image")
	if err != nil {
		zlog.Logger.Error().Msg("invalid image: " + err.Error())
		c.JSON(http.StatusBadRequest, ginext.H{"error": "invalid image: " + err.Error()})
		return
	}

	file, err := fileHeader.Open()
	if err != nil {
		zlog.Logger.Error().Msg("could not open the image: " + err.Error())
		c.JSON(http.StatusBadRequest, ginext.H{"error": "could not open the image"})
		return
	}
	defer file.Close()

	fileBytes, err := io.ReadAll(file)
	if err != nil {
		zlog.Logger.Error().Msg("could not open the image: " + err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": "could not open the image"})
		return
	}

	metadataStr := c.PostForm("metadata")
	var message dto.Message
	if err := json.Unmarshal([]byte(metadataStr), &message); err != nil {
		zlog.Logger.Error().Msg("could not unmarshal metadata: " + err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	id, err := h.service.CreateImage(fileBytes, message)
	if err != nil {
		if errors.Is(err, service.ErrInvalidImageFormat) || errors.Is(err, service.ErrInvalidTask) {
			zlog.Logger.Error().Msg("could not create file: " + err.Error())
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request: " + err.Error()})
			return
		}
		zlog.Logger.Error().Msg("could not create file: " + err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"error": "invalid request: " + err.Error()})
		return
	}

	zlog.Logger.Info().Msg("successfully handled GET request and created image with id: " + id.String())
	c.JSON(http.StatusOK, ginext.H{"id": id.String()})
}
