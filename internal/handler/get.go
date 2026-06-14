package handler

import (
	"errors"
	"net/http"
	"os"

	_ "github.com/akhmed9505/image-processor/internal/model"
	repository "github.com/akhmed9505/image-processor/internal/repository/db"
	"github.com/akhmed9505/image-processor/internal/service"
	"github.com/google/uuid"
	"github.com/wb-go/wbf/ginext"
	"github.com/wb-go/wbf/zlog"
)

// GetImageByID godoc
// @Summary      Get processed image by ID
// @Description  Download the processed image file by its ID
// @Tags         images
// @Accept       json
// @Produce      application/octet-stream
// @Param        id   path     string true  "Image ID"
// @Success      200  {file}   file   "Processed image file"
// @Failure      400  {object} map[string]string "error"
// @Failure      500  {object} map[string]string "error"
// @Router       /image/{id} [get]
func (h *Handler) GetImageByID(c *ginext.Context) {
	uid := c.Param("id")
	id, err := uuid.Parse(uid)
	if err != nil {
		zlog.Logger.Error().Msg("could not parse id to uuid: " + err.Error())
		c.JSON(http.StatusBadRequest, ginext.H{"error": "invalid id was provided"})
		return
	}

	image, err := h.service.GetImageById(id)
	if err != nil {
		if errors.Is(err, service.ErrNotProcessdYet) {
			c.JSON(http.StatusOK, ginext.H{"status": "in processing, not ready yet"})
			return
		}

		if errors.Is(err, repository.ErrNoSuchImage) {
			c.JSON(http.StatusBadRequest, ginext.H{"error": err.Error()})
			return
		}

		zlog.Logger.Error().Msg("could not get image: " + err.Error())
		c.JSON(http.StatusInternalServerError, ginext.H{"error": "could not get image"})
		return
	}

	zlog.Logger.Info().Msg("sucessfully handled GET reques and returned image to user")
	c.File(image)

	if err := os.Remove(image); err != nil {
		zlog.Logger.Error().Msg("could not delete processed image from local storage after sending to user: " + err.Error())
	}
}

// GetImageInfo godoc
// @Summary      Get image processing status
// @Description  Get information about image processing status and metadata
// @Tags         images
// @Accept       json
// @Produce      json
// @Param        id   path     string true  "Image ID"
// @Success      200  {object} model.Image "Image information"
// @Failure      400  {object} map[string]string "error"
// @Failure      500  {object} map[string]string "error"
// @Router       /image/info/{id} [get]
func (h *Handler) GetImageInfo(c *ginext.Context) {
	uid := c.Param("id")
	id, err := uuid.Parse(uid)
	if err != nil {
		zlog.Logger.Error().Msg("could not parse id to uuid: " + err.Error())
		c.JSON(http.StatusBadRequest, ginext.H{"error": "invalid id was provided"})
		return
	}

	info, err := h.service.GetImageStatus(id)
	if err != nil {
		if errors.Is(err, repository.ErrNoSuchImage) {
			c.JSON(http.StatusOK, ginext.H{"error": err.Error()})
			return
		}
		zlog.Logger.Error().Msg("could not get image info: " + err.Error())
		c.JSON(http.StatusInternalServerError, ginext.H{"error": "could not get image info"})
		return
	}

	zlog.Logger.Info().Msg("sucessfully handled GET request and returned image info to user")
	c.JSON(http.StatusOK, info)
}

// GetMainPage godoc
// @Summary      Get main page
// @Description  Get the main HTML page of the application
// @Tags         pages
// @Accept       json
// @Produce      html
// @Success      200  {string} string "HTML page content"
// @Router       / [get]
func (h *Handler) GetMainPage(c *ginext.Context) {
	c.HTML(http.StatusOK, "index.html", nil)
}
