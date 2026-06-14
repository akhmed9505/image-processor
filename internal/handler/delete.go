package handler

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/wb-go/wbf/ginext"
	"github.com/wb-go/wbf/zlog"
)

// DeleteImageByID godoc
// @Summary      Delete image by ID
// @Description  Delete an image and its associated data by ID
// @Tags         images
// @Accept       json
// @Produce      json
// @Param        id   path     string true  "Image ID"
// @Success      200  {object} map[string]string "status"
// @Failure      400  {object} map[string]string "error"
// @Failure      500  {object} map[string]string "error"
// @Router       /image/{id} [delete]
func (h *Handler) DeleteImageByID(c *ginext.Context) {
	uid := c.Param("id")
	id, err := uuid.Parse(uid)
	if err != nil {
		zlog.Logger.Error().Msg("could not parse id to uuid: " + err.Error())
		c.JSON(http.StatusBadRequest, ginext.H{"error": "invalid id was provided"})
		return
	}

	if err := h.service.DeleteImage(id); err != nil {
		zlog.Logger.Error().Msg("could not parse id to uuid: " + err.Error())
		c.JSON(http.StatusInternalServerError, ginext.H{"error": "could not delete image: " + err.Error()})
		return
	}

	zlog.Logger.Info().Msg("sucessfully handled DELETE reques and deleted image")
	c.JSON(http.StatusOK, ginext.H{"status": "successfully deleted image"})
}
