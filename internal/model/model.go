package model

import (
	"time"

	"github.com/google/uuid"
)

type Image struct {
	ID       uuid.UUID `json:"id"`
	Format   string    `json:"-"`
	Status   string    `json:"status"`
	CreateAt time.Time `json:"create_at"`
}
