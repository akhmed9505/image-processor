package repository

import (
	"database/sql"
	"fmt"

	"github.com/akhmed9505/image-processor/internal/model"
	"github.com/google/uuid"
)

func (p *Postgres) GetImageInfo(id uuid.UUID) (*model.Image, error) {
	query := "SELECT * FROM images WHERE id = $1"

	var image model.Image
	err := p.db.Master.QueryRow(query, id).Scan(
		&image.ID,
		&image.Format,
		&image.Status,
		&image.CreateAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrNoSuchImage
		}

		return nil, fmt.Errorf("could not get image from db: %w", err)
	}

	return &image, nil
}
