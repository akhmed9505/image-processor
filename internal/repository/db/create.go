package repository

import (
	"fmt"

	"github.com/akhmed9505/image-processor/internal/model"
)

func (p *Postgres) CreateImage(image model.Image) error {
	query := "INSERT INTO images(id, format, status) VALUES ($1, $2, $3)"

	_, err := p.db.Master.Exec(query, image.ID, image.Format, image.Status)
	if err != nil {
		return fmt.Errorf("could not save image info in db: %w", err)
	}

	return nil
}
