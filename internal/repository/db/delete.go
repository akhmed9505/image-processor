package repository

import (
	"fmt"

	"github.com/google/uuid"
)

func (p *Postgres) DeleteImage(id uuid.UUID) error {
	query := "DELETE FROM images WHERE id = $1"

	_, err := p.db.Master.Exec(query, id)
	if err != nil {
		return fmt.Errorf("could not delete image info from db: %w", err)
	}

	return nil
}
