package repository

import (
	"errors"

	"github.com/wb-go/wbf/dbpg"
)

var (
	ErrNoSuchImage = errors.New("there is no image with such id")
)

type Postgres struct {
	db *dbpg.DB
}

func NewPostgres(db *dbpg.DB) *Postgres {
	return &Postgres{
		db: db,
	}
}
