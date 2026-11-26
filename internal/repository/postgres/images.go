// Package postgres implements PostgreSQL repositories.
package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/akhmed9505/image-processor/internal/domain"
	imageService "github.com/akhmed9505/image-processor/internal/service/image"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrInvalidPagination indicates an invalid page size or incomplete cursor.
var ErrInvalidPagination = errors.New("invalid image pagination")

// ImageCursor identifies the last image of the previous page.
type ImageCursor = imageService.ImageCursor

// ImagePage contains repository records and a cursor for the next page.
type ImagePage = imageService.ImageRecords

// ImageRepository reads active images and their complete job sets.
type ImageRepository struct{ pool *pgxpool.Pool }

// NewImageRepository uses pool without taking ownership of its lifecycle.
func NewImageRepository(pool *pgxpool.Pool) *ImageRepository {
	return &ImageRepository{pool: pool}
}

// GetByID returns an active image or domain.ErrImageNotFound.
func (r *ImageRepository) GetByID(ctx context.Context, id string) (domain.Image, error) {
	page, err := r.read(ctx, &id, 1, nil)
	if err != nil {
		return domain.Image{}, fmt.Errorf("get image: %w", err)
	}
	if len(page.Images) == 0 {
		return domain.Image{}, domain.ErrImageNotFound
	}
	return page.Images[0], nil
}

// List returns images ordered by creation time and UUID, both descending.
// A zero limit selects 20 images; supported limits are 1 through 100.
func (r *ImageRepository) List(ctx context.Context, limit int, cursor *ImageCursor) (ImagePage, error) {
	if limit == 0 {
		limit = 20
	}
	if limit < 1 || limit > 100 || (cursor != nil && (cursor.CreatedAt.IsZero() || cursor.ID == "")) {
		return ImagePage{}, ErrInvalidPagination
	}
	page, err := r.read(ctx, nil, limit, cursor)
	if err != nil {
		return ImagePage{}, fmt.Errorf("list images: %w", err)
	}
	return page, nil
}

func (r *ImageRepository) read(ctx context.Context, id *string, limit int, cursor *ImageCursor) (ImagePage, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return ImagePage{}, fmt.Errorf("begin image snapshot: %w", err)
	}
	defer tx.Rollback(ctx)
	query := `SELECT id::text,original_name,original_key,content_type,size_bytes,width,height,created_at
 FROM images WHERE deleted_at IS NULL`
	var args []any
	if id != nil {
		query += ` AND id=$1::uuid`
		args = []any{*id}
	} else {
		var afterTime, afterID any
		if cursor != nil {
			afterTime = cursor.CreatedAt
			afterID = cursor.ID
		}
		query += ` AND ($1::timestamptz IS NULL OR (created_at,id)<($1::timestamptz,$2::uuid))
 ORDER BY created_at DESC,id DESC LIMIT $3`
		args = []any{afterTime, afterID, limit + 1}
	}
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return ImagePage{}, fmt.Errorf("query images: %w", err)
	}
	images, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.Image, error) {
		var image domain.Image
		err := row.Scan(&image.ID, &image.OriginalName, &image.OriginalKey, &image.ContentType, &image.SizeBytes, &image.Width, &image.Height, &image.CreatedAt)
		image.Jobs = []domain.Job{}
		return image, err
	})
	if err != nil {
		return ImagePage{}, fmt.Errorf("scan images: %w", err)
	}
	page := ImagePage{Images: images}
	if id == nil && len(images) > limit {
		page.Images = images[:limit]
		last := page.Images[limit-1]
		page.NextCursor = &ImageCursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	if len(page.Images) > 0 {
		ids := make([]string, len(page.Images))
		positions := make(map[string]int, len(ids))
		for i, image := range page.Images {
			ids[i] = image.ID
			positions[image.ID] = i
		}
		rows, err := tx.Query(ctx, `SELECT `+jobColumns+`   FROM jobs WHERE image_id=ANY($1::uuid[]) ORDER BY image_id,created_at,id`, ids)
		if err != nil {
			return ImagePage{}, fmt.Errorf("query image jobs: %w", err)
		}
		jobs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.Job, error) { return scanJob(row) })
		if err != nil {
			return ImagePage{}, fmt.Errorf("scan image jobs: %w", err)
		}
		for _, job := range jobs {
			index := positions[job.ImageID]
			page.Images[index].Jobs = append(page.Images[index].Jobs, job)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ImagePage{}, fmt.Errorf("commit image snapshot: %w", err)
	}
	return page, nil
}

const jobColumns = `id::text,image_id::text,operation,params,status,attempts,
   COALESCE(attempt_token::text,''),lease_until,next_retry_at,
   COALESCE(result_key,''),COALESCE(result_width,0),COALESCE(result_height,0),COALESCE(result_size_bytes,0),
   COALESCE(last_error_code,''),COALESCE(last_error,''),created_at,updated_at,completed_at
`

func scanJob(row pgx.Row) (domain.Job, error) {
	var job domain.Job
	var params []byte
	var lease, retry, completed pgtype.Timestamptz
	if err := row.Scan(&job.ID, &job.ImageID, &job.Operation, &params, &job.Status, &job.Attempts, &job.AttemptToken,
		&lease, &retry, &job.ResultKey, &job.ResultWidth, &job.ResultHeight, &job.ResultSizeBytes,
		&job.LastErrorCode, &job.LastError, &job.CreatedAt, &job.UpdatedAt, &completed); err != nil {
		return domain.Job{}, err
	}
	job.LeaseUntil = lease.Time
	job.NextRetryAt = retry.Time
	job.CompletedAt = completed.Time
	var target any
	switch job.Operation {
	case domain.OperationResize:
		target = &job.Params.Resize
	case domain.OperationThumbnail:
		target = &job.Params.Thumbnail
	case domain.OperationWatermark:
		target = &struct{}{}
	default:
		return domain.Job{}, domain.ErrInvalidOperation
	}
	decoder := json.NewDecoder(bytes.NewReader(params))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return domain.Job{}, fmt.Errorf("decode job parameters: %w", err)
	}
	if err := job.Params.Validate(job.Operation); err != nil {
		return domain.Job{}, fmt.Errorf("validate stored job parameters: %w", err)
	}
	return job, nil
}
