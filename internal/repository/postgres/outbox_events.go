package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func enqueueJobRequest(ctx context.Context, tx pgx.Tx, jobID, topic string, availableAt *time.Time) error {
	return enqueueRequestedEvent(ctx, tx, jobID, topic, availableAt, true)
}

func enqueueMissingJobRequest(ctx context.Context, tx pgx.Tx, jobID, topic string) error {
	return enqueueRequestedEvent(ctx, tx, jobID, topic, nil, false)
}

func enqueueRequestedEvent(ctx context.Context, tx pgx.Tx, jobID, topic string, availableAt *time.Time, reschedule bool) error {
	eventID, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("generate requested event ID: %w", err)
	}
	payload, err := json.Marshal(struct {
		EventID   string `json:"event_id"`
		EventType string `json:"event_type"`
		Version   int    `json:"version"`
		JobID     string `json:"job_id"`
	}{eventID.String(), "image.processing.requested", 1, jobID})
	if err != nil {
		return fmt.Errorf("serialize requested event: %w", err)
	}
	// Retry invalidates claims; redispatch preserves publisher ownership and backoff.
	conflict := "DO NOTHING"
	if reschedule {
		conflict = "DO UPDATE SET available_at=EXCLUDED.available_at,lease_until=NULL,claim_token=NULL"
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox(id,job_id,event_type,payload,topic,message_key,available_at)
 VALUES ($1,$2,'image.processing.requested',$3,$4,$5,COALESCE($6::timestamptz,now()))
 ON CONFLICT (job_id) WHERE published_at IS NULL AND event_type='image.processing.requested'
 `+conflict, eventID.String(), jobID, payload, topic, jobID, availableAt)
	if err != nil {
		return fmt.Errorf("enqueue requested event: %w", err)
	}
	return nil
}
