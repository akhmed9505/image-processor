package domain

import (
	"fmt"
	"time"
)

// ImageStatus is the aggregate processing status of an active image.
type ImageStatus string

// Image processing statuses do not include deletion, which is tracked separately.
const (
	ImageStatusPending    ImageStatus = "pending"
	ImageStatusProcessing ImageStatus = "processing"
	ImageStatusCompleted  ImageStatus = "completed"
	ImageStatusPartial    ImageStatus = "partial"
	ImageStatusFailed     ImageStatus = "failed"
)

// Image contains original metadata and its complete set of processing jobs.
// ID contains a UUID string. A nonzero DeletedAt is a deletion tombstone.
type Image struct {
	ID           string
	OriginalName string
	OriginalKey  string
	ContentType  string
	SizeBytes    int64
	Width        int
	Height       int
	CreatedAt    time.Time
	DeletedAt    time.Time
	Jobs         []Job
}

// ProcessingStatus derives the status from all jobs of an active image.
// Deleted images, missing jobs, and invalid job states return domain errors.
func (i Image) ProcessingStatus() (ImageStatus, error) {
	if !i.DeletedAt.IsZero() {
		return "", ErrImageDeleted
	}
	if len(i.Jobs) == 0 {
		return "", ErrNoJobs
	}
	var pending, completed, failed int
	for _, job := range i.Jobs {
		switch job.Status {
		case JobStatusPending:
			pending++
		case JobStatusCompleted:
			completed++
		case JobStatusFailed:
			failed++
		case JobStatusProcessing, JobStatusRetrying:
		case JobStatusCancelled:
			return "", ErrCancelledJob
		default:
			return "", fmt.Errorf("%w: %q", ErrInvalidJobStatus, job.Status)
		}
	}
	switch {
	case pending == len(i.Jobs):
		return ImageStatusPending, nil
	case completed == len(i.Jobs):
		return ImageStatusCompleted, nil
	case failed == len(i.Jobs):
		return ImageStatusFailed, nil
	case completed+failed == len(i.Jobs):
		return ImageStatusPartial, nil
	default:
		return ImageStatusProcessing, nil
	}
}
