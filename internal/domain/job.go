package domain

import "time"

// JobStatus is the processing state of an individual operation.
type JobStatus string

// Job states follow the processing state machine.
const (
	JobStatusPending    JobStatus = "pending"
	JobStatusProcessing JobStatus = "processing"
	JobStatusRetrying   JobStatus = "retrying"
	JobStatusCompleted  JobStatus = "completed"
	JobStatusFailed     JobStatus = "failed"
	JobStatusCancelled  JobStatus = "cancelled"
)

// IsTerminal reports whether a job can no longer be processed.
func (s JobStatus) IsTerminal() bool {
	return s == JobStatusCompleted || s == JobStatusFailed || s == JobStatusCancelled
}

// Job represents one independent operation on an image original.
// IDs and attempt tokens contain UUID strings. Zero times denote absent values.
type Job struct {
	ID              string
	ImageID         string
	Operation       Operation
	Params          OperationParams
	Status          JobStatus
	Attempts        int
	AttemptToken    string
	LeaseUntil      time.Time
	NextRetryAt     time.Time
	ResultKey       string
	ResultWidth     int
	ResultHeight    int
	ResultSizeBytes int64
	LastErrorCode   string
	LastError       string
	CreatedAt       time.Time
	UpdatedAt       time.Time
	CompletedAt     time.Time
}
