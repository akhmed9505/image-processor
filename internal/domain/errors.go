package domain

import "errors"

// Domain errors describe invalid operations and image processing states.
var (
	ErrInvalidOperation       = errors.New("unsupported image operation")
	ErrInvalidOperationParams = errors.New("invalid operation parameters")
	ErrNoJobs                 = errors.New("image has no jobs")
	ErrInvalidJobStatus       = errors.New("unknown job status")
	ErrCancelledJob           = errors.New("active image has a cancelled job")
	ErrImageDeleted           = errors.New("image is deleted")
)
