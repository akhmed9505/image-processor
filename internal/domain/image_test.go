package domain

import (
	"errors"
	"testing"
	"time"
)

func TestImageProcessingStatus(t *testing.T) {
	for _, tc := range []struct {
		name     string
		statuses []JobStatus
		want     ImageStatus
		wantErr  error
	}{
		{"no jobs", nil, "", ErrNoJobs},
		{"pending", []JobStatus{JobStatusPending}, ImageStatusPending, nil},
		{"all pending", []JobStatus{JobStatusPending, JobStatusPending}, ImageStatusPending, nil},
		{"processing", []JobStatus{JobStatusProcessing}, ImageStatusProcessing, nil},
		{"retrying", []JobStatus{JobStatusRetrying}, ImageStatusProcessing, nil},
		{"waiting and processing", []JobStatus{JobStatusPending, JobStatusProcessing}, ImageStatusProcessing, nil},
		{"waiting and retrying", []JobStatus{JobStatusPending, JobStatusRetrying}, ImageStatusProcessing, nil},
		{"waiting and completed", []JobStatus{JobStatusPending, JobStatusCompleted}, ImageStatusProcessing, nil},
		{"waiting and failed", []JobStatus{JobStatusPending, JobStatusFailed}, ImageStatusProcessing, nil},
		{"not yet terminal", []JobStatus{JobStatusCompleted, JobStatusFailed, JobStatusRetrying}, ImageStatusProcessing, nil},
		{"completed", []JobStatus{JobStatusCompleted, JobStatusCompleted}, ImageStatusCompleted, nil},
		{"failed", []JobStatus{JobStatusFailed, JobStatusFailed}, ImageStatusFailed, nil},
		{"partial", []JobStatus{JobStatusCompleted, JobStatusFailed}, ImageStatusPartial, nil},
		{"cancelled", []JobStatus{JobStatusCancelled}, "", ErrCancelledJob},
		{"completed and cancelled", []JobStatus{JobStatusCompleted, JobStatusCancelled}, "", ErrCancelledJob},
		{"unknown after pending", []JobStatus{JobStatusPending, "unknown"}, "", ErrInvalidJobStatus},
		{"zero status", []JobStatus{""}, "", ErrInvalidJobStatus},
	} {
		t.Run(tc.name, func(t *testing.T) {
			image := Image{}
			for _, status := range tc.statuses {
				image.Jobs = append(image.Jobs, Job{Status: status})
			}
			for pass := 0; pass < 2; pass++ {
				got, err := image.ProcessingStatus()
				if got != tc.want || !errors.Is(err, tc.wantErr) {
					t.Fatalf("got (%q,%v), want (%q,%v)", got, err, tc.want, tc.wantErr)
				}
				for left, right := 0, len(image.Jobs)-1; left < right; left, right = left+1, right-1 {
					image.Jobs[left], image.Jobs[right] = image.Jobs[right], image.Jobs[left]
				}
			}
		})
	}
}

func TestDeletedImageProcessingStatus(t *testing.T) {
	for _, jobs := range [][]Job{nil, {{Status: JobStatusCancelled}}, {{Status: JobStatusCompleted}}} {
		image := Image{DeletedAt: time.Now(), Jobs: jobs}
		got, err := image.ProcessingStatus()
		if got != "" || !errors.Is(err, ErrImageDeleted) {
			t.Fatalf("deleted image: got (%q,%v)", got, err)
		}
	}
}

func TestJobStatusIsTerminal(t *testing.T) {
	for _, tc := range []struct {
		status JobStatus
		want   bool
	}{
		{JobStatusPending, false}, {JobStatusProcessing, false}, {JobStatusRetrying, false},
		{JobStatusCompleted, true}, {JobStatusFailed, true}, {JobStatusCancelled, true}, {"unknown", false}, {"", false},
	} {
		t.Run(string(tc.status), func(t *testing.T) {
			if got := tc.status.IsTerminal(); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}
