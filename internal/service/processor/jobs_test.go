package processor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
)

type jobRepositoryMock struct {
	claim    func(context.Context, string) (ClaimResult, error)
	complete func(context.Context, string, string, JobResult) (domain.Job, error)
	retry    func(context.Context, string, string, JobFailure, time.Time) (domain.Job, error)
	fail     func(context.Context, string, string, JobFailure) (domain.Job, error)
}

func (m jobRepositoryMock) Claim(c context.Context, id string) (ClaimResult, error) {
	return m.claim(c, id)
}
func (m jobRepositoryMock) Complete(c context.Context, id, token string, r JobResult) (domain.Job, error) {
	return m.complete(c, id, token, r)
}
func (m jobRepositoryMock) Retry(c context.Context, id, token string, f JobFailure, at time.Time) (domain.Job, error) {
	return m.retry(c, id, token, f, at)
}
func (m jobRepositoryMock) Fail(c context.Context, id, token string, f JobFailure) (domain.Job, error) {
	return m.fail(c, id, token, f)
}

type jobStorageMock struct {
	open func(context.Context, string) (io.ReadCloser, error)
	put  func(context.Context, string, io.Reader, int64, string) error
}

func (m jobStorageMock) OpenOriginal(c context.Context, k string) (io.ReadCloser, error) {
	return m.open(c, k)
}
func (m jobStorageMock) Put(c context.Context, k string, r io.Reader, n int64, ct string) error {
	return m.put(c, k, r, n, ct)
}

type originalReader struct {
	io.Reader
	closed   bool
	closeErr error
}

func (r *originalReader) Close() error { r.closed = true; return r.closeErr }

func acquiredAttempt() ClaimResult {
	return ClaimResult{Disposition: ClaimAcquired, OriginalKey: "image/original.png", ResultKey: "image/results/job/token.png", Job: domain.Job{ID: "job", ImageID: "image", Operation: domain.OperationResize, Params: domain.OperationParams{Resize: domain.ResizeParams{Width: 60, Height: 40}}, Status: domain.JobStatusProcessing, Attempts: 1, AttemptToken: "token", LeaseUntil: time.Now().Add(AttemptLease)}}
}

func TestJobAdmission(t *testing.T) {
	for _, disposition := range []ClaimDisposition{ClaimAcquired, ClaimProcessing, ClaimWaitingRetry, ClaimTerminal, ClaimImageDeleted, ClaimExhausted} {
		t.Run(string(disposition), func(t *testing.T) {
			expected := ClaimResult{Disposition: disposition}
			r := jobRepositoryMock{claim: func(ctx context.Context, id string) (ClaimResult, error) {
				deadline, ok := ctx.Deadline()
				if id != "job" || !ok || time.Until(deadline) > jobDatabaseTimeout {
					t.Fatal("claim arguments")
				}
				return expected, nil
			}}
			got, e := NewJobService(r, nil).Admit(context.Background(), "job")
			if e != nil || !got.CanAck() || !reflect.DeepEqual(got, expected) {
				t.Fatal(got, e)
			}
		})
	}
	for _, cause := range []error{errors.New("database unavailable"), ErrJobCommitUncertain, context.Canceled} {
		r := jobRepositoryMock{claim: func(context.Context, string) (ClaimResult, error) { return acquiredAttempt(), cause }}
		got, e := NewJobService(r, nil).Admit(context.Background(), "job")
		if !errors.Is(e, cause) || got.CanAck() || got.Job.ID != "" {
			t.Fatal("unsafe acknowledgement", got, e)
		}
	}
	if (ClaimResult{}).CanAck() {
		t.Fatal("zero admission can acknowledge")
	}
}

func TestJobProcessingSuccess(t *testing.T) {
	for _, format := range []string{"jpeg", "png"} {
		t.Run(format, func(t *testing.T) {
			admission := acquiredAttempt()
			reader := &originalReader{Reader: bytes.NewReader(encode(t, stripes(), format))}
			var calls []string
			var spool *os.File
			var spoolName string
			var stored JobResult
			storage := jobStorageMock{open: func(ctx context.Context, key string) (io.ReadCloser, error) {
				if key != admission.OriginalKey {
					t.Fatal(key)
				}
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > AttemptTimeout {
					t.Fatal("missing processing deadline")
				}
				calls = append(calls, "open")
				return reader, nil
			}, put: func(ctx context.Context, key string, input io.Reader, size int64, mime string) error {
				calls = append(calls, "put")
				if !reader.closed || key != admission.ResultKey {
					t.Fatal("original not closed or wrong key")
				}
				var ok bool
				spool, ok = input.(*os.File)
				if !ok {
					t.Fatal("output is not streamed from disk")
				}
				spoolName = spool.Name()
				stat, e := spool.Stat()
				if e != nil || stat.Mode().Perm() != 0600 {
					t.Fatal("spool permissions", e)
				}
				data, e := io.ReadAll(input)
				if e != nil || int64(len(data)) != size || mime != "image/"+format {
					t.Fatal("output metadata", e)
				}
				stored = JobResult{key, 60, 40, size}
				return nil
			}}
			repo := jobRepositoryMock{complete: func(ctx context.Context, id, token string, result JobResult) (domain.Job, error) {
				calls = append(calls, "complete")
				if id != admission.Job.ID || token != admission.Job.AttemptToken || result != stored {
					t.Fatal("completion arguments", result)
				}
				if _, e := os.Stat(spoolName); !os.IsNotExist(e) {
					t.Fatal("spool not removed before finalization")
				}
				return domain.Job{ID: id, Status: domain.JobStatusCompleted}, nil
			}}
			got, e := NewJobService(repo, storage).ProcessAttempt(context.Background(), admission)
			if e != nil || got.Status != domain.JobStatusCompleted || !reflect.DeepEqual(calls, []string{"open", "put", "complete"}) {
				t.Fatal(got, e, calls)
			}
			if _, e := spool.Stat(); !errors.Is(e, os.ErrClosed) {
				t.Fatal("spool left open", e)
			}
		})
	}
}

func TestJobProcessingFailures(t *testing.T) {
	network := errors.New("connection reset with sensitive details")
	for _, tc := range []struct {
		name      string
		err       error
		stage     string
		code      string
		permanent bool
	}{
		{"missing", ErrOriginalNotFound, "open", "original_not_found", true},
		{"network", network, "open", "processing_unavailable", false},
		{"read", network, "read", "processing_unavailable", false},
		{"corrupt", ErrUnsupportedFormat, "corrupt", "invalid_original", true},
		{"invalid params", domain.ErrInvalidOperationParams, "params", "invalid_operation", true},
		{"limit", ErrLimitExceeded, "limit", "image_limit_exceeded", true},
		{"close", network, "close", "processing_unavailable", false},
		{"partial put", network, "put", "processing_unavailable", false},
		{"timeout", context.DeadlineExceeded, "timeout", "attempt_timeout", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			admission := acquiredAttempt()
			data := encode(t, stripes(), "png")
			reader := &originalReader{Reader: bytes.NewReader(data)}
			switch tc.stage {
			case "read":
				reader.Reader = errorReader{tc.err}
			case "corrupt":
				reader.Reader = strings.NewReader("corrupt")
			case "params":
				admission.Job.Params = domain.OperationParams{}
			case "limit":
				admission.Job.Params.Resize.Width = MaxDimension + 1
			case "close":
				reader.closeErr = tc.err
			case "timeout":
				admission.Job.LeaseUntil = time.Now().Add(AttemptLease - AttemptTimeout - time.Second)
			}
			var spoolName string
			storage := jobStorageMock{open: func(context.Context, string) (io.ReadCloser, error) {
				if tc.stage == "open" {
					return nil, tc.err
				}
				return reader, nil
			}, put: func(_ context.Context, _ string, r io.Reader, _ int64, _ string) error {
				if tc.stage != "put" {
					t.Fatal("unexpected put")
				}
				file := r.(*os.File)
				spoolName = file.Name()
				prefix := make([]byte, 8)
				_, _ = r.Read(prefix)
				return tc.err
			}}
			finalized := false
			check := func(ctx context.Context, id, token string, f JobFailure, permanent bool) (domain.Job, error) {
				finalized = true
				if ctx.Err() != nil || id != admission.Job.ID || token != admission.Job.AttemptToken || f.Code != tc.code || permanent != tc.permanent || strings.Contains(f.Message, "sensitive") {
					t.Fatal("failure arguments", f)
				}
				status := domain.JobStatusRetrying
				if permanent {
					status = domain.JobStatusFailed
				}
				return domain.Job{ID: id, Status: status}, nil
			}
			repo := jobRepositoryMock{retry: func(ctx context.Context, id, token string, f JobFailure, at time.Time) (domain.Job, error) {
				delay := time.Until(at)
				if delay < 3900*time.Millisecond || delay > 6*time.Second {
					t.Fatal("retry delay", delay)
				}
				return check(ctx, id, token, f, false)
			}, fail: func(ctx context.Context, id, token string, f JobFailure) (domain.Job, error) {
				return check(ctx, id, token, f, true)
			}}
			got, e := NewJobService(repo, storage).ProcessAttempt(context.Background(), admission)
			if !errors.Is(e, tc.err) || !finalized || got.ID != admission.Job.ID {
				t.Fatal(got, e)
			}
			if tc.stage != "open" && tc.stage != "timeout" && !reader.closed {
				t.Fatal("reader left open")
			}
			if spoolName != "" {
				if _, e := os.Stat(spoolName); !os.IsNotExist(e) {
					t.Fatal("spool left behind")
				}
			}
		})
	}
}

func TestJobFinalizationErrors(t *testing.T) {
	for _, stage := range []string{"complete", "retry", "fail"} {
		for _, cause := range []error{ErrAttemptFenced, ErrJobCommitUncertain, errors.New("postgres unavailable")} {
			t.Run(fmt.Sprint(stage, "/", cause), func(t *testing.T) {
				admission := acquiredAttempt()
				reader := &originalReader{Reader: bytes.NewReader(encode(t, stripes(), "png"))}
				var spoolName string
				storage := jobStorageMock{open: func(context.Context, string) (io.ReadCloser, error) {
					if stage == "retry" {
						return nil, io.ErrUnexpectedEOF
					}
					if stage == "fail" {
						return nil, ErrOriginalNotFound
					}
					return reader, nil
				}, put: func(_ context.Context, _ string, r io.Reader, _ int64, _ string) error {
					spoolName = r.(*os.File).Name()
					return nil
				}}
				repo := jobRepositoryMock{
					complete: func(context.Context, string, string, JobResult) (domain.Job, error) {
						if stage != "complete" {
							t.Fatal("unexpected complete")
						}
						return domain.Job{}, cause
					},
					retry: func(context.Context, string, string, JobFailure, time.Time) (domain.Job, error) {
						if stage != "retry" {
							t.Fatal("unexpected retry")
						}
						return domain.Job{}, cause
					},
					fail: func(context.Context, string, string, JobFailure) (domain.Job, error) {
						if stage != "fail" {
							t.Fatal("unexpected fail")
						}
						return domain.Job{}, cause
					},
				}
				got, e := NewJobService(repo, storage).ProcessAttempt(context.Background(), admission)
				if !errors.Is(e, cause) || got.ID != "" {
					t.Fatal("lost finalization cause", got, e)
				}
				if spoolName != "" {
					if _, e := os.Stat(spoolName); !os.IsNotExist(e) {
						t.Fatal("spool leaked")
					}
				}
			})
		}
	}
}

func TestJobCancellation(t *testing.T) {
	for _, stage := range []string{"before", "read", "put"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if stage == "before" {
				cancel()
			}
			reader := &originalReader{Reader: bytes.NewReader(encode(t, stripes(), "png"))}
			storage := jobStorageMock{open: func(context.Context, string) (io.ReadCloser, error) {
				if stage == "before" {
					t.Fatal("I/O after cancellation")
				}
				if stage == "read" {
					reader.Reader = cancelRead{cancel}
				}
				return reader, nil
			}, put: func(context.Context, string, io.Reader, int64, string) error { cancel(); return ctx.Err() }}
			_, e := NewJobService(jobRepositoryMock{}, storage).ProcessAttempt(ctx, acquiredAttempt())
			if !errors.Is(e, context.Canceled) {
				t.Fatal(e)
			}
			if stage != "before" && !reader.closed {
				t.Fatal("reader leaked")
			}
		})
	}
}

type cancelRead struct{ cancel context.CancelFunc }

func (r cancelRead) Read([]byte) (int, error) { r.cancel(); return 0, context.Canceled }

func TestJobFinalAttemptAndInvalidAdmission(t *testing.T) {
	admission := acquiredAttempt()
	admission.Job.Attempts = MaxAttempts
	called := false
	repo := jobRepositoryMock{fail: func(context.Context, string, string, JobFailure) (domain.Job, error) {
		called = true
		return domain.Job{Status: domain.JobStatusFailed}, nil
	}}
	storage := jobStorageMock{open: func(context.Context, string) (io.ReadCloser, error) { return nil, io.ErrUnexpectedEOF }}
	got, e := NewJobService(repo, storage).ProcessAttempt(context.Background(), admission)
	if !called || got.Status != domain.JobStatusFailed || !errors.Is(e, io.ErrUnexpectedEOF) {
		t.Fatal(got, e)
	}
	for _, a := range []ClaimResult{{}, {Disposition: ClaimTerminal}, {Disposition: ClaimAcquired, Job: domain.Job{Status: domain.JobStatusPending}}} {
		if _, e := NewJobService(jobRepositoryMock{}, jobStorageMock{}).ProcessAttempt(context.Background(), a); !errors.Is(e, ErrInvalidAdmission) {
			t.Fatal(e)
		}
	}
}

func TestRetryDelay(t *testing.T) {
	for attempts, base := range []time.Duration{5 * time.Second, 15 * time.Second, 45 * time.Second, 135 * time.Second} {
		varied := make(map[time.Duration]bool)
		for i := 0; i < 100; i++ {
			delay := RetryDelay(attempts + 1)
			if delay < base-base/5 || delay > base+base/5 {
				t.Fatal("jitter out of bounds", delay)
			}
			varied[delay] = true
		}
		if len(varied) < 2 {
			t.Fatal("jitter is constant")
		}
	}
	for _, attempts := range []int{-1, 0, 5, 6, 100} {
		if RetryDelay(attempts) != 0 {
			t.Fatal("retry outside attempt budget")
		}
	}
}

func TestJobAttemptDeadline(t *testing.T) {
	admission := acquiredAttempt()
	admission.Job.LeaseUntil = time.Now().Add(AttemptLease - AttemptTimeout + 30*time.Millisecond)
	storage := jobStorageMock{open: func(ctx context.Context, _ string) (io.ReadCloser, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	retried := false
	repo := jobRepositoryMock{retry: func(ctx context.Context, _ string, _ string, f JobFailure, _ time.Time) (domain.Job, error) {
		retried = true
		if ctx.Err() != nil || f.Code != "attempt_timeout" {
			t.Fatal("attempt deadline prevented failure persistence", ctx.Err(), f)
		}
		return domain.Job{Status: domain.JobStatusRetrying}, nil
	}}
	got, e := NewJobService(repo, storage).ProcessAttempt(context.Background(), admission)
	if !retried || got.Status != domain.JobStatusRetrying || !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal(got, e)
	}
}

func TestJobSpoolCreationFailure(t *testing.T) {
	dir := t.TempDir() + "/absent"
	t.Setenv("TMPDIR", dir)
	reader := &originalReader{Reader: bytes.NewReader(encode(t, stripes(), "png"))}
	storage := jobStorageMock{open: func(context.Context, string) (io.ReadCloser, error) { return reader, nil }}
	repo := jobRepositoryMock{retry: func(context.Context, string, string, JobFailure, time.Time) (domain.Job, error) {
		return domain.Job{Status: domain.JobStatusRetrying}, nil
	}}
	got, e := NewJobService(repo, storage).ProcessAttempt(context.Background(), acquiredAttempt())
	if !errors.Is(e, os.ErrNotExist) || !reader.closed || got.Status != domain.JobStatusRetrying {
		t.Fatal("spool failure handling", got, e)
	}
}

func TestJobCompleteDeadline(t *testing.T) {
	for _, budget := range []time.Duration{AttemptTimeout, time.Second} {
		t.Run(budget.String(), func(t *testing.T) {
			admission := acquiredAttempt()
			admission.Job.LeaseUntil = time.Now().Add(AttemptLease - AttemptTimeout + budget)
			var attemptDeadline time.Time
			var completeCtx context.Context
			storage := jobStorageMock{
				open: func(ctx context.Context, _ string) (io.ReadCloser, error) {
					attemptDeadline, _ = ctx.Deadline()
					return io.NopCloser(bytes.NewReader(encode(t, stripes(), "png"))), nil
				},
				put: func(context.Context, string, io.Reader, int64, string) error { return nil },
			}
			repo := jobRepositoryMock{complete: func(ctx context.Context, _, _ string, _ JobResult) (domain.Job, error) {
				completeCtx = ctx
				deadline, ok := ctx.Deadline()
				if !ok || deadline.After(attemptDeadline) || time.Until(deadline) > jobDatabaseTimeout {
					t.Fatal("completion deadline exceeds database or attempt budget", deadline)
				}
				if budget < jobDatabaseTimeout && !deadline.Equal(attemptDeadline) {
					t.Fatal("earlier attempt deadline was not preserved")
				}
				return domain.Job{}, ErrJobCommitUncertain
			}}
			_, err := NewJobService(repo, storage).ProcessAttempt(context.Background(), admission)
			if !errors.Is(err, ErrJobCommitUncertain) || completeCtx.Err() != context.Canceled {
				t.Fatal("completion error or context cleanup lost", err)
			}
		})
	}
}

func TestProcessingFailureContextPriority(t *testing.T) {
	for _, cancellation := range []error{context.Canceled, context.DeadlineExceeded} {
		for _, validation := range []error{ErrInvalidImage, ErrUnsupportedFormat, ErrLimitExceeded, domain.ErrInvalidOperationParams} {
			failure, permanent := processingFailure(fmt.Errorf("decode: %w", errors.Join(validation, cancellation)))
			expected := "attempt_cancelled"
			if cancellation == context.DeadlineExceeded {
				expected = "attempt_timeout"
			}
			if permanent || failure.Code != expected {
				t.Fatal("context error classified as permanent", failure)
			}
		}
	}
}

func TestJobContextOverridesInvalidImage(t *testing.T) {
	for _, parentCancelled := range []bool{false, true} {
		t.Run(fmt.Sprint("parent_cancelled=", parentCancelled), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			admission := acquiredAttempt()
			if !parentCancelled {
				admission.Job.LeaseUntil = time.Now().Add(AttemptLease - AttemptTimeout + 30*time.Millisecond)
			}
			storage := jobStorageMock{open: func(attemptCtx context.Context, _ string) (io.ReadCloser, error) {
				if parentCancelled {
					cancel()
				} else {
					<-attemptCtx.Done()
				}
				return nil, ErrInvalidImage
			}}
			retried := false
			repo := jobRepositoryMock{retry: func(ctx context.Context, _, _ string, f JobFailure, _ time.Time) (domain.Job, error) {
				retried = true
				if parentCancelled || ctx.Err() != nil || f.Code != "attempt_timeout" {
					t.Fatal("incorrect cancellation handling", f)
				}
				return domain.Job{Status: domain.JobStatusRetrying}, nil
			}}
			_, err := NewJobService(repo, storage).ProcessAttempt(ctx, admission)
			expected := context.DeadlineExceeded
			if parentCancelled {
				expected = context.Canceled
			}
			if !errors.Is(err, expected) || retried == parentCancelled {
				t.Fatal("context did not override validation", err, retried)
			}
		})
	}
}
