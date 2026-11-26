package image

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

type cleanupRepoMock struct {
	imageFind func(context.Context, int, string, time.Duration, CleanupClaim) (CleanupPage, error)
	claim     func(context.Context, int) ([]CleanupClaim, error)
	find      func(context.Context, int, string, time.Duration) (CleanupPage, error)
	finish    func(context.Context, string, string, time.Time, bool) error
}

func (m cleanupRepoMock) Claim(c context.Context, n int) ([]CleanupClaim, error) {
	return m.claim(c, n)
}
func (m cleanupRepoMock) FindDeletable(c context.Context, n int, k string, g time.Duration) (CleanupPage, error) {
	return m.find(c, n, k, g)
}
func (m cleanupRepoMock) FindDeletableForImage(c context.Context, n int, k string, g time.Duration, claim CleanupClaim) (CleanupPage, error) {
	if m.imageFind != nil {
		return m.imageFind(c, n, k, g, claim)
	}
	return m.find(c, n, k, g)
}
func (m cleanupRepoMock) Complete(c context.Context, id, t string, at time.Time) error {
	return m.finish(c, id, t, at, true)
}
func (m cleanupRepoMock) Reschedule(c context.Context, id, t string, at time.Time) error {
	return m.finish(c, id, t, at, false)
}

type cleanupRegistryMock func(context.Context, string) (StorageObject, error)

func (f cleanupRegistryMock) GetByKey(c context.Context, k string) (StorageObject, error) {
	return f(c, k)
}

type cleanupStorageMock func(context.Context, string) error

func (f cleanupStorageMock) Delete(c context.Context, k string) error { return f(c, k) }
func cleanupLogger() *slog.Logger                                     { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
func checkCleanupTimeout(t *testing.T, ctx context.Context, limit time.Duration) {
	t.Helper()
	d, ok := ctx.Deadline()
	if !ok || time.Until(d) > limit || ctx.Err() != nil {
		t.Fatal("timeout", d, ctx.Err())
	}
}

func TestCleanupPaginationAndRepeat(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "partial failure"}[partial], func(t *testing.T) {
			deletes := 0
			finishes := 0
			pages := 0
			repo := cleanupRepoMock{claim: func(ctx context.Context, n int) ([]CleanupClaim, error) {
				checkCleanupTimeout(t, ctx, 5*time.Second)
				if n != 1 {
					t.Fatal(n)
				}
				return []CleanupClaim{{ImageID: "image", Token: "token", LeaseUntil: time.Now().Add(CleanupLease)}}, nil
			}, find: func(ctx context.Context, n int, cursor string, grace time.Duration) (CleanupPage, error) {
				checkCleanupTimeout(t, ctx, 5*time.Second)
				if n != 100 || grace != 10*time.Minute {
					t.Fatal("limits")
				}
				pages++
				switch cursor {
				case "":
					return CleanupPage{NextCursor: "a"}, nil
				case "a":
					return CleanupPage{Objects: []StorageObject{{ObjectKey: "b"}, {ObjectKey: "c"}}, NextCursor: "c"}, nil
				case "c":
					return CleanupPage{}, nil
				}
				t.Fatal(cursor)
				return CleanupPage{}, nil
			}, finish: func(ctx context.Context, id, token string, at time.Time, complete bool) error {
				finishes++
				checkCleanupTimeout(t, ctx, 5*time.Second)
				if id != "image" || token != "token" || complete == partial || !at.After(time.Now()) {
					t.Fatal("finalization", complete)
				}
				return nil
			}}
			registry := cleanupRegistryMock(func(ctx context.Context, k string) (StorageObject, error) {
				checkCleanupTimeout(t, ctx, 5*time.Second)
				return StorageObject{ObjectKey: k, State: StorageStateDiscarded}, nil
			})
			storage := cleanupStorageMock(func(ctx context.Context, k string) error {
				checkCleanupTimeout(t, ctx, 30*time.Second)
				deletes++
				if partial && k == "b" {
					return io.ErrUnexpectedEOF
				}
				return nil
			})
			s := NewCleanupService(repo, registry, storage, cleanupLogger())
			s.cleanupTombstone(context.Background())
			s.cleanupTombstone(context.Background())
			if deletes != 4 || pages != 6 || finishes != 2 {
				t.Fatal(deletes, pages, finishes)
			}
		})
	}
}

func TestCleanupUncertainTransactions(t *testing.T) {
	for _, stage := range []string{"claim", "find", "finish"} {
		t.Run(stage, func(t *testing.T) {
			deletes := 0
			repo := cleanupRepoMock{claim: func(context.Context, int) ([]CleanupClaim, error) {
				c := []CleanupClaim{{ImageID: "i", Token: "t", LeaseUntil: time.Now().Add(CleanupLease)}}
				if stage == "claim" {
					return c, ErrCleanupCommitUncertain
				}
				return c, nil
			}, find: func(context.Context, int, string, time.Duration) (CleanupPage, error) {
				p := CleanupPage{Objects: []StorageObject{{ObjectKey: "x"}}}
				if stage == "find" {
					return p, ErrCleanupCommitUncertain
				}
				return p, nil
			}, finish: func(context.Context, string, string, time.Time, bool) error { return ErrCleanupCommitUncertain }}
			s := NewCleanupService(repo, cleanupRegistryMock(func(context.Context, string) (StorageObject, error) {
				return StorageObject{State: StorageStateDiscarded}, nil
			}), cleanupStorageMock(func(context.Context, string) error { deletes++; return nil }), cleanupLogger())
			s.cleanupTombstone(context.Background())
			expected := 0
			if stage == "finish" {
				expected = 1
			}
			if deletes != expected {
				t.Fatal("unsafe deletion or compensation", deletes)
			}
		})
	}
}

func TestCleanupRegistryProtection(t *testing.T) {
	for _, state := range []StorageState{StorageStateWriting, StorageStateRetained} {
		s := NewCleanupService(nil, cleanupRegistryMock(func(context.Context, string) (StorageObject, error) { return StorageObject{State: state}, nil }), cleanupStorageMock(func(context.Context, string) error { t.Fatal("protected object deleted"); return nil }), cleanupLogger())
		if s.deleteObject(context.Background(), "x") == nil {
			t.Fatal("protection not checked")
		}
	}
	cause := errors.New("postgres unavailable")
	s := NewCleanupService(nil, cleanupRegistryMock(func(context.Context, string) (StorageObject, error) { return StorageObject{}, cause }), nil, cleanupLogger())
	if !errors.Is(s.deleteObject(context.Background(), "x"), cause) {
		t.Fatal("lost cause")
	}
}

func TestCleanupCancellationAndCursor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	repo := cleanupRepoMock{claim: func(context.Context, int) ([]CleanupClaim, error) { return nil, nil }, find: func(context.Context, int, string, time.Duration) (CleanupPage, error) {
		return CleanupPage{Objects: []StorageObject{{ObjectKey: "a"}, {ObjectKey: "b"}}, NextCursor: "b"}, nil
	}}
	s := NewCleanupService(repo, cleanupRegistryMock(func(context.Context, string) (StorageObject, error) {
		return StorageObject{State: StorageStateDiscarded}, nil
	}), cleanupStorageMock(func(context.Context, string) error { cancel(); return context.Canceled }), cleanupLogger())
	scan := cleanupScan{}
	s.cleanupPass(ctx, &scan)
	if scan.cursor != "a" || !scan.failed {
		t.Fatal("lost partial scan position or error", scan)
	}
}

func TestCleanupRunWaitAndStop(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "error"}[failed], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := make(chan struct{}, 2)
			repo := cleanupRepoMock{claim: func(context.Context, int) ([]CleanupClaim, error) {
				calls <- struct{}{}
				if failed {
					return nil, ErrCleanupCommitUncertain
				}
				return nil, nil
			}, find: func(context.Context, int, string, time.Duration) (CleanupPage, error) { return CleanupPage{}, nil }}
			done := make(chan error, 1)
			go func() { done <- NewCleanupService(repo, nil, nil, cleanupLogger()).Run(ctx) }()
			select {
			case <-calls:
			case <-time.After(time.Second):
				t.Fatal("not started")
			}
			select {
			case <-calls:
				t.Fatal("busy loop")
			case <-time.After(30 * time.Millisecond):
			}
			cancel()
			select {
			case e := <-done:
				if e != nil {
					t.Fatal(e)
				}
			case <-time.After(time.Second):
				t.Fatal("not stopped")
			}
		})
	}
}

func TestCleanupFencedClaim(t *testing.T) {
	repo := cleanupRepoMock{claim: func(context.Context, int) ([]CleanupClaim, error) {
		return []CleanupClaim{{LeaseUntil: time.Now().Add(-time.Second)}}, nil
	}, finish: func(context.Context, string, string, time.Time, bool) error { return ErrCleanupFenced }}
	s := NewCleanupService(repo, nil, nil, cleanupLogger())
	s.cleanupTombstone(context.Background())
}

func TestCleanupStructuredLogging(t *testing.T) {
	for _, tc := range []struct {
		err   error
		level string
	}{{ErrCleanupCommitUncertain, "WARN"}, {ErrCleanupFenced, "DEBUG"}, {io.ErrUnexpectedEOF, "ERROR"}} {
		var output bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
		s := NewCleanupService(nil, nil, nil, logger)
		s.logError(context.Background(), "Cleanup deferred", tc.err)
		if !strings.Contains(output.String(), `"component":"storage_cleanup"`) || !strings.Contains(output.String(), `"level":"`+tc.level+`"`) || !strings.Contains(output.String(), tc.err.Error()) {
			t.Fatal(output.String())
		}
	}
}

func TestCleanupTombstonesDoNotShareScan(t *testing.T) {
	claims := []string{"first", "second", "first"}
	starts := 0
	finishes := 0
	global := 0
	expiredOnce := false
	repo := cleanupRepoMock{
		claim: func(context.Context, int) ([]CleanupClaim, error) {
			id := claims[starts]
			starts++
			lease := time.Now().Add(CleanupLease)
			if starts == 1 {
				lease = time.Now().Add(cleanupDatabaseTimeout + 30*time.Millisecond)
			}
			return []CleanupClaim{{ImageID: id, Token: id, LeaseUntil: lease}}, nil
		},
		imageFind: func(_ context.Context, _ int, cursor string, _ time.Duration, c CleanupClaim) (CleanupPage, error) {
			if cursor != "" {
				t.Fatal("tombstone resumed another or failed scan", cursor, c.ImageID)
			}
			return CleanupPage{Objects: []StorageObject{{ObjectKey: c.ImageID + "/a"}, {ObjectKey: c.ImageID + "/b"}}}, nil
		},
		find: func(context.Context, int, string, time.Duration) (CleanupPage, error) {
			global++
			return CleanupPage{}, nil
		},
		finish: func(_ context.Context, id, token string, _ time.Time, complete bool) error {
			finishes++
			if id != claims[finishes-1] || token != id || complete != (id == "second") {
				t.Fatal("false cleanup completion", id, complete)
			}
			return nil
		},
	}
	storage := cleanupStorageMock(func(ctx context.Context, key string) error {
		if key == "first/a" {
			return io.ErrUnexpectedEOF
		}
		if key == "first/b" && !expiredOnce {
			expiredOnce = true
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	s := NewCleanupService(repo, cleanupRegistryMock(func(context.Context, string) (StorageObject, error) {
		return StorageObject{State: StorageStateDiscarded}, nil
	}), storage, cleanupLogger())
	scan := cleanupScan{cursor: "unrelated/global/cursor"}
	for range claims {
		s.cleanupPass(context.Background(), &scan)
	}
	if finishes != 3 || global != 3 {
		t.Fatal(finishes, global)
	}
}

func TestCleanupGlobalFailureSurvivesDeadline(t *testing.T) {
	interrupted := false
	repo := cleanupRepoMock{find: func(ctx context.Context, _ int, cursor string, _ time.Duration) (CleanupPage, error) {
		if cursor == "" {
			return CleanupPage{Objects: []StorageObject{{ObjectKey: "a"}}, NextCursor: "a"}, nil
		}
		if !interrupted {
			interrupted = true
			<-ctx.Done()
			return CleanupPage{}, ctx.Err()
		}
		return CleanupPage{Objects: []StorageObject{{ObjectKey: "b"}}}, nil
	}}
	s := NewCleanupService(repo, cleanupRegistryMock(func(context.Context, string) (StorageObject, error) {
		return StorageObject{State: StorageStateDiscarded}, nil
	}), cleanupStorageMock(func(_ context.Context, k string) error {
		if k == "a" {
			return io.ErrUnexpectedEOF
		}
		return nil
	}), cleanupLogger())
	scan := cleanupScan{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := s.sweep(ctx, &scan, nil); !errors.Is(err, context.DeadlineExceeded) || scan.cursor != "a" || !scan.failed {
		t.Fatal("lost interrupted scan", scan, err)
	}
	if err := s.sweep(context.Background(), &scan, nil); err == nil {
		t.Fatal("earlier delete failure forgotten")
	}
	if scan.cursor != "" || scan.failed {
		t.Fatal("new scan was not reset", scan)
	}
}

func TestCleanupGlobalDoesNotRequireClaim(t *testing.T) {
	deleted := false
	repo := cleanupRepoMock{claim: func(context.Context, int) ([]CleanupClaim, error) { return nil, ErrCleanupCommitUncertain }, find: func(context.Context, int, string, time.Duration) (CleanupPage, error) {
		return CleanupPage{Objects: []StorageObject{{ObjectKey: "orphan"}}}, nil
	}}
	s := NewCleanupService(repo, cleanupRegistryMock(func(context.Context, string) (StorageObject, error) {
		return StorageObject{State: StorageStateDiscarded}, nil
	}), cleanupStorageMock(func(context.Context, string) error { deleted = true; return nil }), cleanupLogger())
	s.cleanupPass(context.Background(), &cleanupScan{})
	if !deleted {
		t.Fatal("claim error prevented independently authorized global cleanup")
	}
}
