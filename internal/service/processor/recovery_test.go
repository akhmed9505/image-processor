package processor

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

type recoveryRepositoryMock struct {
	recover    func(context.Context, int, func(int) time.Duration) ([]string, error)
	redispatch func(context.Context, int) ([]string, error)
}

func (m recoveryRepositoryMock) Recover(c context.Context, n int, f func(int) time.Duration) ([]string, error) {
	return m.recover(c, n, f)
}
func (m recoveryRepositoryMock) Redispatch(c context.Context, n int) ([]string, error) {
	return m.redispatch(c, n)
}

func TestRecoveryPass(t *testing.T) {
	var log bytes.Buffer
	calls := 0
	var firstCtx context.Context
	check := func(ctx context.Context, n int) {
		t.Helper()
		deadline, ok := ctx.Deadline()
		if n != 100 || !ok || time.Until(deadline) > 5*time.Second || ctx.Err() != nil {
			t.Fatal("batch limits or timeout", n, ctx.Err())
		}
	}
	repo := recoveryRepositoryMock{
		recover: func(ctx context.Context, n int, delay func(int) time.Duration) ([]string, error) {
			calls++
			firstCtx = ctx
			check(ctx, n)
			for i, base := range []time.Duration{5 * time.Second, 15 * time.Second, 45 * time.Second, 135 * time.Second} {
				d := delay(i + 1)
				if d < base*4/5 || d > base*6/5 {
					t.Fatal("retry policy", d)
				}
			}
			if delay(5) != 0 {
				t.Fatal("exhausted attempt retried")
			}
			return []string{"a", "b"}, nil
		}, redispatch: func(ctx context.Context, n int) ([]string, error) {
			calls++
			check(ctx, n)
			if firstCtx.Err() != context.Canceled || ctx == firstCtx {
				t.Fatal("database contexts reused")
			}
			return []string{"c"}, nil
		}}
	NewRecoveryService(repo, slog.New(slog.NewJSONHandler(&log, nil))).recoverPass(context.Background())
	if calls != 2 || !strings.Contains(log.String(), `"jobs":2`) || !strings.Contains(log.String(), `"operation":"redispatch"`) || !strings.Contains(log.String(), `"component":"job_recovery"`) {
		t.Fatal(calls, log.String())
	}
}

func TestRecoveryPassErrors(t *testing.T) {
	for _, operation := range []string{"recover", "redispatch"} {
		for _, cause := range []error{errors.New("database unavailable"), ErrJobCommitUncertain, context.DeadlineExceeded} {
			t.Run(operation+"/"+cause.Error(), func(t *testing.T) {
				var log bytes.Buffer
				calls := 0
				result := func(stage string) ([]string, error) {
					calls++
					if stage == operation {
						return []string{"unconfirmed"}, cause
					}
					return nil, nil
				}
				repo := recoveryRepositoryMock{recover: func(context.Context, int, func(int) time.Duration) ([]string, error) { return result("recover") }, redispatch: func(context.Context, int) ([]string, error) { return result("redispatch") }}
				NewRecoveryService(repo, slog.New(slog.NewJSONHandler(&log, nil))).recoverPass(context.Background())
				level := "ERROR"
				if errors.Is(cause, ErrJobCommitUncertain) {
					level = "WARN"
				}
				if calls != 2 || !strings.Contains(log.String(), `"level":"`+level+`"`) || strings.Contains(log.String(), `"jobs"`) || !strings.Contains(log.String(), cause.Error()) {
					t.Fatal("unconfirmed batch reported or wrong log", calls, log.String())
				}
			})
		}
	}
}

func TestRecoveryRunCancellation(t *testing.T) {
	for _, stage := range []string{"before", "recover", "redispatch", "wait"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if stage == "before" {
				cancel()
			}
			reached := make(chan struct{})
			repo := recoveryRepositoryMock{recover: func(c context.Context, _ int, _ func(int) time.Duration) ([]string, error) {
				if stage == "before" {
					t.Error("query after cancellation")
				}
				if stage == "recover" {
					close(reached)
					<-c.Done()
					return nil, c.Err()
				}
				return nil, nil
			}, redispatch: func(c context.Context, _ int) ([]string, error) {
				if stage == "recover" {
					t.Error("redispatch after cancellation")
				}
				close(reached)
				if stage == "redispatch" {
					<-c.Done()
					return nil, c.Err()
				}
				return nil, nil
			}}
			done := make(chan error, 1)
			go func() { done <- NewRecoveryService(repo, slog.New(slog.NewTextHandler(io.Discard, nil))).Run(ctx) }()
			if stage != "before" {
				select {
				case <-reached:
				case <-time.After(time.Second):
					t.Fatal("service did not start")
				}
			}
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("service did not stop")
			}
		})
	}
}

func TestRecoveryRunInterval(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "idle", true: "error"}[failed], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := make(chan time.Time, 3)
			repo := recoveryRepositoryMock{recover: func(context.Context, int, func(int) time.Duration) ([]string, error) {
				calls <- time.Now()
				if failed {
					return nil, ErrJobCommitUncertain
				}
				return nil, nil
			}, redispatch: func(context.Context, int) ([]string, error) { return nil, nil }}
			done := make(chan error, 1)
			go func() { done <- NewRecoveryService(repo, slog.New(slog.NewTextHandler(io.Discard, nil))).Run(ctx) }()
			first := <-calls
			select {
			case second := <-calls:
				if second.Sub(first) < RecoveryInterval {
					t.Fatal("busy loop", second.Sub(first))
				}
			case <-time.After(7 * time.Second):
				t.Fatal("no subsequent pass")
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("service did not stop")
			}
		})
	}
}
