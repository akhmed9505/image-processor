package outbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

type repositoryMock struct {
	claim      func(context.Context, int) ([]Event, error)
	mark       func(context.Context, string, string) error
	reschedule func(context.Context, string, string, string, time.Time) error
}

func (m repositoryMock) Claim(ctx context.Context, n int) ([]Event, error) { return m.claim(ctx, n) }
func (m repositoryMock) MarkPublished(ctx context.Context, id, token string) error {
	return m.mark(ctx, id, token)
}
func (m repositoryMock) Reschedule(ctx context.Context, id, token, code string, at time.Time) error {
	return m.reschedule(ctx, id, token, code, at)
}

type producerMock func(context.Context, string, []byte, []byte) error

func (m producerMock) Publish(ctx context.Context, topic string, key, payload []byte) error {
	return m(ctx, topic, key, payload)
}
func testEvent() Event {
	return Event{ID: "event", Topic: "jobs", MessageKey: "job", Payload: json.RawMessage(`{"version":1}`), ClaimToken: "token", LeaseUntil: time.Now().Add(ClaimLease), PublishAttempts: 3}
}
func quietLogger() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }
func TestPublishOne(t *testing.T) {
	for _, kind := range []string{"success", "publish failure", "mark failure", "mark fenced", "mark uncertain", "reschedule failure", "reschedule uncertain"} {
		t.Run(kind, func(t *testing.T) {
			event := testEvent()
			cause := errors.New("dependency failure")
			calls := []string{}
			expected := error(nil)
			if kind != "success" {
				expected = cause
			}
			if kind == "mark fenced" {
				expected = ErrClaimFenced
			}
			if kind == "mark uncertain" {
				expected = ErrCommitUncertain
			}
			repo := repositoryMock{
				claim: func(ctx context.Context, limit int) ([]Event, error) {
					calls = append(calls, "claim")
					if limit != 1 {
						t.Fatal("claimed a sequential batch")
					}
					return []Event{event}, nil
				},
				mark: func(ctx context.Context, id, token string) error {
					calls = append(calls, "mark")
					if id != event.ID || token != event.ClaimToken {
						t.Fatal("claim identity lost")
					}
					if strings.HasPrefix(kind, "mark") {
						return expected
					}
					return nil
				},
				reschedule: func(ctx context.Context, id, token, code string, at time.Time) error {
					calls = append(calls, "reschedule")
					if id != event.ID || token != event.ClaimToken || code != "publish_failed" || time.Until(at) < 3*time.Second || time.Until(at) > 4*time.Second {
						t.Fatal("incorrect backoff or fencing", at)
					}
					if kind == "reschedule failure" {
						return ErrClaimFenced
					}
					if kind == "reschedule uncertain" {
						return ErrCommitUncertain
					}
					return nil
				},
			}
			producer := producerMock(func(ctx context.Context, topic string, key, value []byte) error {
				calls = append(calls, "publish")
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > PublishTimeout || time.Until(deadline) < 9*time.Second {
					t.Fatal("missing 10-second timeout")
				}
				if topic != event.Topic || string(key) != event.MessageKey || !bytes.Equal(value, event.Payload) {
					t.Fatal("wire record changed")
				}
				if kind == "publish failure" || strings.HasPrefix(kind, "reschedule") {
					return cause
				}
				return nil
			})
			worked, e := New(repo, producer, quietLogger()).publishOne(context.Background())
			if !worked || !errors.Is(e, expected) {
				t.Fatalf("result %v %v want %v", worked, e, expected)
			}
			if len(calls) != 3 || calls[0] != "claim" || calls[1] != "publish" {
				t.Fatal("unexpected sequence", calls)
			}
			want := "mark"
			if kind == "publish failure" || strings.HasPrefix(kind, "reschedule") {
				want = "reschedule"
			}
			if kind == "reschedule uncertain" && !errors.Is(e, ErrCommitUncertain) {
				t.Fatal("reschedule uncertainty lost", e)
			}
			if calls[2] != want {
				t.Fatal("unsafe finalization", calls)
			}
		})
	}
}
func TestUncertainClaimDoesNotPublish(t *testing.T) {
	repo := repositoryMock{claim: func(context.Context, int) ([]Event, error) { return []Event{testEvent()}, ErrCommitUncertain }}
	worked, e := New(repo, nil, quietLogger()).publishOne(context.Background())
	if worked || !errors.Is(e, ErrCommitUncertain) {
		t.Fatal(worked, e)
	}
}
func TestCancellationDuringPublish(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	repo := repositoryMock{claim: func(context.Context, int) ([]Event, error) { return []Event{testEvent()}, nil }}
	producer := producerMock(func(ctx context.Context, _ string, _, _ []byte) error { cancel(); <-ctx.Done(); return ctx.Err() })
	if e := New(repo, producer, quietLogger()).Run(ctx); e != nil {
		t.Fatal(e)
	}
}
func TestRunWaitsOnEmptyPollAndErrors(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "SQL unavailable"}[fail], func(t *testing.T) {
			calls := 0
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			repo := repositoryMock{claim: func(context.Context, int) ([]Event, error) {
				calls++
				if fail {
					return nil, errors.New("unavailable")
				}
				return nil, nil
			}}
			if e := New(repo, nil, quietLogger()).Run(ctx); e != nil || calls != 1 {
				t.Fatal("busy loop", calls, e)
			}
		})
	}
}
func TestBoundedBackoff(t *testing.T) {
	for attempts, want := range map[int]time.Duration{-1: time.Second, 1: time.Second, 2: 2 * time.Second, 5: 16 * time.Second, 6: 30 * time.Second, 1000000: 30 * time.Second} {
		if got := backoff(attempts); got != want {
			t.Fatalf("attempt %d: %v want %v", attempts, got, want)
		}
	}
}
func TestFailureLogsContainEventMetadata(t *testing.T) {
	var output bytes.Buffer
	event := testEvent()
	cause := errors.New("network unavailable")
	repo := repositoryMock{claim: func(context.Context, int) ([]Event, error) { return []Event{event}, nil }, reschedule: func(context.Context, string, string, string, time.Time) error { return nil }}
	New(repo, producerMock(func(context.Context, string, []byte, []byte) error { return cause }), slog.New(slog.NewJSONHandler(&output, nil))).publishOne(context.Background())
	var record map[string]any
	if e := json.Unmarshal(output.Bytes(), &record); e != nil || record["event_id"] != event.ID || record["topic"] != event.Topic || record["error_code"] != "publish_failed" {
		t.Fatal("missing structured metadata", output.String(), e)
	}
	if strings.Contains(output.String(), string(event.Payload)) {
		t.Fatal("payload logged")
	}
}
