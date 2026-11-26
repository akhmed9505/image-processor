package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/domain"
	"github.com/akhmed9505/image-processor/internal/service/processor"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

type clientMock struct {
	poll   func(context.Context, int) kgo.Fetches
	commit func(context.Context, ...*kgo.Record) error
	rewind func(map[string]map[int32]kgo.EpochOffset)
	allow  func()
}

func (m clientMock) PollRecords(c context.Context, n int) kgo.Fetches { return m.poll(c, n) }
func (m clientMock) CommitRecords(c context.Context, r ...*kgo.Record) error {
	return m.commit(c, r...)
}
func (m clientMock) SetOffsets(o map[string]map[int32]kgo.EpochOffset) { m.rewind(o) }
func (m clientMock) AllowRebalance() {
	if m.allow != nil {
		m.allow()
	}
}

type jobsMock struct {
	admit   func(context.Context, string) (processor.ClaimResult, error)
	process func(context.Context, processor.ClaimResult) (domain.Job, error)
}

func (m jobsMock) Admit(c context.Context, id string) (processor.ClaimResult, error) {
	return m.admit(c, id)
}
func (m jobsMock) ProcessAttempt(c context.Context, a processor.ClaimResult) (domain.Job, error) {
	return m.process(c, a)
}
func logger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
func requested(t *testing.T, offset int64) *kgo.Record {
	t.Helper()
	id := uuid.NewString()
	payload, e := json.Marshal(map[string]any{"event_id": uuid.NewString(), "event_type": "image.processing.requested", "version": 1, "job_id": id})
	if e != nil {
		t.Fatal(e)
	}
	return &kgo.Record{Topic: "jobs", Partition: 0, Offset: offset, LeaderEpoch: 3, Key: []byte(id), Value: payload}
}
func fetched(r *kgo.Record) kgo.Fetches {
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: r.Topic, Partitions: []kgo.FetchPartition{{Partition: r.Partition, Records: []*kgo.Record{r}}}}}}}
}
func waitDone(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not stop")
	}
}

func TestAdmissionOffsetSafety(t *testing.T) {
	for _, kind := range []string{"error", "uncertain", "zero", "durable", "malformed"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			record := requested(t, 4)
			if kind == "malformed" {
				record.Value = []byte("private malformed payload")
			}
			commits, rewinds, allows := 0, 0, 0
			client := clientMock{poll: func(context.Context, int) kgo.Fetches { return fetched(record) }, commit: func(ctx context.Context, r ...*kgo.Record) error {
				commits++
				if kind != "durable" && kind != "malformed" {
					t.Error("unsafe commit")
				}
				if len(r) != 1 || r[0] != record {
					t.Error("wrong offset")
				}
				d, ok := ctx.Deadline()
				if !ok || time.Until(d) > offsetTimeout {
					t.Error("unbounded commit")
				}
				return nil
			}, rewind: func(o map[string]map[int32]kgo.EpochOffset) {
				rewinds++
				if o["jobs"][0].Offset != 4 {
					t.Error("offset skipped")
				}
			}, allow: func() { allows++; cancel() }}
			jobs := jobsMock{admit: func(context.Context, string) (processor.ClaimResult, error) {
				switch kind {
				case "error":
					return processor.ClaimResult{Disposition: processor.ClaimAcquired}, errors.New("postgres down")
				case "uncertain":
					return processor.ClaimResult{Disposition: processor.ClaimAcquired}, processor.ErrJobCommitUncertain
				case "zero":
					return processor.ClaimResult{}, nil
				case "malformed":
					t.Error("malformed admitted")
				}
				return processor.ClaimResult{Disposition: processor.ClaimTerminal}, nil
			}}
			if e := New(client, jobs, logger()).Run(ctx); e != nil {
				t.Fatal(e)
			}
			want := 0
			if kind == "durable" || kind == "malformed" {
				want = 1
			}
			if commits != want || rewinds != 1-want || allows < 1 {
				t.Fatal(commits, rewinds, allows)
			}
		})
	}
}

func TestAcquiredStartsBeforeOffsetCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	record := requested(t, 7)
	started := make(chan struct{})
	release := make(chan struct{})
	jobs := jobsMock{admit: func(context.Context, string) (processor.ClaimResult, error) {
		return processor.ClaimResult{Disposition: processor.ClaimAcquired}, nil
	}, process: func(ctx context.Context, _ processor.ClaimResult) (domain.Job, error) {
		close(started)
		<-release
		if ctx.Err() != nil {
			t.Error("attempt cancelled without drain")
		}
		return domain.Job{}, errors.New("attempt error")
	}}
	rewound := false
	client := clientMock{poll: func(context.Context, int) kgo.Fetches { return fetched(record) }, commit: func(context.Context, ...*kgo.Record) error {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Error("processing waited for offset commit")
		}
		cancel()
		return errors.New("commit failed")
	}, rewind: func(o map[string]map[int32]kgo.EpochOffset) { rewound = o["jobs"][0].Offset == 7 }}
	done := make(chan error, 1)
	go func() { done <- New(client, jobs, logger()).Run(ctx) }()
	<-started
	select {
	case <-done:
		t.Fatal("shutdown did not wait for attempt")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	waitDone(t, done)
	if !rewound {
		t.Fatal("failed commit not replayed")
	}
}

func TestWorkerPoolBackpressure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var polls, admissions, running, maxRunning atomic.Int32
	jobs := jobsMock{admit: func(context.Context, string) (processor.ClaimResult, error) {
		admissions.Add(1)
		return processor.ClaimResult{Disposition: processor.ClaimAcquired}, nil
	}, process: func(context.Context, processor.ClaimResult) (domain.Job, error) {
		n := running.Add(1)
		for old := maxRunning.Load(); n > old && !maxRunning.CompareAndSwap(old, n); old = maxRunning.Load() {
		}
		started <- struct{}{}
		<-release
		running.Add(-1)
		return domain.Job{}, nil
	}}
	records := []*kgo.Record{requested(t, 0), requested(t, 1), requested(t, 2)}
	client := clientMock{poll: func(_ context.Context, n int) kgo.Fetches {
		if n != 1 {
			t.Error("application fetch queue")
		}
		i := polls.Add(1) - 1
		return fetched(records[i])
	}, commit: func(context.Context, ...*kgo.Record) error { return nil }, rewind: func(map[string]map[int32]kgo.EpochOffset) { t.Error("unexpected rewind") }}
	done := make(chan error, 1)
	go func() { done <- New(client, jobs, logger()).Run(ctx) }()
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("workers did not start")
		}
	}
	time.Sleep(30 * time.Millisecond)
	if polls.Load() != 2 || admissions.Load() != 2 || maxRunning.Load() != 2 {
		t.Fatal("slot not reserved before admission", polls.Load(), admissions.Load(), maxRunning.Load())
	}
	cancel()
	close(release)
	waitDone(t, done)
}

func TestDecodeRequested(t *testing.T) {
	good := requested(t, 0)
	for _, field := range []string{"event_id", "event_type", "version", "job_id", "key", "json", "trailing"} {
		t.Run(field, func(t *testing.T) {
			copy := *good
			var data map[string]any
			_ = json.Unmarshal(good.Value, &data)
			switch field {
			case "key":
				copy.Key = []byte("wrong")
			case "json":
				copy.Value = []byte("{")
			case "trailing":
				copy.Value = append(append([]byte{}, good.Value...), []byte(" {}")...)
			default:
				delete(data, field)
				copy.Value, _ = json.Marshal(data)
			}
			if _, e := decodeRequested(&copy); e == nil {
				t.Fatal("invalid event accepted")
			}
		})
	}
	if _, e := decodeRequested(good); e != nil {
		t.Fatal(e)
	}
}

func TestOffsetGapReplaysBeforeLaterRecord(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	records := []*kgo.Record{requested(t, 4), requested(t, 6)}
	next := 0
	admissions := 0
	committed := []int64{}
	client := clientMock{poll: func(context.Context, int) kgo.Fetches { return fetched(records[next]) }, rewind: func(o map[string]map[int32]kgo.EpochOffset) {
		if o["jobs"][0].Offset != 4 {
			t.Error("gap not rewound")
		}
		next = 0
	}, commit: func(_ context.Context, r ...*kgo.Record) error {
		committed = append(committed, r[0].Offset)
		next++
		if next == 2 {
			cancel()
		}
		return nil
	}}
	jobs := jobsMock{admit: func(context.Context, string) (processor.ClaimResult, error) {
		admissions++
		if admissions == 1 {
			next++
			return processor.ClaimResult{}, errors.New("postgres down")
		}
		return processor.ClaimResult{Disposition: processor.ClaimWaitingRetry}, nil
	}}
	if e := New(client, jobs, logger()).Run(ctx); e != nil {
		t.Fatal(e)
	}
	if admissions != 3 || len(committed) != 2 || committed[0] != 4 || committed[1] != 6 {
		t.Fatal("earlier admission skipped", admissions, committed)
	}
}

func TestShutdownDeadlineWaitsForWorkerCleanup(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	cancelled := make(chan struct{})
	release := make(chan struct{})
	cleaned := make(chan struct{})
	var once sync.Once
	releaseCleanup := func() { once.Do(func() { close(release) }) }
	defer releaseCleanup()
	jobs := jobsMock{admit: func(context.Context, string) (processor.ClaimResult, error) {
		return processor.ClaimResult{Disposition: processor.ClaimAcquired}, nil
	}, process: func(c context.Context, _ processor.ClaimResult) (domain.Job, error) {
		defer close(cleaned)
		close(started)
		<-c.Done()
		close(cancelled)
		<-release
		return domain.Job{}, c.Err()
	}}
	record := requested(t, 0)
	client := clientMock{poll: func(context.Context, int) kgo.Fetches { return fetched(record) }, commit: func(context.Context, ...*kgo.Record) error { <-started; cancel(); return nil }}
	before := time.Now()
	done := make(chan error, 1)
	go func() { done <- New(client, jobs, logger()).Run(ctx) }()
	select {
	case <-cancelled:
	case e := <-done:
		t.Fatal("Run returned before forced cancellation and cleanup", e)
	case <-time.After(shutdownTimeout + 3*time.Second):
		t.Fatal("forced cancellation missing")
	}
	if time.Since(before) < shutdownTimeout {
		t.Fatal("graceful budget shortened")
	}
	select {
	case e := <-done:
		t.Fatal("Run returned while worker was releasing resources", e)
	case <-time.After(30 * time.Millisecond):
	}
	releaseCleanup()
	waitDone(t, done)
	select {
	case <-cleaned:
	default:
		t.Fatal("successful Run returned before resource cleanup")
	}
}

func TestShutdownUnresponsiveWorkerHasCompletionSignal(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	cleaned := make(chan struct{})
	var once sync.Once
	releaseWorker := func() { once.Do(func() { close(release) }) }
	defer releaseWorker()
	jobs := jobsMock{admit: func(context.Context, string) (processor.ClaimResult, error) {
		return processor.ClaimResult{Disposition: processor.ClaimAcquired}, nil
	}, process: func(context.Context, processor.ClaimResult) (domain.Job, error) {
		defer close(cleaned)
		close(started)
		<-release
		return domain.Job{}, nil
	}}
	record := requested(t, 0)
	client := clientMock{poll: func(context.Context, int) kgo.Fetches { return fetched(record) }, commit: func(context.Context, ...*kgo.Record) error { <-started; cancel(); return nil }}
	done := make(chan error, 1)
	go func() { done <- New(client, jobs, logger()).Run(ctx) }()
	var shutdownErr *ShutdownError
	select {
	case e := <-done:
		if !errors.As(e, &shutdownErr) {
			t.Fatal("unresponsive worker reported safe shutdown", e)
		}
	case <-time.After(shutdownTimeout + workerStopTimeout + 3*time.Second):
		t.Fatal("Run waited indefinitely")
	}
	select {
	case <-shutdownErr.Done:
		t.Fatal("completion signaled while worker was active")
	default:
	}
	releaseWorker()
	select {
	case <-shutdownErr.Done:
	case <-time.After(time.Second):
		t.Fatal("completion blocked after Run returned")
	}
	select {
	case <-cleaned:
	default:
		t.Fatal("completion preceded worker cleanup")
	}
}

func TestMalformedLoggingDoesNotExposePayload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	record := requested(t, 42)
	record.Value = []byte("secret malformed bytes")
	client := clientMock{poll: func(context.Context, int) kgo.Fetches { return fetched(record) }, commit: func(context.Context, ...*kgo.Record) error { cancel(); return nil }}
	var output bytes.Buffer
	if e := New(client, jobsMock{}, slog.New(slog.NewJSONHandler(&output, nil))).Run(ctx); e != nil {
		t.Fatal(e)
	}
	if strings.Contains(output.String(), "secret") || !strings.Contains(output.String(), `"offset":42`) || !strings.Contains(output.String(), `"component":"job_consumer"`) {
		t.Fatal(output.String())
	}
}

func TestCancellationBeforePoll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := clientMock{poll: func(context.Context, int) kgo.Fetches { t.Fatal("poll after shutdown"); return nil }}
	if e := New(client, jobsMock{}, logger()).Run(ctx); e != nil {
		t.Fatal(e)
	}
}
