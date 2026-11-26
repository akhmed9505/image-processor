//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/config"
	delivery "github.com/akhmed9505/image-processor/internal/delivery/kafka"
	"github.com/akhmed9505/image-processor/internal/domain"
	infrastructure "github.com/akhmed9505/image-processor/internal/infra/kafka"
	"github.com/akhmed9505/image-processor/internal/service/processor"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func startJobConsumer(t *testing.T, cfg config.Kafka, jobs delivery.Jobs, wrap func(*kgo.Client) delivery.Client) func() {
	t.Helper()
	client, e := infrastructure.NewConsumer(context.Background(), cfg)
	if e != nil {
		t.Fatal(e)
	}
	var port delivery.Client = client
	if wrap != nil {
		port = wrap(client)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- delivery.New(port, jobs, slog.New(slog.NewTextHandler(io.Discard, nil))).Run(ctx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case e := <-done:
				if e != nil {
					t.Error(e)
				}
			case <-time.After(32 * time.Second):
				t.Error("consumer did not drain")
			}
			client.CloseAllowingRebalance()
		})
	}
	t.Cleanup(stop)
	return stop
}
func waitConsumerOffsets(t *testing.T, ctx context.Context, admin *kadm.Client, cfg config.Kafka, total int64) {
	t.Helper()
	awaitOutboxState(t, ctx, func() (bool, error) {
		offsets, e := admin.FetchOffsets(ctx, cfg.ConsumerGroup)
		if e != nil {
			return false, e
		}
		var n int64
		for _, p := range offsets[cfg.JobsTopic] {
			if p.At > 0 {
				n += p.At
			}
		}
		return n == total, nil
	})
}
func publishJobEvent(t *testing.T, p *pgxpool.Pool, producer *infrastructure.Producer, cfg config.Kafka, id string) {
	t.Helper()
	var payload []byte
	if e := p.QueryRow(context.Background(), "SELECT payload FROM outbox WHERE job_id=$1 AND event_type='image.processing.requested'", id).Scan(&payload); e != nil {
		t.Fatal(e)
	}
	if e := producer.Publish(context.Background(), cfg.JobsTopic, []byte(id), payload); e != nil {
		t.Fatal(e)
	}
}

type rebalanceJobs struct {
	*processor.JobService
	started chan struct{}
	release chan struct{}
}

func (j rebalanceJobs) ProcessAttempt(ctx context.Context, a processor.ClaimResult) (domain.Job, error) {
	select {
	case j.started <- struct{}{}:
	default:
	}
	select {
	case <-j.release:
	case <-ctx.Done():
		return domain.Job{}, ctx.Err()
	}
	return j.JobService.ProcessAttempt(ctx, a)
}

func TestJobConsumerProcessingAndRebalance(t *testing.T) {
	p := migratedReadPool(t)
	s := uploadStorage(t)
	cfg := kafkaConfig()
	admin := kafkaAdmin(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	if e := infrastructure.InitTopics(ctx, cfg); e != nil {
		t.Fatal(e)
	}
	producer, e := infrastructure.NewProducer(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer producer.Close()
	jobs := rebalanceJobs{processor.NewJobService(processingRepo(t, p), s), make(chan struct{}, 2), make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-jobs.release:
		default:
			close(jobs.release)
		}
	})
	stop1 := startJobConsumer(t, cfg, jobs, nil)
	var all []domain.Job
	for range 2 {
		image, e := imageUpload(t, p, s).Upload(ctx, "input.png", bytes.NewReader(uploadBytes(t, "png")), uploadOperations())
		if e != nil {
			t.Fatal(e)
		}
		all = append(all, image.Jobs...)
	}
	for _, j := range all {
		publishJobEvent(t, p, producer, cfg, j.ID)
	}
	for range 2 {
		select {
		case <-jobs.started:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	stop2 := startJobConsumer(t, cfg, jobs, nil)
	awaitOutboxState(t, ctx, func() (bool, error) {
		groups, e := admin.DescribeGroups(ctx, cfg.ConsumerGroup)
		g := groups[cfg.ConsumerGroup]
		return e == nil && g.Err == nil && g.State == "Stable" && len(g.Members) == 2, e
	})
	close(jobs.release)
	waitConsumerOffsets(t, ctx, admin, cfg, 6)
	for _, j := range all {
		publishJobEvent(t, p, producer, cfg, j.ID)
	}
	if e := producer.Publish(ctx, cfg.JobsTopic, []byte("malformed"), []byte("private invalid payload")); e != nil {
		t.Fatal(e)
	}
	waitConsumerOffsets(t, ctx, admin, cfg, 13)
	stop1()
	stop2()
	var completed int
	if e := p.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE status='completed' AND attempts=1").Scan(&completed); e != nil || completed != 6 {
		t.Fatal("duplicate attempts or missing results", completed, e)
	}
	for _, job := range all {
		var key string
		if e := p.QueryRow(ctx, "SELECT result_key FROM jobs WHERE id=$1", job.ID).Scan(&key); e != nil {
			t.Fatal(e)
		}
		if _, e := s.Stat(ctx, key); e != nil {
			t.Fatal("result not stored", e)
		}
	}
}

type commitFailureClient struct {
	*kgo.Client
	failed  atomic.Bool
	started <-chan struct{}
}

func (c *commitFailureClient) CommitRecords(ctx context.Context, r ...*kgo.Record) error {
	if c.failed.CompareAndSwap(false, true) {
		select {
		case <-c.started:
		case <-ctx.Done():
			return ctx.Err()
		}
		return errors.New("injected Kafka commit failure")
	}
	return c.Client.CommitRecords(ctx, r...)
}

type consumerJobs struct {
	*processor.JobService
	started       chan struct{}
	once          sync.Once
	admissions    atomic.Int32
	failAdmission bool
}

func (j *consumerJobs) Admit(ctx context.Context, id string) (processor.ClaimResult, error) {
	if j.admissions.Add(1) == 1 && j.failAdmission {
		return processor.ClaimResult{Disposition: processor.ClaimAcquired}, processor.ErrJobCommitUncertain
	}
	return j.JobService.Admit(ctx, id)
}
func (j *consumerJobs) ProcessAttempt(ctx context.Context, a processor.ClaimResult) (domain.Job, error) {
	j.once.Do(func() { close(j.started) })
	return j.JobService.ProcessAttempt(ctx, a)
}
func TestJobConsumerFailureReplay(t *testing.T) {
	for _, admissionFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "admission"}[admissionFailure], func(t *testing.T) {
			p := migratedReadPool(t)
			s := uploadStorage(t)
			cfg := kafkaConfig()
			admin := kafkaAdmin(t, cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if e := infrastructure.InitTopics(ctx, cfg); e != nil {
				t.Fatal(e)
			}
			producer, e := infrastructure.NewProducer(ctx, cfg)
			if e != nil {
				t.Fatal(e)
			}
			defer producer.Close()
			image, e := imageUpload(t, p, s).Upload(ctx, "input", bytes.NewReader(uploadBytes(t, "png")), uploadOperations()[:1])
			if e != nil {
				t.Fatal(e)
			}
			jobs := &consumerJobs{JobService: processor.NewJobService(processingRepo(t, p), s), started: make(chan struct{}), failAdmission: admissionFailure}
			var wrap func(*kgo.Client) delivery.Client
			if !admissionFailure {
				wrap = func(client *kgo.Client) delivery.Client {
					return &commitFailureClient{Client: client, started: jobs.started}
				}
			}
			stop := startJobConsumer(t, cfg, jobs, wrap)
			for range 2 {
				publishJobEvent(t, p, producer, cfg, image.Jobs[0].ID)
			}
			waitConsumerOffsets(t, ctx, admin, cfg, 2)
			stop()
			if jobs.admissions.Load() < 3 {
				t.Fatal("failed offset not replayed", jobs.admissions.Load())
			}
			var status string
			var attempts int
			if e := p.QueryRow(ctx, "SELECT status,attempts FROM jobs WHERE id=$1", image.Jobs[0].ID).Scan(&status, &attempts); e != nil || status != "completed" || attempts != 1 {
				t.Fatal(status, attempts, e)
			}
		})
	}
}
