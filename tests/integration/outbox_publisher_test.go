//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/config"
	infrastructure "github.com/akhmed9505/image-processor/internal/infra/kafka"
	repository "github.com/akhmed9505/image-processor/internal/repository/postgres"
	service "github.com/akhmed9505/image-processor/internal/service/outbox"
	"github.com/jackc/pgx/v5/pgxpool"
)

type publishFunc func(context.Context, string, []byte, []byte) error

func (f publishFunc) Publish(ctx context.Context, topic string, key, payload []byte) error {
	return f(ctx, topic, key, payload)
}

type publisherRepository struct {
	*repository.OutboxRepository
	mark  func(context.Context, string, string) error
	claim func(context.Context, int) ([]service.Event, error)
}

func (r publisherRepository) MarkPublished(ctx context.Context, id, token string) error {
	if r.mark != nil {
		return r.mark(ctx, id, token)
	}
	return r.OutboxRepository.MarkPublished(ctx, id, token)
}
func (r publisherRepository) Claim(ctx context.Context, n int) ([]service.Event, error) {
	if r.claim != nil {
		return r.claim(ctx, n)
	}
	return r.OutboxRepository.Claim(ctx, n)
}
func runPublisher(t *testing.T, repo service.Repository, producer service.Producer) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.New(repo, producer, slog.New(slog.NewJSONHandler(io.Discard, nil))).Run(ctx) }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case e := <-done:
			if e != nil {
				t.Error(e)
			}
		case <-time.After(5 * time.Second):
			t.Error("publisher did not stop")
		}
	}
	t.Cleanup(stop)
	return stop
}
func setupPublisherKafka(t *testing.T) (config.Kafka, *infrastructure.Producer) {
	t.Helper()
	cfg := kafkaConfig()
	kafkaAdmin(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if e := infrastructure.InitTopics(ctx, cfg); e != nil {
		t.Fatal(e)
	}
	producer, e := infrastructure.NewProducer(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(producer.Close)
	return cfg, producer
}
func seedPublisher(t *testing.T, p *pgxpool.Pool, topic string, n int) {
	t.Helper()
	seedOutbox(t, p, n)
	if _, e := p.Exec(context.Background(), "UPDATE outbox SET topic=$1", topic); e != nil {
		t.Fatal(e)
	}
}
func waitPublished(t *testing.T, p *pgxpool.Pool, n int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	awaitOutboxState(t, ctx, func() (bool, error) {
		var count int
		e := p.QueryRow(ctx, "SELECT count(*) FROM outbox WHERE published_at IS NOT NULL AND claim_token IS NULL AND lease_until IS NULL").Scan(&count)
		return count == n, e
	})
}
func verifyKafkaRecords(t *testing.T, cfg config.Kafka, n int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	consumer, e := infrastructure.NewConsumer(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer consumer.CloseAllowingRebalance()
	for i := 0; i < n; i++ {
		record := pollKafkaRecord(t, ctx, consumer)
		if record.Topic != cfg.JobsTopic || string(record.Key) != "test.key" || !bytes.Equal(record.Value, []byte(`{"version": 1}`)) {
			t.Fatalf("wire event changed: topic=%s key=%s value=%s", record.Topic, record.Key, record.Value)
		}
		consumer.AllowRebalance()
	}
}
func TestOutboxPublisherConcurrent(t *testing.T) {
	p := migratedReadPool(t)
	cfg, producer := setupPublisherKafka(t)
	seedPublisher(t, p, cfg.JobsTopic, 8)
	r, e := repository.NewOutboxRepository(p, service.ClaimLease)
	if e != nil {
		t.Fatal(e)
	}
	first := runPublisher(t, r, producer)
	second := runPublisher(t, r, producer)
	waitPublished(t, p, 8)
	first()
	second()
	var attempts int
	if e := p.QueryRow(context.Background(), "SELECT sum(publish_attempts) FROM outbox").Scan(&attempts); e != nil || attempts != 8 {
		t.Fatal("concurrent claim overlap", attempts, e)
	}
	verifyKafkaRecords(t, cfg, 8)
}
func TestOutboxPublisherFailureAndRedelivery(t *testing.T) {
	for _, kind := range []string{"publish failure", "unmarked acknowledgement", "fenced", "expired lease"} {
		t.Run(kind, func(t *testing.T) {
			p := migratedReadPool(t)
			cfg, producer := setupPublisherKafka(t)
			seedPublisher(t, p, cfg.JobsTopic, 1)
			repo := outboxRepo(t, p)
			reached := make(chan struct{})
			cause := errors.New("injected publication failure")
			wrapped := publisherRepository{OutboxRepository: repo}
			var transport service.Producer = producer
			if kind == "publish failure" {
				transport = publishFunc(func(context.Context, string, []byte, []byte) error { return cause })
			} else {
				wrapped.mark = func(ctx context.Context, id, token string) error {
					defer close(reached)
					switch kind {
					case "unmarked acknowledgement":
						return cause
					case "fenced":
						_, e := p.Exec(ctx, "UPDATE outbox SET claim_token=NULL,lease_until=NULL,available_at=clock_timestamp()+interval '1 hour'")
						if e != nil {
							return e
						}
					case "expired lease":
						_, e := p.Exec(ctx, "UPDATE outbox SET lease_until=clock_timestamp()-interval '1 second'")
						if e != nil {
							return e
						}
					}
					return repo.MarkPublished(ctx, id, token)
				}
			}
			stop := runPublisher(t, wrapped, transport)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if kind == "publish failure" {
				awaitOutboxState(t, ctx, func() (bool, error) {
					var ready bool
					e := p.QueryRow(ctx, "SELECT last_error_code='publish_failed' AND claim_token IS NULL AND lease_until IS NULL AND available_at>clock_timestamp() AND published_at IS NULL FROM outbox").Scan(&ready)
					return ready, e
				})
			} else {
				select {
				case <-reached:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			stop()
			var published bool
			if e := p.QueryRow(ctx, "SELECT published_at IS NOT NULL FROM outbox").Scan(&published); e != nil || published {
				t.Fatal("unconfirmed event marked", e)
			}
			if _, e := p.Exec(ctx, "UPDATE outbox SET available_at=clock_timestamp()-interval '1 second',lease_until=CASE WHEN claim_token IS NOT NULL THEN clock_timestamp()-interval '1 second' END"); e != nil {
				t.Fatal(e)
			}
			stop = runPublisher(t, repo, producer)
			waitPublished(t, p, 1)
			stop()
			n := 2
			if kind == "publish failure" {
				n = 1
			}
			verifyKafkaRecords(t, cfg, n)
		})
	}
}

func TestOutboxPublisherUncertainCommit(t *testing.T) {
	for _, operation := range []string{"claim", "mark"} {
		t.Run(operation, func(t *testing.T) {
			p := migratedReadPool(t)
			cfg, producer := setupPublisherKafka(t)
			seedPublisher(t, p, cfg.JobsTopic, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			blocked, release := make(chan struct{}), make(chan struct{})
			poolCfg := p.Config()
			dial := poolCfg.ConnConfig.DialFunc
			poolCfg.ConnConfig.DialFunc = func(c context.Context, n, a string) (net.Conn, error) {
				conn, e := dial(c, n, a)
				if e != nil {
					return nil, e
				}
				return &uploadCommitResponseDrop{Conn: conn, ctx: ctx, blocked: blocked, release: release}, nil
			}
			client, e := pgxpool.NewWithConfig(ctx, poolCfg)
			if e != nil {
				t.Fatal(e)
			}
			defer func() { cancel(); client.Close() }()
			uncertain, e := repository.NewOutboxRepository(client, service.ClaimLease)
			if e != nil {
				t.Fatal(e)
			}
			repo := outboxRepo(t, p)
			wrapped := publisherRepository{OutboxRepository: repo}
			failed := make(chan error, 1)
			if operation == "claim" {
				wrapped.claim = func(c context.Context, n int) ([]service.Event, error) {
					events, e := uncertain.Claim(c, n)
					failed <- e
					return events, e
				}
			} else {
				wrapped.mark = func(c context.Context, id, token string) error {
					e := uncertain.MarkPublished(c, id, token)
					failed <- e
					return e
				}
			}
			stop := runPublisher(t, wrapped, producer)
			select {
			case <-blocked:
			case e := <-failed:
				t.Fatalf("COMMIT not reached: %v", e)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			awaitOutboxState(t, ctx, func() (bool, error) {
				var ready bool
				predicate := "claim_token IS NOT NULL"
				if operation == "mark" {
					predicate = "published_at IS NOT NULL"
				}
				e := p.QueryRow(ctx, "SELECT "+predicate+" FROM outbox").Scan(&ready)
				return ready, e
			})
			close(release)
			select {
			case e := <-failed:
				if !errors.Is(e, repository.ErrOutboxCommitUncertain) {
					t.Fatal("uncertainty lost", e)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			stop()
			if operation == "claim" {
				if _, e := p.Exec(ctx, "UPDATE outbox SET lease_until=clock_timestamp()-interval '1 second'"); e != nil {
					t.Fatal(e)
				}
			}
			stop = runPublisher(t, repo, producer)
			waitPublished(t, p, 1)
			stop()
			verifyKafkaRecords(t, cfg, 1)
		})
	}
}

func TestOutboxPublisherBrokerTimeout(t *testing.T) {
	p := migratedReadPool(t)
	cfg, producer := setupPublisherKafka(t)
	seedPublisher(t, p, cfg.JobsTopic+".missing", 1)
	repo := outboxRepo(t, p)
	stop := runPublisher(t, repo, producer)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	awaitOutboxState(t, ctx, func() (bool, error) {
		var ready bool
		e := p.QueryRow(ctx, `SELECT COALESCE(last_error_code='publish_timeout' AND claim_token IS NULL AND published_at IS NULL,false) FROM outbox`).Scan(&ready)
		return ready, e
	})
	stop()
	if _, e := p.Exec(ctx, "UPDATE outbox SET topic=$1,available_at=clock_timestamp()-interval '1 second'", cfg.JobsTopic); e != nil {
		t.Fatal(e)
	}
	stop = runPublisher(t, repo, producer)
	waitPublished(t, p, 1)
	stop()
	verifyKafkaRecords(t, cfg, 1)
}
