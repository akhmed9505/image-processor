package app

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/akhmed9505/image-processor/internal/config"
	httpDelivery "github.com/akhmed9505/image-processor/internal/delivery/http"
	kafkaDelivery "github.com/akhmed9505/image-processor/internal/delivery/kafka"
	"github.com/akhmed9505/image-processor/internal/infra/kafka"
	"github.com/akhmed9505/image-processor/internal/infra/postgres"
	"github.com/akhmed9505/image-processor/internal/infra/storage"
	"github.com/akhmed9505/image-processor/internal/logger"
	repository "github.com/akhmed9505/image-processor/internal/repository/postgres"
	imageService "github.com/akhmed9505/image-processor/internal/service/image"
	"github.com/akhmed9505/image-processor/internal/service/outbox"
	"github.com/akhmed9505/image-processor/internal/service/processor"
)

// RunServer runs HTTP, outbox publication, and cleanup until cancellation or failure.
// ErrShutdownIncomplete leaves dependencies open; the entry point must terminate the process.
func RunServer(ctx context.Context, cfg config.Config, output io.Writer) error {
	return run(ctx, cfg, output, false)
}

// RunWorker runs admission, processing, and recovery with the same shutdown contract.
func RunWorker(ctx context.Context, cfg config.Config, output io.Writer) error {
	return run(ctx, cfg, output, true)
}

func run(ctx context.Context, cfg config.Config, output io.Writer, worker bool) (retErr error) {
	if err := cfg.Validate(); err != nil {
		return stage("configuration", err)
	}
	log, err := logger.New(cfg.Log, output)
	if err != nil {
		return err
	}
	startup, cancel := context.WithTimeout(ctx, cfg.StartupTimeout)
	defer cancel()
	var resources closeStack
	safe := true
	defer func() {
		if safe {
			retErr = errors.Join(retErr, resources.closeBounded())
		} else {
			log.Error("Dependencies left open for process termination")
		}
	}()
	pool, err := postgres.New(startup, cfg.Postgres)
	if err != nil {
		return stage("PostgreSQL initialization", err)
	}
	resources = append(resources, pool.Close)
	checkCtx, stopCheck := context.WithTimeout(startup, cfg.Postgres.Timeout)
	err = checkSchema(checkCtx, pool, cfg.MigrationsDir)
	stopCheck()
	if err != nil {
		return stage("schema version; run server -migrate", err)
	}
	store, err := storage.New(startup, cfg.MinIO)
	if err != nil {
		return stage("MinIO initialization", err)
	}
	resources = append(resources, store.Close)
	if err := kafka.InitTopics(startup, cfg.Kafka); err != nil {
		return stage("Kafka topic initialization", err)
	}
	var components []func(context.Context) error
	if worker {
		client, err := kafka.NewConsumer(startup, cfg.Kafka)
		if err != nil {
			return err
		}
		resources = append(resources, client.CloseAllowingRebalance)
		jobs, err := repository.NewJobRepository(pool, repository.JobRepositoryOptions{JobsTopic: cfg.Kafka.JobsTopic, DLQTopic: cfg.Kafka.DLQTopic, MaxAttempts: processor.MaxAttempts, LeaseDuration: processor.AttemptLease, RedispatchInterval: processor.RedispatchInterval})
		if err != nil {
			return err
		}
		consumer := kafkaDelivery.New(client, processor.NewJobService(jobs, store), log)
		recovery := processor.NewRecoveryService(repository.NewRecoveryRepository(jobs), log)
		components = []func(context.Context) error{consumer.Run, recovery.Run}
	} else {
		producer, err := kafka.NewProducer(startup, cfg.Kafka)
		if err != nil {
			return err
		}
		resources = append(resources, producer.Close)
		uploadRepo, err := repository.NewUploadRepository(pool, cfg.Kafka.JobsTopic, processor.RedispatchInterval)
		if err != nil {
			return err
		}
		upload, err := imageService.NewUploadService(uploadRepo, repository.NewStorageRepository(pool), store, cfg.ValidationConcurrency)
		if err != nil {
			return err
		}
		read := imageService.NewReadDeleteService(repository.NewImageRepository(pool), repository.NewDeleteRepository(pool), store)
		outboxRepo, err := repository.NewOutboxRepository(pool, outbox.ClaimLease)
		if err != nil {
			return err
		}
		cleanupRepo, err := repository.NewCleanupRepository(pool, imageService.CleanupLease)
		if err != nil {
			return err
		}
		publisher := outbox.New(outboxRepo, producer, log)
		cleanup := imageService.NewCleanupService(cleanupRepo, repository.NewStorageRepository(pool), store, log)
		listener, err := net.Listen("tcp", cfg.HTTP.Address)
		if err != nil {
			return stage("HTTP listener", err)
		}
		resources = append(resources, func() { _ = listener.Close() })
		router := httpDelivery.NewRouter(upload, read, log)
		components = []func(context.Context) error{func(c context.Context) error { return serve(c, listener, cfg.HTTP, router, log) }, publisher.Run, cleanup.Run}
	}
	if err := startup.Err(); err != nil {
		return err
	}
	cancel()
	log.Info("Application started", "worker", worker)
	safe, err = supervise(ctx, components...)
	return err
}
