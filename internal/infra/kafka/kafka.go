// Package kafka initializes plaintext Kafka clients for the local environment.
package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/akhmed9505/image-processor/internal/config"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func connect(ctx context.Context, cfg config.Kafka, options ...kgo.Opt) (*kgo.Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	opts := []kgo.Opt{kgo.SeedBrokers(cfg.Brokers...), kgo.DialTimeout(cfg.Timeout), kgo.RequestTimeoutOverhead(cfg.Timeout)}
	client, err := kgo.NewClient(append(opts, options...)...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka client: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	if err := client.Ping(pingCtx); err != nil {
		client.CloseAllowingRebalance()
		return nil, fmt.Errorf("ping Kafka: %w", err)
	}
	return client, nil
}

// InitTopics creates missing topics and rejects incompatible existing settings.
func InitTopics(ctx context.Context, cfg config.Kafka) error {
	client, err := connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	admin := kadm.NewClient(client)
	for _, spec := range []struct {
		name       string
		partitions int32
		retention  time.Duration
	}{{cfg.JobsTopic, 6, 7 * 24 * time.Hour}, {cfg.DLQTopic, 1, 14 * 24 * time.Hour}} {
		retention := strconv.FormatInt(spec.retention.Milliseconds(), 10)
		created, err := admin.CreateTopics(ctx, spec.partitions, 1, map[string]*string{"retention.ms": &retention, "cleanup.policy": kadm.StringPtr("delete")}, spec.name)
		if err != nil {
			return fmt.Errorf("create Kafka topic: %w", err)
		}
		response, ok := created[spec.name]
		if !ok {
			return errors.New("missing Kafka create topic response")
		}
		if response.Err != nil && !errors.Is(response.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", spec.name, response.Err)
		}
		topic, err := waitTopic(ctx, admin, spec.name)
		if err != nil {
			return err
		}
		if len(topic.Partitions) != int(spec.partitions) {
			return fmt.Errorf("topic %s must have %d partitions", spec.name, spec.partitions)
		}
		configs, err := admin.DescribeTopicConfigs(ctx, spec.name)
		if err != nil {
			return fmt.Errorf("describe topic settings: %w", err)
		}
		settings, err := configs.On(spec.name, nil)
		if err != nil {
			return fmt.Errorf("find topic settings: %w", err)
		}
		if settings.Err != nil {
			return fmt.Errorf("describe topic settings: %w", settings.Err)
		}
		values := map[string]string{}
		for _, c := range settings.Configs {
			values[c.Key] = c.MaybeValue()
		}
		if values["retention.ms"] != retention || values["cleanup.policy"] != "delete" {
			return fmt.Errorf("topic %s has incompatible retention or cleanup policy", spec.name)
		}
	}
	return nil
}

// Creation can be acknowledged before metadata and partition leaders are visible.
func waitTopic(ctx context.Context, admin *kadm.Client, name string) (kadm.TopicDetail, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		details, err := admin.ListTopics(ctx, name)
		if err != nil {
			return kadm.TopicDetail{}, fmt.Errorf("describe Kafka topic: %w", err)
		}
		topic, ok := details[name]
		if !ok {
			return kadm.TopicDetail{}, errors.New("missing Kafka topic metadata")
		}
		ready := topic.Err == nil && len(topic.Partitions) > 0
		if topic.Err != nil && !errors.Is(topic.Err, kerr.UnknownTopicOrPartition) && !errors.Is(topic.Err, kerr.LeaderNotAvailable) {
			return kadm.TopicDetail{}, fmt.Errorf("describe topic %s: %w", name, topic.Err)
		}
		for _, partition := range topic.Partitions {
			if partition.Err != nil && !errors.Is(partition.Err, kerr.LeaderNotAvailable) && !errors.Is(partition.Err, kerr.UnknownTopicOrPartition) {
				return kadm.TopicDetail{}, fmt.Errorf("describe partition: %w", partition.Err)
			}
			if partition.Err != nil || partition.Leader < 0 {
				ready = false
			}
		}
		if ready {
			return topic, nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return kadm.TopicDetail{}, fmt.Errorf("wait for topic %s: %w", name, ctx.Err())
		}
	}
}

// Producer publishes with broker acknowledgement and franz-go's default idempotency.
type Producer struct{ client *kgo.Client }

// NewProducer checks connectivity; the caller must close the producer.
func NewProducer(ctx context.Context, cfg config.Kafka) (*Producer, error) {
	client, err := connect(ctx, cfg, kgo.RequiredAcks(kgo.AllISRAcks()), kgo.AllowIdempotentProduceCancellation())
	if err != nil {
		return nil, err
	}
	return &Producer{client}, nil
}

// Publish waits for broker acknowledgement or context cancellation.
// Errors may leave delivery uncertain; idempotency covers only protocol retries.
func (p *Producer) Publish(ctx context.Context, topic string, key, value []byte) error {
	if topic == "" || len(key) == 0 {
		return errors.New("Kafka topic and message key are required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	done := make(chan error, 1)
	// The SDK may retain the record after a cancelled Publish returns.
	p.client.Produce(ctx, &kgo.Record{Topic: topic, Key: bytes.Clone(key), Value: bytes.Clone(value)}, func(_ *kgo.Record, err error) { done <- err })
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("publish Kafka record: %w", err)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("publish Kafka record: %w", ctx.Err())
	}
}

// Close stops the producer and releases its connections.
func (p *Producer) Close() { p.client.Close() }

// NewConsumer disables auto-commit; commit only contiguous, durably handled offsets.
// Call AllowRebalance after handling each poll; close with CloseAllowingRebalance.
func NewConsumer(ctx context.Context, cfg config.Kafka) (*kgo.Client, error) {
	return connect(ctx, cfg, kgo.ConsumerGroup(cfg.ConsumerGroup), kgo.ConsumeTopics(cfg.JobsTopic), kgo.DisableAutoCommit(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.BlockRebalanceOnPoll())
}
