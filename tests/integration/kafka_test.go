//go:build integration

package integration

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/config"
	infrastructure "github.com/akhmed9505/image-processor/internal/infra/kafka"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func kafkaConfig() config.Kafka {
	port := os.Getenv("TEST_KAFKA_PORT")
	if port == "" {
		port = "59092"
	}
	suffix := uuid.NewString()
	return config.Kafka{Brokers: []string{net.JoinHostPort("127.0.0.1", port)}, JobsTopic: "test.jobs." + suffix, DLQTopic: "test.dlq." + suffix, ConsumerGroup: "test.workers." + suffix, Timeout: 10 * time.Second}
}
func kafkaAdmin(t *testing.T, cfg config.Kafka) *kadm.Client {
	t.Helper()
	client, e := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(client.Close)
	admin := kadm.NewClient(client)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, e := admin.DeleteTopics(ctx, cfg.JobsTopic, cfg.DLQTopic); e != nil {
			t.Error(e)
		}
	})
	return admin
}
func TestKafkaTopicInitialization(t *testing.T) {
	cfg := kafkaConfig()
	admin := kafkaAdmin(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	done := make(chan error, 4)
	for i := 0; i < 4; i++ {
		go func() { done <- infrastructure.InitTopics(ctx, cfg) }()
	}
	for i := 0; i < 4; i++ {
		if e := <-done; e != nil {
			t.Fatal(e)
		}
	}
	details, e := admin.ListTopics(ctx, cfg.JobsTopic, cfg.DLQTopic)
	if e != nil {
		t.Fatal(e)
	}
	if len(details[cfg.JobsTopic].Partitions) != 6 || len(details[cfg.DLQTopic].Partitions) != 1 {
		t.Fatal("incorrect partitions")
	}
	settings, e := admin.DescribeTopicConfigs(ctx, cfg.JobsTopic, cfg.DLQTopic)
	if e != nil {
		t.Fatal(e)
	}
	for _, s := range settings {
		want := "604800000"
		if s.Name == cfg.DLQTopic {
			want = "1209600000"
		}
		found := false
		for _, c := range s.Configs {
			if c.Key == "retention.ms" {
				found = true
				if c.MaybeValue() != want {
					t.Fatal("incorrect retention")
				}
			}
		}
		if s.Err != nil || !found {
			t.Fatal("missing topic settings", s.Err)
		}
	}
}
func TestKafkaIncompatibleTopic(t *testing.T) {
	for _, wrongPartitions := range []bool{false, true} {
		name := "retention"
		if wrongPartitions {
			name = "partitions"
		}
		t.Run(name, func(t *testing.T) {
			cfg := kafkaConfig()
			admin := kafkaAdmin(t, cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			count := int32(6)
			retention := "1"
			if wrongPartitions {
				count = 1
				retention = "604800000"
			}
			result, e := admin.CreateTopics(ctx, count, 1, map[string]*string{"retention.ms": &retention}, cfg.JobsTopic)
			if e != nil || result[cfg.JobsTopic].Err != nil {
				t.Fatal("seed topic", e)
			}
			if e := infrastructure.InitTopics(ctx, cfg); e == nil {
				t.Fatal("incompatible topic accepted")
			}
			details, e := admin.ListTopics(ctx, cfg.JobsTopic)
			if e != nil || len(details[cfg.JobsTopic].Partitions) != int(count) {
				t.Fatal("existing topic altered", e)
			}
		})
	}
}
func pollKafkaRecord(t *testing.T, ctx context.Context, client *kgo.Client) *kgo.Record {
	t.Helper()
	fetches := client.PollRecords(ctx, 1)
	if errs := fetches.Errors(); len(errs) != 0 {
		t.Fatal(errs)
	}
	var record *kgo.Record
	fetches.EachRecord(func(r *kgo.Record) { record = r })
	if record == nil {
		t.Fatal("no record fetched")
	}
	return record
}
func TestKafkaPublishAndManualOffsets(t *testing.T) {
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
	for _, value := range []string{"first", "second"} {
		if e := producer.Publish(ctx, cfg.JobsTopic, []byte("job-id"), []byte(value)); e != nil {
			t.Fatal(e)
		}
	}
	consumer, e := infrastructure.NewConsumer(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer consumer.CloseAllowingRebalance()
	if consumer.OptValue(kgo.DisableAutoCommit) != true {
		t.Fatal("auto-commit enabled")
	}
	first := pollKafkaRecord(t, ctx, consumer)
	if first.ProducerID < 0 || first.ProducerEpoch < 0 {
		t.Fatal("record was not produced idempotently")
	}
	if string(first.Key) != "job-id" || string(first.Value) != "first" {
		t.Fatal("payload or key lost")
	}
	// Closing must not commit even after records were fetched.
	consumer.CloseAllowingRebalance()
	offsets, e := admin.FetchOffsets(ctx, cfg.ConsumerGroup)
	if e != nil {
		t.Fatal(e)
	}
	for _, partitions := range offsets {
		for _, offset := range partitions {
			if offset.At >= 0 {
				t.Fatal("offset auto-committed")
			}
		}
	}
	consumer, e = infrastructure.NewConsumer(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer consumer.CloseAllowingRebalance()
	replay := pollKafkaRecord(t, ctx, consumer)
	if replay.Offset != first.Offset || replay.Partition != first.Partition {
		t.Fatal("uncommitted record not replayed")
	}
	if e := consumer.CommitRecords(ctx, replay); e != nil {
		t.Fatal(e)
	}
	consumer.AllowRebalance()
	second := pollKafkaRecord(t, ctx, consumer)
	if string(second.Value) != "second" || second.Partition != first.Partition || second.Offset != first.Offset+1 {
		t.Fatal("key affinity or ordering lost")
	}
	if e := consumer.CommitRecords(ctx, second); e != nil {
		t.Fatal(e)
	}
	consumer.AllowRebalance()
	offsets, e = admin.FetchOffsets(ctx, cfg.ConsumerGroup)
	if e != nil {
		t.Fatal(e)
	}
	if offsets[cfg.JobsTopic][second.Partition].At != second.Offset+1 {
		t.Fatal("explicit commit not stored")
	}
	cancelled, stop := context.WithCancel(ctx)
	stop()
	if e := producer.Publish(cancelled, cfg.JobsTopic, []byte("job-id"), nil); !errors.Is(e, context.Canceled) {
		t.Fatal("publish cancellation", e)
	}
}
