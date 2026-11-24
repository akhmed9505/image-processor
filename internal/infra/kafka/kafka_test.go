package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/config"
)

func TestInitializationCancellation(t *testing.T) {
	cfg := config.Kafka{Brokers: []string{"127.0.0.1:1"}, JobsTopic: "jobs", DLQTopic: "dlq", ConsumerGroup: "workers", Timeout: 100 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if p, e := NewProducer(ctx, cfg); !errors.Is(e, context.Canceled) || p != nil {
		t.Fatalf("producer %v %v", p, e)
	}
	if c, e := NewConsumer(ctx, cfg); !errors.Is(e, context.Canceled) || c != nil {
		t.Fatalf("consumer %v %v", c, e)
	}
	if e := InitTopics(ctx, cfg); !errors.Is(e, context.Canceled) {
		t.Fatal("topic init lost context", e)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if p, e := NewProducer(ctx, cfg); e == nil || p != nil {
		t.Fatal("unreachable broker accepted")
	}
}
func TestPublishInput(t *testing.T) {
	p := &Producer{}
	if e := p.Publish(context.Background(), "", []byte("key"), nil); e == nil {
		t.Fatal("empty topic accepted")
	}
	if e := p.Publish(context.Background(), "jobs", nil, nil); e == nil {
		t.Fatal("empty key accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e := p.Publish(ctx, "jobs", []byte("key"), nil); !errors.Is(e, context.Canceled) {
		t.Fatal("lost context", e)
	}
}
