package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/akhmed9505/image-processor/internal/config"
	"github.com/akhmed9505/image-processor/internal/dto"
	"github.com/wb-go/wbf/kafka"
	"github.com/wb-go/wbf/retry"
	"github.com/wb-go/wbf/zlog"
)

type Kafka struct {
	producer *kafka.Producer
	consumer *kafka.Consumer
}

func New() *Kafka {
	address := config.Cfg.Kafka.Host + config.Cfg.Kafka.Port

	producer := kafka.NewProducer([]string{address}, "images")
	consumer := kafka.NewConsumer([]string{address}, "images", "img")

	return &Kafka{
		producer: producer,
		consumer: consumer,
	}
}

func (k *Kafka) ProduceMessage(message dto.Message) error {
	strategy := retry.Strategy{
		Attempts: 3,
		Delay:    time.Second,
		Backoff:  1,
	}

	payload, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("could not marshal kafka message payload to produce: %w", err)
	}

	if err := k.producer.SendWithRetry(context.Background(), strategy, nil, payload); err != nil {
		return fmt.Errorf("could not send message to kafka: %w", err)
	}

	zlog.Logger.Info().Msg("successfully produced message to kafka with id: " + message.ID.String())
	return nil
}

func (k *Kafka) ConsumeMessage() (*dto.Message, error) {
	strategy := retry.Strategy{
		Attempts: 3,
		Delay:    time.Second,
		Backoff:  1,
	}

	kafkaMessage, err := k.consumer.FetchWithRetry(context.Background(), strategy)
	if err != nil {
		return nil, fmt.Errorf("could not consume message from kafka: %w", err)
	}

	var message dto.Message
	if err := json.Unmarshal(kafkaMessage.Value, &message); err != nil {
		return nil, fmt.Errorf("could not unmarshal kafka message payload: %w", err)
	}

	if err := k.consumer.Commit(context.Background(), kafkaMessage); err != nil {
		return nil, fmt.Errorf("could not commit kafka message: %w", err)
	}

	zlog.Logger.Info().Msg("successfully consumed message from kafka with id: " + message.ID.String())
	return &message, nil
}
