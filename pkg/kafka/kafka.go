package kafka

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/nazip/platform_common/pkg/kafka/consumer"
)

type Consumer interface {
	Consume(ctx context.Context, topicName string, handler consumer.Handler) (err error)
	Close() error
}

type Producer interface {
	Produce(ctx context.Context, msg *sarama.ProducerMessage) (int32, int64, error)
	Close() error
}
