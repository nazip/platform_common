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
}

func NewSyncProducer(brokerList []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

func NewAsyncProducer(brokerList []string) (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		return nil, err
	}

	return producer, nil
}
