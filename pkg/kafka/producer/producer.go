package producer

import (
	"context"
	"github.com/IBM/sarama"
)

type SyncProducer struct {
	producer sarama.SyncProducer
}

func (s *SyncProducer) Produce(_ context.Context, msg *sarama.ProducerMessage) (int32, int64, error) {
	partition, offset, err := s.producer.SendMessage(msg)
	return partition, offset, err
}

type AsyncProducer struct {
	producer sarama.AsyncProducer
}

func (s *AsyncProducer) Produce(ctx context.Context, msg *sarama.ProducerMessage) (int32, int64, error) {
	s.producer.Input() <- msg

	select {
	case <-ctx.Done():
		return -1, -1, ctx.Err()
	case success := <-s.producer.Successes():
		return success.Partition, success.Offset, nil
	case err := <-s.producer.Errors():
		return 0, 0, err
	}
}
