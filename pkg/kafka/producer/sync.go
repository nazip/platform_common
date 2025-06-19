package producer

import (
	"context"
	"github.com/IBM/sarama"
)

type SyncProducer struct {
	Producer sarama.SyncProducer
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

func (s *SyncProducer) Produce(_ context.Context, msg *sarama.ProducerMessage) (int32, int64, error) {
	partition, offset, err := s.Producer.SendMessage(msg)
	return partition, offset, err
}

func (s *SyncProducer) Close() error {
	return s.Producer.Close()
}
