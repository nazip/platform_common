package producer

import (
	"context"
	"github.com/IBM/sarama"
)

type AsyncProducer struct {
	Producer sarama.AsyncProducer
}

func NewAsyncProducer(brokerList []string) (*AsyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	prd, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		return nil, err
	}

	return &AsyncProducer{
		Producer: prd,
	}, nil
}

func (s *AsyncProducer) Produce(ctx context.Context, msg *sarama.ProducerMessage) (int32, int64, error) {
	s.Producer.Input() <- msg

	select {
	case <-ctx.Done():
		return -1, -1, ctx.Err()
	case success := <-s.Producer.Successes():
		return success.Partition, success.Offset, nil
	case err := <-s.Producer.Errors():
		return 0, 0, err
	}
}

func (s *AsyncProducer) Close() error {
	return s.Producer.Close()
}
