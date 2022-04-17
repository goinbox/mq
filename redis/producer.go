package redis

import (
	"fmt"

	"github.com/goinbox/mq"
	"github.com/goinbox/pcontext"
	"github.com/goinbox/redis"
)

type ProducerConfig struct {
	StreamKey string
	MaxLen    int
}

type producer struct {
	client *redis.Client
	config *ProducerConfig
}

func NewProducer(client *redis.Client, config *ProducerConfig) mq.Producer {
	return &producer{
		client: client,
		config: config,
	}
}

func (p *producer) Send(ctx pcontext.Context, message *mq.Message) error {
	mid := "*"
	if message.ID != "" {
		mid = message.ID
	}

	var err error
	r := p.client.Do(ctx, "XADD", p.config.StreamKey, "MAXLEN", p.config.MaxLen, mid, "data", message.Data)
	if r.Err != nil {
		return fmt.Errorf("client.Do error: %w", r.Err)
	}
	mid, err = r.String()
	if err != nil {
		return fmt.Errorf("reply.String error: %w", err)
	}

	if message.ID == "" {
		message.ID = mid
	}

	return nil
}
