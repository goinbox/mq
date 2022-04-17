package redis

import (
	"fmt"

	"github.com/goinbox/golog"
	"github.com/goinbox/mq"
	"github.com/goinbox/pcontext"
	"github.com/goinbox/redis"
)

type ConsumerConfig struct {
	StreamKey    string
	GroupName    string
	ConsumerName string
}

type consumer struct {
	client *redis.Client
	config *ConsumerConfig

	processor mq.MessageProcessor

	waitExit bool
	waitCh   chan struct{}
}

func NewConsumer(client *redis.Client, config *ConsumerConfig) mq.Consumer {
	return &consumer{
		client: client,
		config: config,

		waitCh: make(chan struct{}),
	}
}

func (c *consumer) SetMessageProcessor(processor mq.MessageProcessor) {
	c.processor = processor
}

func (c *consumer) Start(ctx pcontext.Context) error {
	_ = c.createConsumerGroup(ctx)

	logger := ctx.Logger()
	logger.Notice("run consumeLoopRoutine")
	go func() {
		c.consumeLoopRoutine(ctx)
	}()

	<-c.waitCh
	logger.Notice("consumer exit")

	return nil
}

func (c *consumer) Stop(ctx pcontext.Context) error {
	c.waitExit = true
	_ = c.client.Close(ctx)

	return nil
}

func (c *consumer) createConsumerGroup(ctx pcontext.Context) error {
	c.client.Do(ctx, "XGROUP", "CREATE", c.config.StreamKey, c.config.GroupName, 0, "MKSTREAM")

	return nil
}

func (c *consumer) consumeLoopRoutine(ctx pcontext.Context) {
	logger := ctx.Logger()

	for {
		message, err := c.readMessage(ctx)
		if err != nil {
			logger.Error("readMessage error", golog.ErrorField(err))
			if c.waitExit {
				logger.Notice("wait all messages process done")
				_ = c.processor.Wait()
				close(c.waitCh)
				return
			}
		} else {
			c.processMessage(ctx, message)
		}
	}
}

func (c *consumer) readMessage(ctx pcontext.Context) (*mq.Message, error) {
	r := c.client.Do(ctx, "XREADGROUP", "BLOCK", 0, "GROUP",
		c.config.GroupName, c.config.ConsumerName, "COUNT", 1, "STREAMS", c.config.StreamKey, ">")
	if r.Err != nil {
		return nil, fmt.Errorf("client.Do error: %w", r.Err)
	}

	message, err := c.parseMessage(r)
	if err != nil {
		return nil, fmt.Errorf("parseMessage error: %w", err)
	}

	return message, nil
}

func (c *consumer) parseMessage(reply *redis.Reply) (message *mq.Message, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("parseMessage error: %v", e)
		}
	}()

	value := reply.Value().(map[interface{}]interface{})
	data := value[c.config.StreamKey].([]interface{})[0].([]interface{})

	message = &mq.Message{
		ID:   data[0].(string),
		Data: []byte(data[1].([]interface{})[1].(string)),
	}

	return message, nil
}

func (c *consumer) processMessage(ctx pcontext.Context, message *mq.Message) {
	logger := ctx.Logger()
	logger.Notice("process message", []*golog.Field{
		{
			Key:   "MessageID",
			Value: message.ID,
		},
		{
			Key:   "Data",
			Value: string(message.Data),
		},
	}...)
	err := c.processor.Process(message)
	if err != nil {
		logger.Error("process message error", golog.ErrorField(err), &golog.Field{
			Key:   "MessageID",
			Value: message.ID,
		})
	} else {
		_ = c.ack(ctx, message)
	}
}

func (c *consumer) ack(ctx pcontext.Context, message *mq.Message) error {
	logger := ctx.Logger()
	logger.Notice("ack message", []*golog.Field{
		{
			Key:   "MessageID",
			Value: message.ID,
		},
		{
			Key:   "Data",
			Value: string(message.Data),
		},
	}...)

	r := c.client.Do(ctx, "XACK", c.config.StreamKey, c.config.GroupName, message.ID)
	if r.Err != nil {
		return fmt.Errorf("client.Do error: %w", r.Err)
	}

	v, _ := r.Int()
	logger.Notice("ack message return", []*golog.Field{
		{
			Key:   "MessageID",
			Value: message.ID,
		},
		{
			Key:   "Return",
			Value: v,
		},
	}...)

	return nil
}
