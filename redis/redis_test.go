package redis

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/goinbox/golog"
	"github.com/goinbox/mq"
	"github.com/goinbox/pcontext"
	"github.com/goinbox/redis"
)

const (
	streamKeyDemo = "lightglobal"
)

var ctx pcontext.Context
var client *redis.Client

func init() {
	w, _ := golog.NewFileWriter("/dev/stdout", 0)
	logger := golog.NewSimpleLogger(w, golog.NewSimpleFormater())
	ctx = pcontext.NewSimpleContext(logger)

	config := redis.NewConfig("127.0.0.1", "123", 6379)
	client = redis.NewClient(config)
}

type messageData struct {
	Name  string
	Value int64
}

type demoProcessor struct {
}

func (p *demoProcessor) Process(message *mq.Message) error {
	fmt.Println("process message", message.ID, string(message.Data))

	data := &messageData{}
	err := json.Unmarshal(message.Data, data)
	fmt.Println(err, data)

	time.Sleep(time.Second * 10)

	return nil
}

func (p *demoProcessor) Wait() error {
	fmt.Println("demo wait start")
	time.Sleep(time.Second * 3)
	fmt.Println("demo wait end")

	return nil
}

func TestProducer(t *testing.T) {
	producer := NewProducer(client, &ProducerConfig{
		StreamKey: streamKeyDemo,
		MaxLen:    10,
	})

	for i := 0; i < 10; i++ {
		msg := new(mq.Message)
		msg.Data, _ = json.Marshal(&messageData{
			Name:  fmt.Sprintf("test-message-%d", i),
			Value: time.Now().UnixNano(),
		})
		err := producer.Send(ctx, msg)

		t.Log("send", i, err, msg.ID)
	}
}

func TestConsumer(t *testing.T) {
	consumer := NewConsumer(client, &ConsumerConfig{
		StreamKey:    streamKeyDemo,
		GroupName:    "demo",
		ConsumerName: "test-consumer",
	})
	consumer.SetMessageProcessor(new(demoProcessor))

	go func() {
		time.Sleep(time.Second * 5)
		err := consumer.Stop(ctx)
		t.Log("consumer.Stop", err)
	}()

	err := consumer.Start(ctx)
	t.Log("consumer.Start", err)
}
