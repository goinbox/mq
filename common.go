package mq

type Message struct {
	ID   string
	Data interface{}
}

type MessageProcessFunc func(*Message) error

type Producer interface {
	Send(message *Message) (messageID string, err error)
}

type Consumer interface {
	Start() error
	Stop() error
}
