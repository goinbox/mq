package mq

import (
	"github.com/goinbox/pcontext"
)

type Message struct {
	ID   string
	Data []byte
}

type Producer interface {
	Send(ctx pcontext.Context, message *Message) error
}

type MessageProcessor interface {
	Process(message *Message) error
	Wait() error
}

type Consumer interface {
	Start(ctx pcontext.Context) error
	Stop(ctx pcontext.Context) error
	SetMessageProcessor(processor MessageProcessor)
}
