package redis

import (
	"github.com/goinbox/golog"
	"github.com/goinbox/redis"
)

type producer struct {
	client *redis.Client
	logger golog.Logger
}

func NewProducer(client *redis.Client, logger golog.Logger) *producer {

}

func
