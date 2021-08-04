package internal

import (
	redis "github.com/go-redis/redis/v7"
)

const (
	AutoIncrement    string = "*"
	NextStreamOffset string = ">"
	LastStreamOffset string = "$"

	MAX_PENDING_SEED_SIZE int64 = 64
)

type (
	Options  = redis.Options
	Client   = redis.Client
	XMessage = redis.XMessage
	XStream  = redis.XStream
)
