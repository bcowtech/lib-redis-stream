package internal

import (
	redis "github.com/go-redis/redis/v7"
)

const (
	AutoIncrement    string = "*"
	NextStreamOffset string = ">"
	LastStreamOffset string = "$"
)

type (
	Options  = redis.Options
	Client   = redis.Client
	XMessage = redis.XMessage
	XStream  = redis.XStream
)
