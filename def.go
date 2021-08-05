package redis

import (
	"log"
	"os"

	"github.com/bcowtech/lib-redis-stream/internal"
	redis "github.com/go-redis/redis/v7"
)

const (
	AutoIncrement    string = internal.AutoIncrement
	NextStreamOffset string = internal.NextStreamOffset
	LastStreamOffset string = internal.LastStreamOffset

	Nil = redis.Nil

	LOGGER_PREFIX string = "[bcowtech/lib-redis-stream] "
)

var (
	logger *log.Logger = log.New(os.Stdout, LOGGER_PREFIX, log.LstdFlags|log.Lmsgprefix)
)

type (
	Options  = redis.Options
	Client   = redis.Client
	XMessage = redis.XMessage
	XStream  = redis.XStream

	StreamOffset = internal.StreamOffset
)

// func
type (
	RedisErrorHandleProc func(err error) (disposed bool)
	MessageHandleProc    func(ctx *ConsumeContext, stream string, message *XMessage)
)
