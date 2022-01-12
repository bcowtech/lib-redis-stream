package redis

import (
	"log"
	"os"

	"github.com/bcowtech/lib-redis-stream/internal"
	redis "github.com/go-redis/redis/v7"
)

const (
	StreamAsteriskID           string = internal.StreamAsteriskID
	StreamLastDeliveredID      string = internal.StreamLastDeliveredID
	StreamZeroID               string = internal.StreamZeroID
	StreamZeroOffset           string = internal.StreamZeroOffset
	StreamNeverDeliveredOffset string = internal.StreamNeverDeliveredOffset

	Nil = redis.Nil

	LOGGER_PREFIX string = "[bcowtech/lib-redis-stream] "

	MAX_PENDING_FETCHING_SIZE         int64 = 512
	MIN_PENDING_FETCHING_SIZE         int64 = 16
	PENDING_FETCHING_SIZE_COEFFICIENT int64 = 3
)

var (
	logger *log.Logger = log.New(os.Stdout, LOGGER_PREFIX, log.LstdFlags|log.Lmsgprefix)
)

type (
	UniversalOptions = redis.UniversalOptions
	UniversalClient  = redis.UniversalClient
	XMessage         = redis.XMessage
	XStream          = redis.XStream

	StreamOffset = internal.StreamOffset
)

// func
type (
	RedisErrorHandleProc func(err error) (disposed bool)
	MessageHandleProc    func(ctx *ConsumeContext, stream string, message *XMessage)
)
