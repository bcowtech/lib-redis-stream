package internal

import (
	redis "github.com/go-redis/redis/v7"
)

const (
	StreamAsteriskID           string = "*"
	StreamLastDeliveredID      string = "$"
	StreamZeroID               string = "0"
	StreamZeroOffset           string = "0"
	StreamNeverDeliveredOffset string = ">"

	MAX_PENDING_FETCHING_SIZE int64 = 512
	MIN_PENDING_FETCHING_SIZE int64 = 16
)

type (
	UniversalOptions = redis.UniversalOptions
	UniversalClient  = redis.UniversalClient
	XMessage         = redis.XMessage
	XStream          = redis.XStream
)
