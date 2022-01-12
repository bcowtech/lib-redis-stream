package redis

import (
	"github.com/bcowtech/lib-redis-stream/internal"
	redis "github.com/go-redis/redis/v7"
)

type AdminClient struct {
	handle redis.UniversalClient
}

func NewAdminClient(opt *UniversalOptions) (*AdminClient, error) {
	client, err := internal.CreateRedisUniversalClient(opt)
	if err != nil {
		return nil, err
	}

	instance := &AdminClient{
		handle: client,
	}
	return instance, nil
}

func (c *AdminClient) Handle() redis.UniversalClient {
	return c.handle
}

func (c *AdminClient) Close() error {
	return c.handle.Close()
}

func (c *AdminClient) CreateConsumerGroup(stream, group, offset string) (string, error) {
	return c.handle.XGroupCreate(stream, group, offset).Result()
}

func (c *AdminClient) CreateConsumerGroupAndStream(stream, group, offset string) (string, error) {
	return c.handle.XGroupCreateMkStream(stream, group, offset).Result()
}

func (c *AdminClient) DeleteConsumerGroup(stream, group string) (int64, error) {
	return c.handle.XGroupDestroy(stream, group).Result()
}

func (c *AdminClient) SetConsumerGroupOffset(stream, group, offset string) (string, error) {
	return c.handle.XGroupSetID(stream, group, offset).Result()
}

func (c *AdminClient) DeleteConsumer(stream, group, consumer string) (int64, error) {
	return c.handle.XGroupDelConsumer(stream, group, consumer).Result()
}

// TODO: it might be add commands like XINFO, XLEN, XTRIM, XPENDING, XRANGE, XREVRANGE
