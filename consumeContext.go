package redis

var _ MessageHandleProc = StopRecursiveForwardUnhandledMessageHandler

func StopRecursiveForwardUnhandledMessageHandler(ctx *ConsumeContext, stream string, message *XMessage) {
	logger.Fatal("invalid forward; it might be recursive forward message to unhandledMessageHandler")
}

type ConsumeContext struct {
	unhandledMessageHandler MessageHandleProc

	consumer *Consumer
}

// get redis client
func (c *ConsumeContext) Handle() UniversalClient {
	return c.consumer.getRedisClient()
}

func (c *ConsumeContext) ConsumerGroup() string {
	return c.consumer.Group
}

func (c *ConsumeContext) ConsumerName() string {
	return c.consumer.Name
}

func (c *ConsumeContext) Ack(key string, id ...string) (int64, error) {
	return c.consumer.handle.Ack(key, id...)
}

func (c *ConsumeContext) Del(key string, id ...string) (int64, error) {
	return c.consumer.handle.Del(key, id...)
}

func (c *ConsumeContext) ForwardUnhandledMessage(stream string, message *XMessage) {
	if c.unhandledMessageHandler != nil {
		ctx := &ConsumeContext{
			consumer:                c.consumer,
			unhandledMessageHandler: StopRecursiveForwardUnhandledMessageHandler,
		}
		c.unhandledMessageHandler(ctx, stream, message)
	}
}
