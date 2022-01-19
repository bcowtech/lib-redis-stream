package internal

import (
	"fmt"
	"sync"
	"time"

	redis "github.com/go-redis/redis/v7"
)

type Consumer struct {
	Group       string
	Name        string
	RedisOption *redis.UniversalOptions

	handle UniversalClient
	wg     sync.WaitGroup

	streamKeys       []string
	streamKeyOffsets []string

	mutex    sync.Mutex
	running  bool
	disposed bool
}

func (c *Consumer) Handle() UniversalClient {
	return c.handle
}

func (c *Consumer) Subscribe(streams ...StreamOffset) error {
	if len(streams) == 0 {
		return fmt.Errorf("specified streams is empty")
	}
	if c.disposed {
		return fmt.Errorf("the Consumer has been disposed")
	}
	if c.running {
		return fmt.Errorf("the Consumer is running")
	}

	var (
		size       = len(streams)
		keys       = make([]string, 0, size)
		keyOffsets = make([]string, 0, size*2)
	)

	var err error
	c.mutex.Lock()
	defer func() {
		if err != nil {
			c.running = false
			c.disposed = true
		}
		c.mutex.Unlock()
	}()
	c.running = true

	// init clent
	if err = c.configRedisClient(); err != nil {
		return err
	}

	if size > 0 {
		for i := 0; i < size; i++ {
			s := streams[i]
			keys = append(keys, s.Stream)
		}
		keyOffsets = append(keyOffsets, keys...)
		for i := 0; i < size; i++ {
			s := streams[i]
			if len(s.Offset) == 0 {
				keyOffsets = append(keyOffsets, StreamNeverDeliveredOffset)
			} else {
				keyOffsets = append(keyOffsets, s.Offset)
			}
		}
		c.streamKeys = keys
		c.streamKeyOffsets = keyOffsets
	}
	return nil
}

func (c *Consumer) Claim(minIdleTime time.Duration, count int64, pendingFetchingSize int64) ([]redis.XStream, error) {
	if c.disposed {
		return nil, fmt.Errorf("the Consumer has been disposed")
	}
	if !c.running {
		return nil, fmt.Errorf("the Consumer is not running")
	}

	c.wg.Add(1)
	defer c.wg.Done()

	var resultStream []redis.XStream = make([]redis.XStream, 0, len(c.streamKeys))
	for _, stream := range c.streamKeys {
		// fetch all pending messages from specified redis stream key
		pendingSet, err := c.handle.XPendingExt(&redis.XPendingExtArgs{
			Stream: stream,
			Group:  c.Group,
			Start:  "-",
			End:    "+",
			Count:  pendingFetchingSize,
		}).Result()
		if err != nil {
			if err != redis.Nil {
				return nil, err
			}
		}

		if len(pendingSet) > 0 {
			var (
				pendingMessageIDs []string = make([]string, 0, count)
			)

			// filter the message ids that only the idle time over
			// the Worker.ClaimMinIdleTime
			for _, pending := range pendingSet {
				// update the last pending id
				if pending.Idle >= minIdleTime {
					pendingMessageIDs = append(pendingMessageIDs, pending.ID)

					if len(pendingMessageIDs) == int(count) {
						break
					}
				}
			}

			if len(pendingMessageIDs) > 0 {
				claimMessages, err := c.handle.XClaim(&redis.XClaimArgs{
					Stream:   stream,
					Group:    c.Group,
					Consumer: c.Name,
					MinIdle:  minIdleTime,
					Messages: pendingMessageIDs,
				}).Result()
				if err != nil {
					if err != redis.Nil {
						return nil, err
					}
				}

				// clear invalid message IDs (ghost IDs)
				if len(claimMessages) != len(pendingMessageIDs) {

					Assert(
						len(claimMessages) < len(pendingMessageIDs),
						"the XCLAIM messages must less or equal than the XPENDING messages")

					if len(claimMessages) == 0 {
						// purge ghost IDs
						if err := c.ackGhostIDs(stream, pendingMessageIDs...); err != nil {
							return nil, err
						}
					} else {
						var (
							ghostIDs               []string
							nextClaimMessagesIndex int = 0
						)

						// Because
						//   1) the XCLAIM messages must less or equal than the XPENDING messages,
						//   2) the XCLAIM messages might be missing part messages but it won't change sequence,
						// we can check XCLAIM messages according to XPENDING messages sequence with their message ID.
						for i, id := range pendingMessageIDs {
							if nextClaimMessagesIndex < len(claimMessages) {
								if id == claimMessages[nextClaimMessagesIndex].ID {
									// advence nextMessagesIndex
									nextClaimMessagesIndex++
								} else {
									ghostIDs = append(ghostIDs, id)
								}
							} else {
								ghostIDs = append(ghostIDs, pendingMessageIDs[i:]...)
								break
							}
						}

						// purge ghost IDs
						if err := c.ackGhostIDs(stream, ghostIDs...); err != nil {
							return nil, err
						}
					}
				}

				if len(claimMessages) > 0 {
					resultStream = append(resultStream, redis.XStream{
						Stream:   stream,
						Messages: claimMessages,
					})
				}
			}
		}
	}
	return resultStream, nil
}

func (c *Consumer) Read(count int64, timeout time.Duration) ([]redis.XStream, error) {
	if c.disposed {
		return nil, fmt.Errorf("the Consumer has been disposed")
	}
	if !c.running {
		return nil, fmt.Errorf("the Consumer is not running")
	}

	c.wg.Add(1)
	defer c.wg.Done()

	messages, err := c.handle.XReadGroup(&redis.XReadGroupArgs{
		Group:    c.Group,
		Consumer: c.Name,
		Count:    count,
		Streams:  c.streamKeyOffsets,
		Block:    timeout,
	}).Result()
	if err != nil {
		if err != redis.Nil {
			return nil, err
		}
	}
	return messages, nil
}

func (c *Consumer) Ack(key string, id ...string) (int64, error) {
	if c.disposed {
		return 0, fmt.Errorf("the Consumer has been disposed")
	}
	if !c.running {
		return 0, fmt.Errorf("the Consumer is not running")
	}

	c.wg.Add(1)
	defer c.wg.Done()

	reply, err := c.handle.XAck(key, c.Group, id...).Result()
	if err != nil {
		if err != redis.Nil {
			return 0, err
		}
	}
	return reply, nil
}

func (c *Consumer) Del(key string, id ...string) (int64, error) {
	if c.disposed {
		return 0, fmt.Errorf("the Consumer has been disposed")
	}
	if !c.running {
		return 0, fmt.Errorf("the Consumer is not running")
	}

	c.wg.Add(1)
	defer c.wg.Done()

	reply, err := c.handle.XDel(key, id...).Result()
	if err != nil {
		if err != redis.Nil {
			return 0, err
		}
	}
	return reply, nil
}

func (c *Consumer) Close() {
	if c.disposed {
		return
	}

	c.mutex.Lock()
	defer func() {
		c.running = false
		c.disposed = true

		c.mutex.Unlock()
	}()

	c.wg.Wait()
	c.handle.Close()
}

func (c *Consumer) configRedisClient() error {
	if c.handle == nil {
		client, err := CreateRedisUniversalClient(c.RedisOption)
		if err != nil {
			return err
		}

		c.handle = client
	}
	return nil
}

func (c *Consumer) ackGhostIDs(stream string, ghostIDs ...string) error {
	for _, id := range ghostIDs {
		reply, err := c.handle.XRange(stream, id, id).Result()
		if err != nil {
			if err != redis.Nil {
				return err
			}
		}

		if len(reply) == 0 {
			err = c.handle.XAck(stream, c.Group, id).Err()
			if err != nil {
				if err != redis.Nil {
					return err
				}
			}
		}
	}
	return nil
}
