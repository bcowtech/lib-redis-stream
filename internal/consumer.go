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
	RedisOption *redis.Options

	handle *redis.Client
	wg     sync.WaitGroup

	streamKeys       []string
	streamKeyOffsets []string

	mutex    sync.Mutex
	running  bool
	disposed bool
}

func (c *Consumer) Handle() *redis.Client {
	return c.handle
}

func (c *Consumer) Subscribe(streams ...StreamKeyOffset) error {
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
			keys = append(keys, s.Key)
		}
		keyOffsets = append(keyOffsets, keys...)
		for i := 0; i < size; i++ {
			s := streams[i]
			if len(s.Offset) == 0 {
				keyOffsets = append(keyOffsets, NextStreamOffset)
			} else {
				keyOffsets = append(keyOffsets, s.Offset)
			}
		}
		c.streamKeys = keys
		c.streamKeyOffsets = keyOffsets
	}
	return nil
}

func (c *Consumer) Claim(count int64, minIdleTime time.Duration) ([]redis.XStream, error) {
	if c.disposed {
		return nil, fmt.Errorf("the Consumer has been disposed")
	}
	if !c.running {
		return nil, fmt.Errorf("the Consumer is not running")
	}

	c.wg.Add(1)
	defer c.wg.Done()

	for _, stream := range c.streamKeys {
		// the block will fetching all pending messages till it is empty
		for done := false; !done; done = true {
			// fetch all pending messages from specified redis stream key
			reply, err := c.handle.XPendingExt(&redis.XPendingExtArgs{
				Stream: stream,
				Group:  c.Group,
				Start:  "-",
				End:    "+",
				Count:  count,
			}).Result()
			if err != nil {
				if err != redis.Nil {
					return nil, err
				}
			}

			for _, pending := range reply {
				// filter the message ids that only the idle time over
				// the Worker.AutoClaimMinIdleTime
				if pending.Idle > minIdleTime {
					messages, err := c.handle.XClaim(&redis.XClaimArgs{
						Stream:   stream,
						Group:    c.Group,
						Consumer: c.Name,
						MinIdle:  minIdleTime,
						Messages: []string{pending.ID},
					}).Result()
					if err != nil {
						if err != redis.Nil {
							return nil, err
						}
					}

					return []redis.XStream{
						{
							Stream:   stream,
							Messages: messages,
						},
					}, err
				}
			}
		}
	}
	return nil, nil
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
		client, err := CreateRedisClient(c.RedisOption)
		if err != nil {
			return err
		}

		c.handle = client
	}
	return nil
}
