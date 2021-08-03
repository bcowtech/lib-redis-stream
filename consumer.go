package redis

import (
	"fmt"
	"sync"
	"time"

	"github.com/bcowtech/lib-redis-stream/internal"
	redis "github.com/go-redis/redis/v7"
)

type Consumer struct {
	Group                   string
	Name                    string
	RedisOption             *redis.Options
	MaxInFlight             int64
	MaxPollingTimeout       time.Duration
	AutoClaimMinIdleTime    time.Duration
	MessageHandler          MessageHandleProc
	UnhandledMessageHandler MessageHandleProc
	ErrorHandler            RedisErrorHandleProc

	handle   *internal.Consumer
	stopChan chan bool
	wg       sync.WaitGroup

	mutex       sync.Mutex
	initialized bool
	running     bool
	disposed    bool
}

func (c *Consumer) Subscribe(streams ...StreamKeyOffset) error {
	if c.disposed {
		return fmt.Errorf("the Consumer has been disposed")
	}
	if c.running {
		return fmt.Errorf("the Consumer is running")
	}

	var err error
	c.mutex.Lock()
	defer func() {
		if err != nil {
			c.running = false
			c.disposed = true
		}
		c.mutex.Unlock()
	}()
	c.init()
	c.running = true

	// new consumer
	{
		consumer := &internal.Consumer{
			Group:       c.Group,
			Name:        c.Name,
			RedisOption: c.RedisOption,
		}

		err = consumer.Subscribe(streams...)
		if err != nil {
			return err
		}

		c.handle = consumer
	}

	var (
		ctx = &ConsumeContext{
			consumer:                c,
			unhandledMessageHandler: c.UnhandledMessageHandler,
		}
	)

	go func() {
		c.wg.Add(1)
		defer c.wg.Done()

		defer c.handle.Close()

		for {
			select {
			case <-c.stopChan:
				return

			default:
				// perform XREADGROUP
				{
					streams, err := c.handle.Read(c.MaxInFlight, c.MaxPollingTimeout)
					if err != nil {
						if err != redis.Nil {
							if !c.processRedisError(err) {
								logger.Fatalf("%% Error: %v\n", err)
								return
							}
						}
						continue
					}
					if len(streams) > 0 {
						for _, stream := range streams {
							for _, message := range stream.Messages {
								c.MessageHandler(ctx, stream.Stream, &message)
							}
						}
						continue
					}
				}
				// perform XAUTOCLAIM
				{
					streams, err := c.handle.Claim(c.MaxInFlight, c.AutoClaimMinIdleTime)
					if err != nil {
						if err != redis.Nil {
							if !c.processRedisError(err) {
								logger.Fatalf("%% Error: %v\n", err)
								return
							}
						}
						continue
					}
					if len(streams) > 0 {
						for _, stream := range streams {
							for _, message := range stream.Messages {
								c.MessageHandler(ctx, stream.Stream, &message)
							}
						}
						continue
					}
				}
			}
		}
	}()
	return nil
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

	c.stopChan <- true
	close(c.stopChan)

	c.wg.Wait()
}

func (c *Consumer) init() {
	if c.initialized {
		return
	}

	if c.stopChan == nil {
		c.stopChan = make(chan bool, 1)
	}
	c.initialized = true
}

func (c *Consumer) processRedisError(err error) (disposed bool) {
	if c.ErrorHandler != nil {
		return c.ErrorHandler(err)
	}
	return false
}

func (c *Consumer) getRedisClient() *redis.Client {
	return c.handle.Handle()
}
