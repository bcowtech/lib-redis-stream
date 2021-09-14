package redis

import (
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
	IdlingTimeout           time.Duration // 若沒有任何訊息時等待多久
	ClaimSensitivity        int           // Read 時取得的訊息數小於等於 n 的話, 執行 Claim
	ClaimOccurrenceRate     int32         // Read 每執行 n 次後 執行 Claim 1 次
	MessageHandler          MessageHandleProc
	UnhandledMessageHandler MessageHandleProc
	ErrorHandler            RedisErrorHandleProc

	handle   *internal.Consumer
	stopChan chan bool
	wg       sync.WaitGroup

	claimCountWheel *internal.CountWheel

	mutex       sync.Mutex
	initialized bool
	running     bool
	disposed    bool
}

func (c *Consumer) Subscribe(streams ...StreamOffset) error {
	if c.disposed {
		logger.Panic("the Consumer has been disposed")
	}
	if c.running {
		logger.Panic("the Consumer is running")
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

	if len(streams) == 0 {
		return nil
	}
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

	// reset
	c.claimCountWheel.Reset()

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
				err := c.poll(ctx)
				if err != nil {
					if !c.processRedisError(err) {
						logger.Fatalf("%% Error: %v\n", err)
						return
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

	if c.stopChan != nil {
		c.stopChan <- true
		close(c.stopChan)
	}

	c.wg.Wait()
}

func (c *Consumer) init() {
	if c.initialized {
		return
	}

	if c.stopChan == nil {
		c.stopChan = make(chan bool, 1)
	}

	if c.claimCountWheel == nil {
		c.claimCountWheel = internal.NewCountWheel(c.ClaimOccurrenceRate)
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

func (c *Consumer) poll(ctx *ConsumeContext) error {
	var (
		numMessage int = 0
	)

	// perform XREADGROUP
	{
		streams, err := c.handle.Read(c.MaxInFlight, c.MaxPollingTimeout)
		if err != nil {
			if err != redis.Nil {
				return err
			}
		}

		if len(streams) > 0 {
			for _, stream := range streams {
				for _, message := range stream.Messages {
					c.MessageHandler(ctx, stream.Stream, &message)
					numMessage++
				}
			}
		}
	}

	// perform XAUTOCLAIM
	if c.claimCountWheel.Spin() || numMessage <= c.ClaimSensitivity {
		// fmt.Println("***CLAIM")
		streams, err := c.handle.Claim(c.MaxInFlight, c.AutoClaimMinIdleTime)
		if err != nil {
			if err != redis.Nil {
				return err
			}
		}
		if len(streams) > 0 {
			for _, stream := range streams {
				for _, message := range stream.Messages {
					c.MessageHandler(ctx, stream.Stream, &message)
				}
			}
			return nil
		}

		if numMessage == 0 {
			time.Sleep(c.IdlingTimeout)
		}
	}
	return nil
}
