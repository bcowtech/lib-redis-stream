package redis

import (
	"fmt"
	"sync"

	"github.com/bcowtech/lib-redis-stream/internal"
	redis "github.com/go-redis/redis/v7"
)

type Producer struct {
	handle redis.UniversalClient

	wg       sync.WaitGroup
	mutex    sync.Mutex
	disposed bool
}

func NewProducer(opt *UniversalOptions) (*Producer, error) {
	instance := &Producer{}

	var err error
	err = instance.init(opt)
	if err != nil {
		return nil, err
	}
	return instance, nil
}

func (p *Producer) Handle() redis.UniversalClient {
	return p.handle
}

func (p *Producer) Write(stream string, id string, content map[string]interface{}) (string, error) {
	if p.disposed {
		return "", fmt.Errorf("the Producer has been disposed")
	}

	p.wg.Add(1)
	defer p.wg.Done()

	reply, err := p.handle.XAdd(&redis.XAddArgs{
		Stream: stream,
		ID:     id,
		Values: content,
	}).Result()
	if err != nil {
		if err != redis.Nil {
			return "", err
		}
	}
	return reply, nil
}

func (p *Producer) Close() {
	if p.disposed {
		return
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.disposed = true

	p.wg.Wait()
	p.handle.Close()
}

func (p *Producer) init(opt *UniversalOptions) error {
	client, err := internal.CreateRedisUniversalClient(opt)
	if err != nil {
		return err
	}
	p.handle = client
	return nil
}
