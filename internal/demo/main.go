package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	redis "github.com/bcowtech/lib-redis-stream"
)

func main() {
	var counter int32 = 0
	var startAt int64

	opt := redis.UniversalOptions{
		Addrs: []string{os.Getenv("REDIS_SERVER")},
		DB:    0,
	}

	startAt = time.Now().UnixNano() / time.Hour.Milliseconds()

	c := &redis.Consumer{
		Group:               "demo-group",
		Name:                "demo-counsumer",
		RedisOption:         &opt,
		MaxInFlight:         128,
		MaxPollingTimeout:   10 * time.Millisecond,
		ClaimMinIdleTime:    3 * time.Second,
		IdlingTimeout:       100 * time.Millisecond,
		ClaimSensitivity:    2,
		ClaimOccurrenceRate: 2,
		MessageHandler: func(ctx *redis.ConsumeContext, stream string, message *redis.XMessage) {
			fmt.Printf("Message on %s: %v\n", stream, message)
			ctx.Ack(stream, message.ID)
			ctx.Del(stream, message.ID)

			if atomic.AddInt32(&counter, 1) == 100000 {
				var endAt = time.Now().UnixNano() / time.Hour.Milliseconds()
				fmt.Printf("%d ms   %.3f qps\n", endAt-startAt, (float64(100000)/float64(endAt-startAt))*float64(1000))
			}
		},
		UnhandledMessageHandler: func(ctx *redis.ConsumeContext, stream string, message *redis.XMessage) {
			fmt.Printf("UnhandledMessage on %s: %v\n", stream, message)
		},
		ErrorHandler: func(err error) (disposed bool) {
			fmt.Printf("%+v\n", err)
			return true
		},
	}

	defer c.Close()

	err := c.Subscribe(
		redis.FromStreamNeverDeliveredOffset("demo-stream"),
	)
	if err != nil {
		panic(err)
	}

	//register for interupt (Ctrl+C) and SIGTERM (docker)
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigchan:
		log.Printf("%% Caught signal %v: terminating\n", sig)
		c.Close()
		fmt.Printf("done")
		break
	}
}
