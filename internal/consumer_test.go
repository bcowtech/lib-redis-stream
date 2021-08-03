package internal

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	redis "github.com/go-redis/redis/v7"
)

func TestConsumer(t *testing.T) {
	/*
		XGROUP CREATE gotestStream1 gotestGroup $ MKSTREAM
		XGROUP CREATE gotestStream2 gotestGroup $ MKSTREAM

		XADD gotestStream1 * name luffy age 19
		XADD gotestStream1 * name nami age 21
		XADD gotestStream2 * name roger age ??
		XADD gotestStream2 * name ace age 22

		XGROUP DESTROY gotestStream1 gotestGroup
		XGROUP DESTROY gotestStream2 gotestGroup

		DEL gotestStream1
		DEL gotestStream2
	*/

	err := setupTestConsumer()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := teardownTestConsumer()
		if err != nil {
			t.Fatal(err)
		}
	}()

	opt := redis.Options{
		Addr: os.Getenv("REDIS_SERVER"),
		DB:   0,
	}

	c := &Consumer{
		Group:       "gotestGroup",
		Name:        "gotestConsumer",
		RedisOption: &opt,
	}

	c.Subscribe(
		StreamKeyOffset{Key: "gotestStream1", Offset: NextStreamOffset},
		StreamKeyOffset{Key: "gotestStream2", Offset: NextStreamOffset},
	)

	ctx, _ := context.WithTimeout(context.Background(), 15*time.Second)

	var msgCnt int = 0

	run := true
	for run {
		select {
		case <-ctx.Done():
			t.Logf("done")
			run = false

		default:
			res, err := c.Read(8, 10*time.Millisecond)
			// t.Logf("%+v", res)
			if err != nil {
				if err != redis.Nil {
					t.Errorf("%+v\n", err)
					return
				}
			} else {
				if len(res) > 0 {
					for _, stream := range res {
						for _, message := range stream.Messages {
							log.Printf("Stream: %s, Message:%+v\n", stream.Stream, message)
							c.Handle().XAck(stream.Stream, message.ID)
							msgCnt++
						}
					}
				}
			}
		}
	}

	// assert
	{
		var expectedMsgCnt int = 4
		if msgCnt != expectedMsgCnt {
			t.Errorf("expect %d messages, but got %d messages", expectedMsgCnt, msgCnt)
		}
	}
}

func setupTestConsumer() error {
	opt := redis.Options{
		Addr: os.Getenv("REDIS_SERVER"),
		DB:   0,
	}

	client := redis.NewClient(&opt)
	if client == nil {
		return fmt.Errorf("fail to create redis.Client")
	}

	var err error
	_, err = client.Ping().Result()
	if err != nil {
		if err != redis.Nil {
			return err
		}
	}

	{
		/*
			DEL gotestStream1
			DEL gotestStream2
		*/
		_, err = client.Del("gotestStream1", "gotestStream2").Result()
		if err != nil {
			return err
		}

		/*
			XGROUP CREATE gotestStream1 gotestGroup $ MKSTREAM
			XGROUP CREATE gotestStream2 gotestGroup $ MKSTREAM

			XADD gotestStream1 * name luffy age 19
			XADD gotestStream1 * name nami age 21
			XADD gotestStream2 * name roger age ??
			XADD gotestStream2 * name ace age 22
		*/
		_, err = client.XGroupCreateMkStream("gotestStream1", "gotestGroup", LastStreamOffset).Result()
		if err != nil {
			return err
		}
		_, err = client.XGroupCreateMkStream("gotestStream2", "gotestGroup", LastStreamOffset).Result()
		if err != nil {
			return err
		}

		_, err = client.XAdd(&redis.XAddArgs{Stream: "gotestStream1", Values: map[string]interface{}{
			"name": "luffy",
			"age":  19,
		}}).Result()
		if err != nil {
			return err
		}
		_, err = client.XAdd(&redis.XAddArgs{Stream: "gotestStream1", Values: map[string]interface{}{
			"name": "nami",
			"age":  21,
		}}).Result()
		if err != nil {
			return err
		}
		_, err = client.XAdd(&redis.XAddArgs{Stream: "gotestStream2", Values: map[string]interface{}{
			"name": "roger",
			"age":  "??",
		}}).Result()
		if err != nil {
			return err
		}
		_, err = client.XAdd(&redis.XAddArgs{Stream: "gotestStream2", Values: map[string]interface{}{
			"name": "ace",
			"age":  "22",
		}}).Result()
		if err != nil {
			return err
		}
	}
	return client.Close()
}

func teardownTestConsumer() error {
	opt := redis.Options{
		Addr: os.Getenv("REDIS_SERVER"),
		DB:   0,
	}

	client := redis.NewClient(&opt)
	if client == nil {
		return fmt.Errorf("fail to create redis.Client")
	}

	var err error
	_, err = client.Ping().Result()
	if err != nil {
		if err != redis.Nil {
			return err
		}
	}

	{
		/*
			XGROUP DESTROY gotestStream1 gotestGroup
			XGROUP DESTROY gotestStream2 gotestGroup

			DEL gotestStream1
			DEL gotestStream2
		*/
		_, err = client.XGroupDestroy("gotestStream1", "gotestGroup").Result()
		if err != nil {
			return err
		}
		_, err = client.XGroupDestroy("gotestStream2", "gotestGroup").Result()
		if err != nil {
			return err
		}

		_, err = client.Del("gotestStream1", "gotestStream2").Result()
		if err != nil {
			return err
		}
	}
	return client.Close()
}
