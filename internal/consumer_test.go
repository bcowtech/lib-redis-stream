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

func TestConsumer_Read(t *testing.T) {
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

	err := setupTestConsumer_Read()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := teardownTestConsumer_Read()
		if err != nil {
			t.Fatal(err)
		}
	}()

	opt := redis.UniversalOptions{
		Addrs: []string{os.Getenv("REDIS_SERVER")},
		DB:    0,
	}

	c := &Consumer{
		Group:       "gotestGroup",
		Name:        "gotestConsumer",
		RedisOption: &opt,
	}

	err = c.Subscribe(
		StreamOffset{Stream: "gotestStream1", Offset: StreamNeverDeliveredOffset},
		StreamOffset{Stream: "gotestStream2", Offset: StreamNeverDeliveredOffset},
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, _ := context.WithTimeout(context.Background(), 15*time.Second)

	var msgCnt int = 0

	run := true
	for run {
		select {
		case <-ctx.Done():
			t.Logf("done")
			c.Close()
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
							c.Handle().XAck(stream.Stream, c.Group, message.ID)
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

func setupTestConsumer_Read() error {
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
		_, err = client.XGroupCreateMkStream("gotestStream1", "gotestGroup", StreamLastDeliveredID).Result()
		if err != nil {
			return err
		}
		_, err = client.XGroupCreateMkStream("gotestStream2", "gotestGroup", StreamLastDeliveredID).Result()
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

func teardownTestConsumer_Read() error {
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

func TestConsumer_Claim(t *testing.T) {
	/*
		XGROUP CREATE gotestStream1 gotestGroup $ MKSTREAM
		XGROUP CREATE gotestStream2 gotestGroup $ MKSTREAM

		XADD gotestStream1 * name luffy age 19
		XADD gotestStream1 * name nami age 21
		XADD gotestStream2 * name roger age ??
		XADD gotestStream2 * name ace age 22

		XREADGROUP GROUP gotestGroup gotest-main COUNT 8 STREAMS gotestStream1 gotestStream2 > >

		XGROUP DESTROY gotestStream1 gotestGroup
		XGROUP DESTROY gotestStream2 gotestGroup

		DEL gotestStream1
		DEL gotestStream2
	*/

	var err error
	err = setupTestConsumer_Claim()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := teardownTestConsumer_Claim()
		if err != nil {
			t.Fatal(err)
		}
	}()

	opt := redis.UniversalOptions{
		Addrs: []string{os.Getenv("REDIS_SERVER")},
		DB:    0,
	}

	c := &Consumer{
		Group:       "gotestGroup",
		Name:        "gotestConsumer",
		RedisOption: &opt,
	}

	err = c.Subscribe(
		StreamOffset{Stream: "gotestStream1", Offset: StreamNeverDeliveredOffset},
		StreamOffset{Stream: "gotestStream2", Offset: StreamNeverDeliveredOffset},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var msgCnt int = 0
	time.Sleep(4 * time.Second)

	res, err := c.Claim(4*time.Second, 1, 10)
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
					c.Handle().XAck(stream.Stream, c.Group, message.ID)
					msgCnt++
				}
			}
		}
	}

	// assert
	{
		var expectedMsgCnt int = 2
		if msgCnt != expectedMsgCnt {
			t.Errorf("expect %d messages, but got %d messages", expectedMsgCnt, msgCnt)
		}
	}
}

func setupTestConsumer_Claim() error {
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
		_, err = client.XGroupCreateMkStream("gotestStream1", "gotestGroup", StreamLastDeliveredID).Result()
		if err != nil {
			return err
		}
		_, err = client.XGroupCreateMkStream("gotestStream2", "gotestGroup", StreamLastDeliveredID).Result()
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

		/*
			XREADGROUP GROUP gotestGroup gotest-main COUNT 8 STREAMS gotestStream1 gotestStream2 > >
		*/
		_, err = client.XReadGroup(&redis.XReadGroupArgs{
			Group:    "gotestGroup",
			Consumer: "gotest-main",
			Count:    8,
			Streams:  []string{"gotestStream1", "gotestStream2", ">", ">"},
			Block:    100 * time.Millisecond,
		}).Result()
		if err != nil {
			if err != redis.Nil {
				return err
			}
		}
	}
	return client.Close()
}

func teardownTestConsumer_Claim() error {
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
