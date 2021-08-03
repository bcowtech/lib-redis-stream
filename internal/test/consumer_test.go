package test

import (
	"context"
	"os"
	"testing"
	"time"

	redis "github.com/bcowtech/lib-redis-stream"
)

func TestConsumer(t *testing.T) {
	var err error
	err = setupTestConsumer()
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

	var msgCnt int = 0

	c := &redis.Consumer{
		Group:                "gotestGroup",
		Name:                 "gotestConsumer",
		RedisOption:          &opt,
		MaxInFlight:          1,
		MaxPollingTimeout:    10 * time.Millisecond,
		AutoClaimMinIdleTime: 30 * time.Millisecond,
		MessageHandler: func(ctx *redis.ConsumeContext, stream string, message *redis.XMessage) {
			t.Logf("Message on %s: %v\n", stream, message)
			ctx.Ack(stream, message.ID)
			ctx.ForwardUnhandledMessage(stream, message)
			time.Sleep(500 * time.Millisecond)
			msgCnt++
		},
		UnhandledMessageHandler: func(ctx *redis.ConsumeContext, stream string, message *redis.XMessage) {
			t.Logf("UnhandledMessage on %s: %v\n", stream, message)
		},
		ErrorHandler: func(err error) (disposed bool) {
			t.Fatal(err)
			return true
		},
	}

	err = c.Subscribe(
		redis.StreamKeyOffset{Key: "gotestStream1", Offset: redis.NextStreamOffset},
		redis.StreamKeyOffset{Key: "gotestStream2", Offset: redis.NextStreamOffset},
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, _ := context.WithTimeout(context.Background(), 15*time.Second)

	select {
	case <-ctx.Done():
		c.Close()
		t.Logf("done")
		break
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
	opt := &redis.Options{
		Addr: os.Getenv("REDIS_SERVER"),
		DB:   0,
	}

	admin, err := redis.NewAdminClient(opt)
	if err != nil {
		return err
	}
	defer admin.Close()

	{
		/*
			DEL gotestStream1
			DEL gotestStream2
		*/
		_, err = admin.Handle().Del("gotestStream1", "gotestStream2").Result()
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
		_, err = admin.CreateConsumerGroupWithStream("gotestStream1", "gotestGroup", redis.LastStreamOffset)
		if err != nil {
			return err
		}
		_, err = admin.CreateConsumerGroupWithStream("gotestStream2", "gotestGroup", redis.LastStreamOffset)
		if err != nil {
			return err
		}

		p, err := redis.NewProducer(opt)
		if err != nil {
			return err
		}
		defer p.Close()

		_, err = p.Write("gotestStream1", redis.AutoIncrement, map[string]interface{}{
			"name": "luffy",
			"age":  19,
		})
		if err != nil {
			return err
		}
		_, err = p.Write("gotestStream1", redis.AutoIncrement, map[string]interface{}{
			"name": "nami",
			"age":  21,
		})
		if err != nil {
			return err
		}
		_, err = p.Write("gotestStream2", redis.AutoIncrement, map[string]interface{}{
			"name": "roger",
			"age":  "??",
		})
		if err != nil {
			return err
		}
		_, err = p.Write("gotestStream2", redis.AutoIncrement, map[string]interface{}{
			"name": "ace",
			"age":  "22",
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func teardownTestConsumer() error {
	admin, err := redis.NewAdminClient(&redis.Options{
		Addr: os.Getenv("REDIS_SERVER"),
		DB:   0,
	})
	if err != nil {
		return err
	}
	defer admin.Close()

	{
		/*
			XGROUP DESTROY gotestStream1 gotestGroup
			XGROUP DESTROY gotestStream2 gotestGroup
		*/
		_, err = admin.DeleteConsumerGroup("gotestStream1", "gotestGroup")
		if err != nil {
			return err
		}
		_, err = admin.DeleteConsumerGroup("gotestStream2", "gotestGroup")
		if err != nil {
			return err
		}

		/*
			DEL gotestStream1
			DEL gotestStream2
		*/
		_, err = admin.Handle().Del("gotestStream1", "gotestStream2").Result()
		if err != nil {
			return err
		}
	}
	return nil
}
