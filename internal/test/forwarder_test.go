package test

import (
	"os"
	"testing"

	redis "github.com/bcowtech/lib-redis-stream"
)

func TestForwarder(t *testing.T) {
	f, err := redis.NewForwarder(&redis.Options{
		Addr: os.Getenv("REDIS_SERVER"),
		DB:   0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		client := f.Handle()
		client.Del("gotestStream1", "gotestStream2")

		f.Close()
	}()

	// reset
	{
		client := f.Handle()
		client.Del("gotestStream1", "gotestStream2")
	}

	// produce message
	{
		reply, err := f.Write("gotestStream1", redis.AutoIncrement, map[string]interface{}{
			"name": "luffy",
			"age":  19,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("ID: %s", reply)
	}

	{
		reply, err := f.Write("gotestStream1", redis.AutoIncrement, map[string]interface{}{
			"name": "nami",
			"age":  21,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("ID: %s", reply)
	}

	{
		reply, err := f.Write("gotestStream1", redis.AutoIncrement, map[string]interface{}{
			"name": "zoro",
			"age":  21,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("ID: %s", reply)
	}

	{
		reply, err := f.Write("gotestStream2", redis.AutoIncrement, map[string]interface{}{
			"name": "roger",
			"age":  "??",
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("ID: %s", reply)
	}

	{
		reply, err := f.Write("gotestStream2", redis.AutoIncrement, map[string]interface{}{
			"name": "ace",
			"age":  "22",
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("ID: %s", reply)
	}

	// assert
	{
		client := f.Handle()
		{
			msgCnt, err := client.XLen("gotestStream1").Result()
			if err != nil {
				t.Fatal(err)
			}
			var expectedMsgCnt int64 = 3
			if msgCnt != expectedMsgCnt {
				t.Errorf("expect %d messages, but got %d messages", expectedMsgCnt, msgCnt)
			}
		}
		{
			msgCnt, err := client.XLen("gotestStream2").Result()
			if err != nil {
				t.Fatal(err)
			}
			var expectedMsgCnt int64 = 2
			if msgCnt != expectedMsgCnt {
				t.Errorf("expect %d messages, but got %d messages", expectedMsgCnt, msgCnt)
			}
		}
	}
}
