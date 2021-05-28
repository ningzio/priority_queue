package priority_queue

import (
	"context"
	"github.com/go-redis/redis/v8"
	"testing"
)

func TestPriorityQueue(t *testing.T) {
	// prepare
	ctx := context.Background()

	rds := redis.NewClient(&redis.Options{
		Addr:     "192.168.0.11:6379",
		Password: "1234",
		DB:       2,
	})
	if err := rds.Ping(ctx).Err(); err != nil {
		t.Fatalf("can't connect to redis: %v", err)
	}

	defer func() {
		rds.Del(ctx, "test_queue")
	}()

	queue := NewPriorityQueue("test_queue", rds)

	// push some keys to top of the queue
	if err := queue.LPush(ctx, "key1"); err != nil {
		t.Fatalf("something went wrong: %v", err)
	}
	if err := queue.LPush(ctx, "key2"); err != nil {
		t.Fatalf("something went wrong: %v", err)
	}
	if err := queue.LPush(ctx, "key3"); err != nil {
		t.Fatalf("something went wrong: %v", err)
	}

	// check queue
	key, err := queue.Head(ctx)
	if err != nil {
		t.Fatalf("something went wrong: %v", err)
	}

	if key != "key3" {
		t.Fatalf("expect key: key3, got: %s", key)
	}

	key, err = queue.Tail(ctx)
	if err != nil {
		t.Fatalf("something went wrong: %v", err)
	}

	if key != "key1" {
		t.Fatalf("expect key: key1, got: %s", key)
	}

	// put key to bottom of the queue
	if err := queue.RPush(ctx, "key4"); err != nil {
		t.Fatalf("something went wrong: %v", err)
	}

	key, err = queue.Tail(ctx)
	if err != nil {
		t.Fatalf("something went wrong: %v", err)
	}

	if key != "key4" {
		t.Fatalf("expect key: key4, got: %s", key)
	}

	// test count
	count, err := queue.Count(ctx)
	if err != nil {
		t.Fatalf("something went wrong: %v", err)
	}
	if count != 4 {
		t.Fatalf("expect 4, bug got: %d", count)
	}

	// test delete
	if err := queue.Delete(ctx, "key1"); err != nil {
		t.Fatalf("something went wrong: %v", err)
	}

	count, err = queue.Count(ctx)
	if err != nil {
		t.Fatalf("something went wrong: %v", err)
	}
	if count != 3 {
		t.Fatalf("expect 3, bug got: %d", count)
	}

	// test increase
	if err := queue.Increase(ctx, "key4", 4); err != nil {
		t.Fatalf("something went wrong: %v", err)
	}

	key, err = queue.Head(ctx)
	if err != nil {
		t.Fatalf("something went wrong: %v", err)
	}

	if key != "key4" {
		t.Fatalf("expect key: key4, got: %s", key)
	}

	key, err = queue.Tail(ctx)
	if err != nil {
		t.Fatalf("something went wrong: %v", err)
	}

	if key != "key2" {
		t.Fatalf("expect key: key2, got: %s", key)
	}

}
