# priority_queue
基于 redis 实现的优先级队列



## Usage

```go
var ctx = context.Background()
var queueName = "some queue name"

// (创建 redis 实例)
var rds *redis.Client


queue := NewPriorityQueue(queueName, rds)

// 将一个任务推到队列顶端
err := queue.LPush(ctx, "key name")
// check error here

// 将一个任务推到队列底端
err := queue.RPush(ctx, "key name")
// check error here

// 获取队列顶端的成员
key, err := queue.Head(ctx)
// check error here

// 获取队列底端的成员
key, err := queue.Tail(ctx)
// check error here

// 增加成员的优先级
err := queue.Increase(ctx, "key name", 10)
// check error here

// 降低成员的优先级
err := queue.Increase(ctx, "key name", -10)
// check error here
```

