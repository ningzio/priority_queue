package priority_queue

import (
	"context"
	"github.com/go-redis/redis/v8"
)

// PriorityQueue 优先级队列，队列左侧为高优先级，右侧为低优先级
type PriorityQueue interface {
	// LPush 将一个成员添加到队列的顶端
	LPush(ctx context.Context, name string) error
	// RPush 将一个成员添加到队列的底端
	RPush(ctx context.Context, name string) error
	// Head 返回优先级最高的成员
	Head(ctx context.Context) (string, error)
	// HeadWithPriority 返回优先级最高的成员，和成员的优先级
	HeadWithPriority(ctx context.Context) (string, float64, error)
	// Tail 返回优先级最低的成员
	Tail(ctx context.Context) (string, error)
	// TailWithPriority 返回优先级最低的成员，和成员的优先级
	TailWithPriority(ctx context.Context) (string, float64, error)
	// Count 返回队列中的成员总和
	Count(ctx context.Context) (int64, error)
	// Delete 将某个成员从队列中删除
	Delete(ctx context.Context, name string) error
	// Increase 增加某个成员的优先级
	Increase(ctx context.Context, name string, priority float64) error
}

// NewPriorityQueue 创建一个新的 PriorityQueue 实例
func NewPriorityQueue(queueName string, rds *redis.Client) PriorityQueue {
	return &priorityQueue{
		key: queueName,
		rds: rds,
	}
}

type priorityQueue struct {
	key string
	rds *redis.Client
}

// LPush 将一个成员添加到队列的顶端
func (p *priorityQueue) LPush(ctx context.Context, name string) error {
	_, priority, err := p.HeadWithPriority(ctx)
	if err != nil {
		return err
	}
	return p.rds.ZAdd(ctx, p.key, &redis.Z{
		Score:  priority + 1,
		Member: name,
	}).Err()
}

// RPush 将一个成员添加到队列的底端
func (p *priorityQueue) RPush(ctx context.Context, name string) error {
	_, priority, err := p.TailWithPriority(ctx)
	if err != nil {
		return err
	}
	return p.rds.ZAdd(ctx, p.key, &redis.Z{
		Score:  priority - 1,
		Member: name,
	}).Err()
}

// Head 返回优先级最高的成员
func (p *priorityQueue) Head(ctx context.Context) (string, error) {
	result, err := p.rds.ZRevRange(ctx, p.key, 0, 0).Result()
	if err != nil {
		return "", err
	}
	return result[0], err
}

// HeadWithPriority 返回优先级最高的成员，和成员的优先级
func (p *priorityQueue) HeadWithPriority(ctx context.Context) (playlistId string, priority float64, err error) {
	var result []redis.Z
	result, err = p.rds.ZRevRangeWithScores(ctx, p.key, 0, 0).Result()
	if err != nil {
		return
	}
	if len(result) == 0 {
		return
	}
	playlistId = result[0].Member.(string)
	priority = result[0].Score
	return
}

// Tail 返回优先级最低的成员
func (p *priorityQueue) Tail(ctx context.Context) (string, error) {
	result, err := p.rds.ZRange(ctx, p.key, 0, 0).Result()
	if err != nil {
		return "", err
	}
	if len(result) == 0 {
		return "", nil
	}
	return result[0], nil
}

// TailWithPriority 返回优先级最低的成员，和成员的优先级
func (p *priorityQueue) TailWithPriority(ctx context.Context) (playlistId string, priority float64, err error) {
	var result []redis.Z
	result, err = p.rds.ZRangeWithScores(ctx, p.key, 0, 0).Result()
	if err != nil {
		return
	}
	if len(result) == 0 {
		return
	}
	playlistId = result[0].Member.(string)
	priority = result[0].Score
	return
}

// Count 返回队列中的成员总和
func (p *priorityQueue) Count(ctx context.Context) (int64, error) {
	return p.rds.ZCard(ctx, p.key).Result()
}

// Delete 将某个成员从队列中删除
func (p *priorityQueue) Delete(ctx context.Context, name string) error {
	return p.rds.ZRem(ctx, p.key, name).Err()
}

// Increase 增加某个成员的优先级
func (p *priorityQueue) Increase(ctx context.Context, name string, priority float64) error {
	return p.rds.ZIncrBy(ctx, p.key, priority, name).Err()
}

// type assertion
var _ PriorityQueue = &priorityQueue{}
