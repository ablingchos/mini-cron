package kvdb

import (
	"context"
	"fmt"

	redisClient "git.code.oa.com/red/ms-go/pkg/redis"
)

type Z = redisClient.Z

type RedisDB struct {
	DbClient *redisClient.RedisClient
	// Rwmutex  sync.RWMutex
}

// func (r *RedisDB) Connect(dbURI string) error {
// 	var err error
// 	r.DbClient, err = redisClient.NewRedisClient(dbURI)
// 	return err
// }

// 连接db，此处为redis
func (r *RedisDB) Connect(dbURI string) error {
	var err error
	r.DbClient, err = redisClient.NewRedisClient(dbURI)
	if err != nil {
		return err
	}
	return nil
}

func (r *RedisDB) Set(ctx context.Context, key string, value string) error {
	return r.DbClient.Set(ctx, key, value, 0)
}

func (r *RedisDB) Get(ctx context.Context, key string) (string, error) {
	return r.DbClient.Get(ctx, key)
}

func (r *RedisDB) Del(ctx context.Context, key string) (bool, error) {
	return r.DbClient.Del(ctx, key)
}

func (r *RedisDB) HGet(ctx context.Context, key, field string) (string, error) {
	return r.DbClient.HGet(ctx, key, field)
}

func (r *RedisDB) HSet(ctx context.Context, key, field string, value interface{}) error {
	return r.DbClient.HSet(ctx, key, field, value)
}

func (r *RedisDB) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return r.DbClient.HGetAll(ctx, key)
}

func (r *RedisDB) HDel(ctx context.Context, key string, fields ...string) error {
	return r.DbClient.HDel(ctx, key, fields...)
}

func (r *RedisDB) HMGet(ctx context.Context, key string, fields ...string) ([]string, error) {
	return r.DbClient.HMGet(ctx, key, fields...)
}

func (r *RedisDB) HMSet(ctx context.Context, key string, fields map[string]interface{}) error {
	return r.DbClient.HMSet(ctx, key, fields)
}

// func (r *RedisDB) LPush(ctx context.Context, key string, values ...interface{}) (interface{}, error) {
// 	return r.DbClient.LPush(ctx, key, values...)
// }

func (r *RedisDB) RPush(ctx context.Context, key string, values interface{}) (interface{}, error) {
	return r.DbClient.RPush(ctx, key, values)
}

func (r *RedisDB) LPop(ctx context.Context, key string) (interface{}, error) {
	return r.DbClient.LPop(ctx, key)
}

// func (r *RedisDB) RPop(ctx context.Context, key string) (interface{}, error) {
// 	return r.DbClient.RPop(ctx, key)
// }

func (r *RedisDB) LLen(ctx context.Context, key string) (interface{}, error) {
	return r.DbClient.LLen(ctx, key)
}

func (r *RedisDB) LRange(ctx context.Context, key string, start, stop int64) (interface{}, error) {
	return r.DbClient.LRange(ctx, key, start, stop)
}

func (r *RedisDB) ZAdd(ctx context.Context, key string, members ...interface{}) (interface{}, error) {
	zMembers := make([]*Z, len(members))
	for i, member := range members {
		zMember, ok := member.(*Z)
		if !ok {
			fmt.Println("Error while transfer \"interface{]\" to \"*Z\"}")
		}
		zMembers[i] = zMember
	}
	return r.DbClient.ZAdd(ctx, key, zMembers...)
}

func (r *RedisDB) ZRange(ctx context.Context, key string, start, stop int64) (interface{}, error) {
	return r.DbClient.ZRange(ctx, key, start, stop)
}

func (r *RedisDB) ZRevRange(ctx context.Context, key string, start, stop int64) (interface{}, error) {
	return r.DbClient.ZRevRange(ctx, key, start, stop)
}

// func (r *RedisDB) ZRangeWithScores(ctx context.Context, key string, start, stop int64) (interface{}, error) {
// 	return r.DbClient.ZRangeWithScores(ctx, key, start, stop)
// }

func (r *RedisDB) ZRangeByScore(ctx context.Context, key string, min, max interface{}) (interface{}, error) {
	return r.DbClient.ZRangeByScore(ctx, key, min.(string), max.(string))
}

func (r *RedisDB) ZCard(ctx context.Context, key string) (interface{}, error) {
	return r.DbClient.ZCard(ctx, key)
}

func (r *RedisDB) ZCount(ctx context.Context, key string, min, max interface{}) (interface{}, error) {
	return r.DbClient.ZCount(ctx, key, min.(string), max.(string))
}

func (r *RedisDB) ZRem(ctx context.Context, key string, members ...interface{}) (interface{}, error) {
	return r.DbClient.ZRem(ctx, key, members...)
}

func (r *RedisDB) ZScore(ctx context.Context, key, member string) (interface{}, error) {
	return r.DbClient.ZScore(ctx, key, member)
}

func (r *RedisDB) ZRemRangeByScore(ctx context.Context, key string, min, max interface{}) (interface{}, error) {
	return r.DbClient.ZRemRangeByScore(ctx, key, min.(string), max.(string))
}

func (r *RedisDB) ZIncrBy(ctx context.Context, key string, incr interface{}, member string) (interface{}, error) {
	return r.DbClient.ZIncrBy(ctx, key, incr.(float64), member)
}
