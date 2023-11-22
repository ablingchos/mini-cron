package kvdb

import (
	"context"
)

// type DBInfo struct {
// 	dbname string // `json:"database"`
// 	host   string // `json:"host"`
// 	port   string // `json:"port"`
// }

// type Z = redis.Z

type KVDb interface {
	// Connect to a database
	Connect(dbURI string) error

	// Set a ctx context.Context, key-value pair in the database
	Set(ctx context.Context, key string, value string) error

	// Get the value associated with a ctx context.Context, key
	Get(ctx context.Context, key string) (string, error)

	// Delete a ctx context.Context, key-value pair from the database
	Del(ctx context.Context, key string) (bool, error)

	HGet(ctx context.Context, key, field string) (string, error)

	HSet(ctx context.Context, key, field string, value interface{}) error

	HGetAll(ctx context.Context, key string) (map[string]string, error)

	HDel(ctx context.Context, key string, fields ...string) error

	HMGet(ctx context.Context, key string, fields ...string) ([]string, error)

	HMSet(ctx context.Context, key string, fields map[string]interface{}) error

	// LPush(ctx context.Context, key string, values ...interface{}) (interface{}, error)

	RPush(ctx context.Context, key string, values interface{}) (interface{}, error)

	LPop(ctx context.Context, key string) (interface{}, error)

	// RPop(ctx context.Context, key string) (interface{}, error)

	LLen(ctx context.Context, key string) (interface{}, error)

	LRange(ctx context.Context, key string, start, stop int64) (interface{}, error)

	ZAdd(ctx context.Context, key string, members ...interface{}) (interface{}, error)

	ZRange(ctx context.Context, key string, start, stop int64) (interface{}, error)

	ZRevRange(ctx context.Context, key string, start, stop int64) (interface{}, error)

	// ZRangeWithScores(ctx context.Context, key string, start, stop int64) (interface{}, error)

	ZRangeByScore(ctx context.Context, key string, min, max interface{}) (interface{}, error)

	ZCard(ctx context.Context, key string) (interface{}, error)

	ZCount(ctx context.Context, key string, min, max interface{}) (interface{}, error)

	// ZRank(ctx context.Context, key, memeber string) (interface{}, error)

	// ZRevRank(ctx context.Context, key, member string) (interface{}, error)

	ZRem(ctx context.Context, key string, members ...interface{}) (interface{}, error)

	ZScore(ctx context.Context, key, member string) (interface{}, error)

	ZRemRangeByScore(ctx context.Context, key string, min, max interface{}) (interface{}, error)

	ZIncrBy(ctx context.Context, key string, incr interface{}, member string) (interface{}, error)
}

// func Connect(dbtype, dbURI string) (interface{}, error) {
// 	var db KVDb
// 	switch dbtype {
// 	case "redis":
// 		db = &RedisDB{}
// 	default:
// 		mlog.Fatal("Unsupported database type")
// 		return nil, nil
// 	}

// 	dbclient, err := db.Connect(dbURI)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return dbclient, err
// }
