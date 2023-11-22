package main

import (
	"time"

	"git.code.oa.com/red/ms-go/pkg/mlog"
	"github.com/ablingchos/my-project/internal/pkg/scheduler"
	"github.com/ablingchos/my-project/internal/pkg/worker"
	"go.uber.org/zap"
)

var (
	// // redisURI = flag.String("redisURI", "", "redis uri")
	redisURI     = "redis://user:Aikefu0530@localhost:6379"
	endpoints    = "http://localhost:2379"
	schedulerKey = "schedulerURI"
	schedulerURI = "localhost:50051"
	workerURI    = "localhost:40051"
	interval     = 10 * time.Second
	loc          *time.Location
)

func initial() error {
	// 设置时区
	var err error
	loc, err = time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return err
	}

	return nil
}

func main() {

	err := initial()
	if err != nil {
		mlog.Fatal("Failed to set time zone!", zap.Error(err))
	}

	go scheduler.Initial(redisURI, endpoints, schedulerKey, schedulerURI, loc, interval)
	// if err != nil {
	// 	mlog.Fatal("Failed to start scheduler", zap.Error(err))
	// }

	go worker.Initial(redisURI, endpoints, schedulerKey, workerURI, loc)
	// if err != nil {
	// 	mlog.Fatal("Failed to start worker", zap.Error(err))
	// }

	// test_redis(redisdb)
	// myetcd.PrintSchedulerURI(endpoints)

	// go myetcd.Watcher_Test(endpoints)
	// go myetcd.HeartBeat(endpoints)
	// go myetcd.NewWorker(endpoints)
	// go myetcd.NewScheduler(endpoints)
	// go myetcd.NewWorker(endpoints)

	select {}
}

// func test_redis(redisdb kvdb.KVDb) {
// 	test_get(redisdb)
// 	test_hgetall(redisdb)
// 	test_hget(redisdb)
// 	// test_zadd(redisdb)
// 	test_zremrangebyscore(redisdb)
// 	test_zrangebyscore(redisdb)
// }

// func test_get(redisdb kvdb.KVDb) {
// 	key := "akf"
// 	value, err := redisdb.Get(ctx.Background(), key)
// 	if err != nil {
// 		mlog.Infof("Can't get \"%s\": %v", key, err)
// 	} else {
// 		fmt.Printf("The value of \"%s\" is: %s\n", key, value)
// 	}
// }

// func test_hgetall(redisdb kvdb.KVDb) {
// 	key := "jintian"
// 	value, err := redisdb.HGetAll(ctx.Background(), key)
// 	if err != nil {
// 		mlog.Infof("Can't hgetall \"%s\": %v", key, err)
// 	} else {
// 		fmt.Printf("The result of \"%s\" is below: \n", key)
// 		for i, val := range value {
// 			fmt.Println(i, ":", val)
// 		}
// 	}
// }

// func test_hget(redisdb kvdb.KVDb) {
// 	key := "jintian"
// 	field := "wo"
// 	value, err := redisdb.HGet(ctx.Background(), key, field)
// 	if err != nil {
// 		mlog.Infof("Can't hget: %v", err)
// 	} else {
// 		fmt.Printf("The result of \"%s\" is: %s\n", key, value)
// 		// fmt.Println(len(value))
// 	}
// }

// func test_zrangebyscore(redisdb kvdb.KVDb) {
// 	key := "myos"
// 	min := "0"
// 	max := "10"
// 	value, err := redisdb.ZRangeByScore(ctx.Background(), key, min, max)
// 	if err != nil {
// 		mlog.Infof("Can't exec zrangebyscore: %v", err)
// 	} else {
// 		fmt.Println("The result of \"", key, "\" is: ", value)
// 		// fmt.Printf("%T\n", value)
// 	}
// }

// func test_zadd(redisdb kvdb.KVDb) {
// 	key := "myos"
// 	member1 := kvdb.Z{
// 		Score:  4,
// 		Member: "buntdb",
// 	}

// 	value, err := redisdb.ZAdd(ctx.Background(), key, &member1)
// 	if err != nil {
// 		mlog.Infof("Can't exec zadd: %v", err)
// 	} else {
// 		fmt.Println("The result of zadd is: ", value)
// 	}
// }

// func test_zremrangebyscore(redisdb kvdb.KVDb) {
// 	key := "myos"
// 	min := "1"
// 	max := "3"
// 	value, err := redisdb.ZRemRangeByScore(ctx.Background(), key, min, max)
// 	if err != nil {
// 		mlog.Infof("Can't exec zremrangebyscore: %v", err)
// 	} else {
// 		fmt.Println("The result of zremrangebyscore is: ", value)
// 	}
// }
