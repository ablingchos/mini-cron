package main

import (
	"time"

	"git.code.oa.com/red/ms-go/pkg/mlog"
	"git.woa.com/kefuai/my-project/internal/pkg/scheduler"
	"go.uber.org/zap"
)

var (
	// // redisURI = flag.String("redisURI", "", "redis uri")
	redisURI           = "redis://user:Aikefu0530@localhost:6379"
	endpoints          = "http://localhost:2379"
	schedulerKey       = "schedulerURI"
	schedulerURI       = "localhost:50051"
	interval           = time.Minute
	workerTimeout      = 4 * time.Second
	schedulerTimeout   = 2 * time.Second
	schedulerHeartbeat = time.Second
	workerHeartbeat    = 2 * time.Second
	loc                *time.Location
	schedulerBackupURI = "localhost:60051"
	// workerURI1   = "localhost:40051"
	// workerURI2   = "localhost:30051"
	// workerURI3   = "localhost:20051"
	// 每次调度的间隔时间
)

func init() {
	// 设置时区
	var err error
	loc, err = time.LoadLocation("Asia/Shanghai")
	if err != nil {
		mlog.Errorf("Failed to set timezone", zap.Error(err))
	}
	mlog.Infof("Set timezone successful")
}

func main() {
	go func() {
		err := scheduler.Initial(redisURI, endpoints, schedulerURI, loc, interval, schedulerHeartbeat, workerTimeout)
		if err != nil {
			mlog.Fatalf("Failed to start scheduler", zap.Error(err))
		}
	}()

	// go func() {
	// 	err := scheduler.InitialBackup(redisURI, endpoints, schedulerBackupURI, loc, interval, schedulerTimeout, workerTimeout)
	// 	if err != nil {
	// 		mlog.Fatalf("Failed to start scheduler", zap.Error(err))
	// 	}
	// }()

	// go func() {
	// err := worker.Initial(redisURI, endpoints, schedulerKey, workerURI1, loc, workerHeartbeat)
	// 	if err != nil {
	// 		mlog.Fatal("Failed to start worker", zap.Error(err))
	// 	}
	// }()
	select {}
}
