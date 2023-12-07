package scheduler

import (
	"container/heap"
	"context"
	"strconv"
	"strings"
	"time"

	"git.code.oa.com/red/ms-go/pkg/mlog"
	"git.woa.com/kefuai/my-project/internal/pkg/myetcd"
	"git.woa.com/kefuai/my-project/pkg/mypb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func checkSchedulerTimeout(hw *myetcd.HeartbeatWatcher) {
	<-hw.Offline
	mlog.Infof("Scheduler crash, begin to take over")

	// 更新schedulerURI的值，防止后续加入的worker发送消息错误
	_, err := EtcdClient.Put(context.Background(), schedulerKey, schedulerBackupURI)
	if err != nil {
		mlog.Fatalf("Failed to update schedulerURI", zap.Error(err))
		return
	}

	// 获取jobNumber的值
	number, err := dbClient.Get(context.Background(), "jobnumber")
	if err != nil {
		mlog.Fatal("Failed to get number from redis", zap.Error(err))
		return
	}
	idNum, _ := strconv.Atoi(number)
	jobNumber = uint32(idNum)

	// 启动grpc服务
	go startSchedulerGrpc()

	initPrometheus(":4397")

	// 启动httplistener
	go httpListener(":8081")

	// 启动job调度
	JobManager = &jobManager{
		jobheap: &jobHeap{},
	}

	// 启动worker调度
	WorkerManager = &workerManager{
		workerheap: &workerHeap{},
	}

	go startFetch()
	go informWorker()
	go assignJob()
	go recoverJobManager()
	mlog.Infof("Back-up Scheduler %s initial successfully, start to work", schedulerBackupURI)
}

// 通知worker新的schedulerURI，并将其加入WorkerManager
func informWorker() {
	resp, err := dbClient.HGetAll(context.Background(), "worker")
	if err != nil {
		mlog.Errorf("Failed to get worker from redis", zap.Error(err))
	}

	for workerURI := range resp {
		go func(workerURI string) {
			conn, err := grpc.Dial(workerURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				mlog.Errorf("Failed to dial worker %s", workerURI, zap.Error(err))
				return
			}

			client1 := mypb.NewJobSchedulerClient(conn)

			client2 := mypb.NewSchedulerSwitchClient(conn)
			_, err = client2.NewScheduler(context.Background(), &mypb.SchedulerSwitchRequest{
				SchedulerURI: schedulerBackupURI,
			})
			if err != nil {
				mlog.Errorf("Failed to infom worker %s", workerURI, zap.Error(err))
				return
			}

			workerLock.Lock()
			workerClient[workerURI] = client1
			workerLock.Unlock()

			newWorker := &WorkerInfo{
				workerURI: workerURI,
				jobList:   make(map[uint32]bool),
			}

			WorkerManager.mutex.Lock()
			heap.Push(WorkerManager.workerheap, newWorker)
			taskToSchedule.Inc()
			WorkerManager.mutex.Unlock()

			workerNumber.Inc()
			mlog.Infof("New worker %s added to schedule list", newWorker.workerURI)
			newworker <- struct{}{}

			hw := myetcd.NewWatcher(EtcdClient, workerURI, workerTimeout)
			go checkWorkerTimeout(hw)
			workerNumber.Inc()
		}(workerURI)
	}
}

func recoverJobManager() {
	resp, err := dbClient.HGetAll(context.Background(), jobHash)
	if err != nil {
		mlog.Fatalf("Failed to recover job from redis", zap.Error(err))
	}

	for id, info := range resp {
		go func(id, info string) {
			parts := strings.Split(info, ",")
			jobname := parts[0]
			timeStr := parts[1]

			nextExecTime, err := time.ParseInLocation(storeLayout, timeStr, Loc)
			if err != nil {
				mlog.Error("Failed to parse time", zap.Error(err))
				return
			}

			durationstr, err := dbClient.HGet(context.Background(), jobname, field3)
			if err != nil {
				mlog.Errorf("Failed to get %s from redis", jobname, zap.Error(err))
				return
			}

			duration, err := time.ParseDuration(durationstr)
			if err != nil {
				mlog.Errorf("Failed to parse %s duration", jobname, zap.Error(err))
				return
			}

			idNum, _ := strconv.Atoi(id)

			now := time.Now().In(Loc)

			if nextExecTime.Before(now) {
				job := &JobInfo{
					jobid:        uint32(idNum),
					Jobname:      jobname,
					NextExecTime: nextExecTime,
					Interval:     duration,
				}
				go jobWatch(job)
			} else {
				makeJob(uint32(idNum), jobname, nextExecTime, duration)
				newJob <- struct{}{}
			}
		}(id, info)
	}
}
