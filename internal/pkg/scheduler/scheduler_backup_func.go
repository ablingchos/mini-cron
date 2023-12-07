package scheduler

import (
	"context"
	"strconv"
	"sync"
	"time"

	"git.code.oa.com/red/ms-go/pkg/mlog"
	"git.woa.com/kefuai/my-project/internal/pkg/myetcd"
	"git.woa.com/kefuai/my-project/pkg/mypb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	workerMap     = make(map[string]*WorkerInfo)
	workerMapLock sync.Mutex
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
	recoverWorker()
	go assignJob()
	recoverJobManager()
	mlog.Infof("Back-up Scheduler %s initial successfully, start to work", schedulerBackupURI)
}

// 通知worker新的schedulerURI，并将其加入WorkerManager
func recoverWorker() {
	resp, err := dbClient.HGetAll(context.Background(), field4)
	if err != nil {
		mlog.Errorf("Failed to get worker from redis", zap.Error(err))
	}

	for workerURI := range resp {
		go func(workerURI string) {
			// 	newWorker := &WorkerInfo{
			// 		workerURI: workerURI,
			// 		jobList:   make(map[uint32]bool),
			// 		online:    true,
			// 	}

			conn, err := grpc.Dial(workerURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				mlog.Errorf("Failed to dial worker %s", workerURI, zap.Error(err))
				return
			}
			defer conn.Close()

			// client1 := mypb.NewJobSchedulerClient(conn)

			client := mypb.NewSchedulerSwitchClient(conn)
			_, err = client.NewScheduler(context.Background(), &mypb.SchedulerSwitchRequest{
				SchedulerURI: schedulerBackupURI,
			})
			if err != nil {
				mlog.Errorf("Failed to infom worker %s", workerURI, zap.Error(err))
				return
			}

			registWorker(workerURI)

			// hw := myetcd.NewWatcher(EtcdClient, workerURI, workerTimeout)
			// go checkWorkerTimeout(hw)
			// workerNumber.Inc()

			// workerLock.Lock()
			// workerClient[workerURI] = client1
			// workerLock.Unlock()

			// workerMapLock.Lock()
			// workerMap[workerURI] = newWorker
			// workerMapLock.Unlock()

			// WorkerManager.mutex.Lock()
			// heap.Push(WorkerManager.workerheap, newWorker)
			// taskToSchedule.Inc()
			// WorkerManager.mutex.Unlock()

			// workerNumber.Inc()
			// mlog.Infof("New worker %s added to schedule list", newWorker.workerURI)
			// newworker <- struct{}{}
		}(workerURI)
	}
	time.Sleep(2 * time.Second)
}

func recoverJobManager() {
	resp, err := dbClient.HGetAll(context.Background(), field6)
	if err != nil {
		mlog.Fatalf("Failed to recover job from redis", zap.Error(err))
	}

	for id := range resp {
		go func(id string) {
			idstr := "job" + id
			tmp, err := strconv.ParseUint(id, 10, 32)
			if err != nil {
				mlog.Errorf("Failed to transfer to uint", zap.Error(err))
				return
			}
			jobid := uint32(tmp)

			resp, err := dbClient.HGetAll(context.Background(), idstr)
			if err != nil {
				mlog.Errorf("Failed to get %s", idstr, zap.Error(err))
				return
			}

			nextExecTime, err := time.ParseInLocation(storeLayout, resp[field2], Loc)
			if err != nil {
				mlog.Error("Failed to parse time", zap.Error(err))
				return
			}
			// durationstr, err := dbClient.HGet(context.Background(), jobname, field3)
			// if err != nil {
			// 	mlog.Errorf("Failed to get %s from redis", jobname, zap.Error(err))
			// 	return
			// }

			duration, err := time.ParseDuration(resp[field3])
			if err != nil {
				mlog.Errorf("Failed to parse duration", zap.Error(err))
				return
			}

			// idNum, _ := strconv.Atoi(id)

			// now := time.Now().In(Loc)
			// 如果job已经被分配给了一个worker
			if _, ok := resp[field4]; ok {
				job := &JobInfo{
					jobid:        jobid,
					Jobname:      resp[field5],
					NextExecTime: nextExecTime,
					Interval:     duration,
				}
				mapLock.Lock()
				worker := workerMap[resp[field4]]
				workerMapLock.Lock()
				jobMap[jobid] = &idMap{
					job:    job,
					worker: worker,
				}
				workerMapLock.Unlock()
				mapLock.Unlock()

				worker.mutex.Lock()
				worker.jobnumber++
				worker.jobList[jobid] = true
				worker.mutex.Unlock()

				go jobWatch(job)
			} else {
				makeJob(jobid, resp[field5], nextExecTime, duration)
				newJob <- struct{}{}
			}
		}(id)
	}
}
