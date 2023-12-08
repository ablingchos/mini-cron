package scheduler

import (
	"context"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"sync"
	"time"

	"git.code.oa.com/red/ms-go/pkg/mlog"
	"git.woa.com/kefuai/my-project/pkg/mypb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	// "git.woa.com/kefuai/my-project/internal/pkg/myetcd"
)

var (
	workerMap     = make(map[string]*WorkerInfo)
	workerMapLock sync.Mutex
	workerDone    = make(chan struct{})
)

func checkSchedulerTimeout(hw *heartbeatWatcher) {
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
	mlog.Debugf("Recovered jobNumber %d", jobNumber)

	// 启动grpc服务
	go startSchedulerGrpc()

	// 启动pprof服务
	go func() {
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			mlog.Errorf("Http server error", zap.Error(err))
		}
	}()

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

	go recoverFetch()
	go recoverWorker()
	go assignJob()
	recoverJobManager()
	mlog.Infof("Back-up Scheduler %s initial successfully, start to work", schedulerBackupURI)
}

func recoverFetch() {
	now := time.Now().In(Loc)
	nextScheduleTime := now.Truncate(Interval).Add(Interval - prefetch)
	var timer *time.Ticker

	resp, err := dbClient.LRange(context.Background(), nextScheduleTime.String(), 0, -1)
	if err != nil {
		mlog.Errorf("Failed to get from db", zap.Error(err))
	}
	if len(resp.([]string)) != 0 {
		fetchCh <- nextScheduleTime.Add(prefetch)
		nextScheduleTime = nextScheduleTime.Add(Interval)
	}
	time.Sleep(time.Until(nextScheduleTime))
	fetchCh <- nextScheduleTime.Add(prefetch)
	nextScheduleTime = nextScheduleTime.Add(Interval + prefetch)
	timer = time.NewTicker(Interval)

	for range timer.C {
		fetchCh <- nextScheduleTime
		nextScheduleTime = nextScheduleTime.Add(Interval)
	}
}

// 通知worker新的schedulerURI，并将其加入WorkerManager
func recoverWorker() {
	resp, err := dbClient.HGetAll(context.Background(), field4)
	if err != nil {
		mlog.Errorf("Failed to get worker from redis", zap.Error(err))
		return
	}

	for workerURI := range resp {
		// go func(workerURI string) {
		// 	newWorker := &WorkerInfo{
		// 		workerURI: workerURI,
		// 		jobList:   make(map[uint32]bool),
		// 		online:    true,
		// 	}
		mlog.Debugf("Worker %s begin to solve", workerURI)
		conn, err := grpc.Dial(workerURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			mlog.Errorf("Failed to dial worker %s", workerURI, zap.Error(err))
			continue
		}

		// client1 := mypb.NewJobSchedulerClient(conn)

		client := mypb.NewSchedulerSwitchClient(conn)
		_, err = client.NewScheduler(context.Background(), &mypb.SchedulerSwitchRequest{
			SchedulerURI: schedulerBackupURI,
		})
		if err != nil {
			mlog.Errorf("Failed to infom worker %s", workerURI, zap.Error(err))
			continue
		}

		worker := registWorker(workerURI)
		workerMapLock.Lock()
		workerMap[workerURI] = worker
		workerMapLock.Unlock()
		mlog.Debugf("Worker %s solved", workerURI)
	}

	mlog.Debugf("111")
	workerDone <- struct{}{}
	mlog.Debugf("Workerdone")
}

func recoverJobManager() {
	<-workerDone
	mlog.Debugf("Begin to recover job")

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

				var worker *WorkerInfo
				workerMapLock.Lock()
				if _, ok = workerMap[resp[field4]]; !ok {
					mlog.Errorf("Failed to get worker %s", resp[field4], zap.Error(err))
					workerMapLock.Unlock()
					return
				}
				worker = workerMap[resp[field4]]
				workerMapLock.Unlock()

				mapLock.Lock()
				jobMap[jobid] = &idMap{
					job:    job,
					worker: worker,
				}
				mapLock.Unlock()

				worker.mutex.Lock()
				worker.jobnumber++
				worker.jobList[jobid] = true
				worker.mutex.Unlock()

				go jobWatch(job)
				mlog.Debugf("Start to watch job %d", jobid)
			} else {
				makeJob(jobid, resp[field5], nextExecTime, duration)
				newJob <- struct{}{}
				mlog.Debugf("Job %d added to jobmanager", jobid)
			}
		}(id)
	}
}
