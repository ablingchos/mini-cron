package scheduler

import (
	"container/heap"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"git.code.oa.com/red/ms-go/pkg/mlog"
	"github.com/ablingchos/my-project/pkg/mypb"
	"github.com/robfig/cron"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	field1    = "method"
	field2    = "nextexectime"
	field3    = "Interval"
	newworker = make(chan struct{})
	fetchCh   = make(chan time.Time)
	layout    = "2006-01-02,15:04:05"
	prefetch  time.Duration
)

// 当有新的worker上线时，在调度器中对其进行注册,并为其开启一个心跳watcher
func registWorker() {
	for workerURI := range newWorker {
		newWorker := &WorkerInfo{
			workerURI: workerURI,
			jobnumber: 0,
			jobList:   make(map[int32]bool),
			status:    "online",
			// jobList:   make([]int32, 0),
		}

		conn, err := grpc.Dial(workerURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			mlog.Error("Can't connect to worker grpc", zap.Error(err))
			continue
		}
		grpcclient := mypb.NewJobSchedulerClient(conn)

		workermu.Lock()
		WorkerManager.mutex.Lock()
		go newWatcher(EtcdClient, workerURI)
		// workerWatcher[workerURI] = newWatcher

		mlog.Infof("New worker %s added to schedule list", newWorker.workerURI)
		workerClient[workerURI] = grpcclient
		heap.Push(WorkerManager.workerheap, newWorker)
		newworker <- struct{}{}

		WorkerManager.mutex.Unlock()
		workermu.Unlock()
	}
}

// 控制取任务
func startFetch() {
	go fetchJob()
	var timer *time.Ticker

	now := time.Now().In(Loc)
	nextScheduleTime := now.Truncate(Interval).Add(Interval - prefetch)
	// mlog.Debugf("nextScheduleTime: %s, prefetch: %s, Interval: %s", nextScheduleTime.String(), prefetch.String(), Interval.String())
	// if now.After(nextScheduleTime) {
	// 	nextScheduleTime = nextScheduleTime.Add(Interval)
	// }

	// time.Sleep(time.Until(nextScheduleTime))
	// timer = time.NewTicker(Interval)
	// fetchCh <- nextScheduleTime

	// 下面的代码功能是用来规划调度器首次调度的开始时间，并从此时间开始初始化定时器
	// 如果时间用调度尺度换算后大于下一次的调度时间，立即开始调度
	if nextScheduleTime.Before(now) {
		fetchCh <- nextScheduleTime.Add(prefetch)
		nextScheduleTime = nextScheduleTime.Add(Interval)
	}
	time.Sleep(time.Until(nextScheduleTime))
	fetchCh <- nextScheduleTime.Add(prefetch)
	nextScheduleTime = nextScheduleTime.Add(Interval).Add(prefetch)
	timer = time.NewTicker(Interval)

	for range timer.C {
		// mlog.Debugf("Fetch once")
		fetchCh <- nextScheduleTime
		nextScheduleTime = nextScheduleTime.Add(Interval)
	}
}

// 每隔Interval时间，从db中取出下一个周期将要执行的任务存入调度器内存中
func fetchJob() {
	// 在第一次开始取job前，休眠到begintime-Interval/10的时间，再开始启动取job的工作
	for scheduleTime := range fetchCh {
		// 同一个时间段（1分钟）的任务id放在同一个list中，所以直接全部取出
		// now := time.Now().In(Loc).Truncate(Interval).Add(Interval)
		// mlog.Debugf("Start fetch time %s", scheduleTime.String())
		resp, err := DbClient.LRange(context.Background(), scheduleTime.String(), 0, -1)
		if err != nil {
			mlog.Error("Can't get from db", zap.Error(err))
			continue
		}

		// 取出该scheduTime的所有任务后删除这个list，回收无效数据
		_, err = DbClient.Del(context.Background(), scheduleTime.String())
		if err != nil {
			mlog.Error("Can't DEL joblist", zap.Error(err))
		}

		jobNumber := len(resp.([]string))
		// 根据任务id，从hashmap中获取所有任务的任务信息
		for _, jobname := range resp.([]string) {
			ret, err := DbClient.HMGet(context.Background(), jobname, field1, field2, field3)
			if err != nil {
				mlog.Error("Can't get from db", zap.Error(err))
				return
			}

			// 把string解析成time类型
			nextexectime, err := time.ParseInLocation("2006-01-02 15:04:05 -0700 MST", ret[1], Loc)
			if err != nil {
				mlog.Error("Failed to parse time", zap.Error(err))
				continue
			}
			duration, err := time.ParseDuration(ret[2])
			if err != nil {
				mlog.Infof("Failed to parse duration", zap.Error(err))
				continue
			}

			job := &JobInfo{
				Jobname:      jobname,
				NextExecTime: nextexectime,
				Interval:     duration,
				// Operation:    ret[],
			}

			numLock.Lock()
			job.jobid = jobNum
			jobNum++
			numLock.Unlock()

			// 添加jobname到[]*JobInfo的映射
			mapLock.Lock()
			jobMap[job.jobid] = &idMap{
				job: job,
			}
			mapLock.Unlock()
			//  mlog.Debugf("fetch job: %s, nextExecTime: %s, duration: %s", job.Jobname, job.NextExecTime, job.Interval)

			// 将job信息添加到调度表中
			JobManager.mutex.Lock()
			heap.Push(JobManager.jobheap, job)
			JobManager.mutex.Unlock()
		}
		// 如果成功取出了任务，通知调度器开始分派
		if jobNumber > 0 {
			// mlog.Debugf("Get %d jobs", jobNumber)
			newJob <- struct{}{}
		}
	}
}

func deleteJob(job *JobInfo) {
	mapLock.Lock()
	worker := jobMap[job.jobid].worker
	worker.mutex.Lock()
	worker.jobnumber--
	delete(worker.jobList, job.jobid)
	worker.mutex.Unlock()
	delete(jobMap, job.jobid)
	mapLock.Unlock()
}

func jobWatcher(job *JobInfo) {
	// mlog.Debugf("now: %s, nextexectime: %s", time.Now().In(Loc), job.NextExecTime)
	var interval time.Duration
	if job.Interval == 0 {
		interval = Interval
	} else {
		interval = job.Interval
	}
	timer := time.NewTimer(interval / 2)

	<-timer.C
	if job.status != 3 {
		mlog.Debugf("job %s didn't receive result, begintime: %s, status: %d", job.Jobname, job.NextExecTime.String(), job.status)
		numLock.Lock()
		num := jobNum
		jobNum++
		numLock.Unlock()
		go dispatch(&JobInfo{
			jobid:        num,
			Jobname:      job.Jobname,
			NextExecTime: time.Now().In(Loc),
			Interval:     job.Interval,
		})
	}
	go deleteJob(job)
}

func dispatch(job *JobInfo) {
	// 取出worker堆顶的worker信息，其所执行的工作数+1，并重新整堆
	WorkerManager.mutex.Lock()
	worker := (*WorkerManager.workerheap)[0]
	worker.jobnumber = worker.jobnumber + 1
	mlog.Debugf("workload of %s: %d", worker.workerURI, worker.jobnumber)
	heap.Fix(WorkerManager.workerheap, 0)
	req := &mypb.JobInfo{
		Jobid:        job.jobid,
		Jobname:      job.Jobname,
		NextExecTime: job.NextExecTime.String(),
		Interval:     job.Interval.String(),
	}

	// 将job添加到worker执行的任务列表中
	// worker.jobList = append(worker.jobList, job.jobid)
	worker.mutex.Lock()
	worker.jobList[job.jobid] = true
	worker.mutex.Unlock()
	WorkerManager.mutex.Unlock()
	mapLock.Lock()
	jobMap[job.jobid].worker = worker
	mapLock.Unlock()

	// 修改任务状态为1
	job.status = 1
	// grpc派发任务
	go workerClient[worker.workerURI].DispatchJob(context.Background(), &mypb.DispatchJobRequest{
		JobInfo: req,
	})
	mlog.Debugf("job %s assigned to worker %s, jobid: %d", job.Jobname, worker.workerURI, job.jobid)
	go jobWatcher(job)

}

// 取worker最小堆的最小元素，即最小负载worker，对其进行任务分派
func assignJob() {
	// 等待至少一个worker到达后才能开始分配任务
	<-newworker
	mlog.Debugf("New worker arrived, start to assign job")
	for {
		// mlog.Debugf("Start to work")
		// mlog.Debugf("size of jobheap is: %d", len(*JobManager.jobheap))
		// for len(*JobManager.jobheap) > 0 && now.After((*JobManager.jobheap)[0].NextExecTime.Add(-Interval/20)) {
		for len(*JobManager.jobheap) > 0 && time.Now().In(Loc).After((*JobManager.jobheap)[0].NextExecTime.Add(100*time.Microsecond)) {
			// job := heap.Pop(JobManager.jobheap)
			// 获取worker堆的堆顶元素
			// mlog.Debugf("size of the jobheap is: %d", len(*JobManager.jobheap))
			WorkerManager.mutex.Lock()
			if len(*WorkerManager.workerheap) == 0 {
				mlog.Debugf("No worker joined, scheduler fall asleep")
				<-newWorker
				mlog.Debugf("New worker arrived, start to assign job")
			}
			WorkerManager.mutex.Unlock()

			JobManager.mutex.Lock()
			job := heap.Pop(JobManager.jobheap).(*JobInfo)
			dispatch(job)

			nextTime := job.NextExecTime.Add(job.Interval)

			// 如果job是单次任务，则无需判断其下一次循环时间，直接开始分配下一个任务
			if job.Interval == 0 {
				continue
			}

			switch judgeScheduleTime(nextTime) {
			case 2:
				insertJob := &JobInfo{
					Jobname:      job.Jobname,
					NextExecTime: nextTime,
					Interval:     job.Interval,
				}
				numLock.Lock()
				insertJob.jobid = jobNum
				jobNum++
				numLock.Unlock()

				mapLock.Lock()
				jobMap[insertJob.jobid] = &idMap{
					job: insertJob,
				}
				mapLock.Unlock()
				heap.Push(JobManager.jobheap, insertJob)
				// mlog.Debugf("%s added to heap again, next time: %s, jobid: %d", job.Jobname, nextTime, job.jobid)
				// mlog.Debugf("Length of jobheap is: %d", len(*JobManager.jobheap))
			case 3:
				var i interface{} = job.Jobname
				var tmp interface{} = nextTime.Truncate(time.Second).Format("2006-01-02 15:04:05 -0700 MST")
				err := DbClient.HSet(context.Background(), job.Jobname, field2, tmp)
				if err != nil {
					mlog.Errorf("Failed to change %s time", job.Jobname, zap.Error(err))
				}
				// mlog.Debugf("%s added to redis, next time: %s", job.Jobname, nextTime)
				_, err = DbClient.RPush(context.Background(), nextTime.Truncate(Interval).String(), i)
				if err != nil {
					mlog.Error("Failed to rpush", zap.Error(err))
				}
			}
			JobManager.mutex.Unlock()

			// ticker := time.After(Interval)
			// if len(*JobManager.jobheap) > 0 {
			// 	ticker = time.After((*JobManager.jobheap)[0].NextExecTime.Sub(time.Now().In(Loc)) - time.Second)
			// }
			// <-ticker
		}
		ticker := time.After(200 * time.Microsecond)
		if len(*JobManager.jobheap) > 0 {
			ticker = time.After((*JobManager.jobheap)[0].NextExecTime.Sub(time.Now().In(Loc)) + 200*time.Microsecond)
		}
		select {
		case <-ticker:
		case <-newJob:
		}
	}
}

// 监听客户端的http请求
// ScheduleScale代表任务调度时精确到哪个尺度，一般是time.Second
func httpListener() {
	waitch := make(chan struct{})
	mu := sync.Mutex{}
	var data string
	go processRequest(waitch, &mu, &data)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "Error reading request body", http.StatusInternalServerError)
				mlog.Error("Error reading request body", zap.Error(err))
				return
			}

			mu.Lock()
			data = string(body)
			mu.Unlock()

			// 唤醒处理协程
			waitch <- struct{}{}
			mlog.Infof("New request received")
		}
	})

	mlog.Infof("httplistener start on port: 8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		mlog.Fatal("Httplistener err", zap.Error(err))
	}
}

func judgeScheduleTime(nextExecTime time.Time) int {
	// 判断job的执行时间是否在当前调度时间内
	// 如果在，将job加入小根堆中，直接开始准备调度
	now := time.Now().In(Loc).Truncate(Interval)
	scheduleTime := nextExecTime.Truncate(Interval)
	// 如果job的调度时间离当前时间已经过了Interval，直接丢弃
	if nextExecTime.Add(Interval).Before(now) {
		return 0
	} else if nextExecTime.Before(time.Now()) {
		return 1
	} else if scheduleTime == now || time.Now().In(Loc).After(scheduleTime.Add(-1*prefetch)) {
		// 如果该job的下一次调度时间段在当前时间段内，或者其所在的时间段的job已经取出了，直接将其加入JobManager中
		return 2
	} else {
		// 该job的下一次调度时间段在当前需要调度的时间段之外，放入redis中等待调度
		return 3
	}
}

// 客户端的请求格式：
// curl -X POST -d "5 * * * * *,2023-11-16,15:00:00,get,http://Localhost:8080/"
// 时间参数也可以缺省，默认从time.Now()时间开始
// job的存储格式：
// jobname ： get@www.baidu.com@5s

// 解析客户端的http请求内容
func processRequest(waitch <-chan struct{}, mu *sync.Mutex, data *string) {
	// job的编号
	// num := 1
	// numLock := sync.Mutex{}
	jobCh := make(chan map[string]interface{})
	go recordJobInfo(jobCh)

	for range waitch {
		mu.Lock()
		content := *data
		mu.Unlock()

		// 解析请求内容
		parts := strings.Split(content, ",")
		var cronexpr, begintime_str, method, targetURI string
		cronexpr = parts[0]

		// 解析cron表达式
		schedule, err := cron.Parse(cronexpr)
		if err != nil {
			mlog.Error("Failed to parse cronexpr", zap.Error(err))
			mu.Lock()
			*data = ""
			mu.Unlock()
			continue
		}

		Loc, _ := time.LoadLocation("Asia/Shanghai")
		randomtime := time.Date(2025, 1, 1, 0, 0, 0, 0, Loc)
		nexttime := schedule.Next(randomtime)
		duration := nexttime.Sub(randomtime)

		// 开始时间为空时，默认为time.Now()+duration时刻开始调度
		if len(parts) == 3 {
			// begintime_str = time.Now().Add(duration).Format(layout)
			begintime_str = time.Now().Format(layout)
			method = parts[1]
			targetURI = parts[2]
		} else {
			begintime_str = parts[1] + "," + parts[2]
			method = parts[3]
			targetURI = parts[4]
		}

		// 将begintime解析为time.Time格式
		begintime, err := time.ParseInLocation(layout, begintime_str, Loc)
		if err != nil {
			mlog.Error("Failed to parse begintime", zap.Error(err))
			return
		}

		jobname := method + "@" + targetURI + "@" + duration.String()
		mlog.Debugf("jobname: %s, begintime: %s", jobname, begintime.String())

		scheduleTime := begintime.Truncate(Interval)

		job := &JobInfo{
			Jobname:      jobname,
			NextExecTime: begintime,
			Interval:     duration,
		}
		numLock.Lock()
		job.jobid = jobNum
		jobNum++
		numLock.Unlock()

		// 如果job的下一次调度时间在当前时刻之前，直接丢弃掉该任务
		switch judgeScheduleTime(begintime) {
		case 0:
			mlog.Debugf("Job %s time expired", jobname)
			continue
		case 1:
			// 立即执行一次
			mapLock.Lock()
			jobMap[job.jobid] = &idMap{
				job: job,
			}
			mapLock.Unlock()
			dispatch(job)
			// mlog.Debugf("Immediately do job %s", jobname)
			// 计算任务从开始时间到现在过去了多少个interval
			pastIntervals := time.Now().In(Loc).Sub(job.NextExecTime) / job.Interval
			// 计算任务的正确的下一次执行时间
			// job.NextExecTime = job.NextExecTime.Add((pastIntervals + 1) * job.Interval)

			nextjob := &JobInfo{
				Jobname:      jobname,
				NextExecTime: job.NextExecTime.Add((pastIntervals + 1) * job.Interval),
				Interval:     duration,
			}
			numLock.Lock()
			nextjob.jobid = jobNum
			jobNum++
			numLock.Unlock()
			mapLock.Lock()
			jobMap[nextjob.jobid] = &idMap{
				job: nextjob,
			}
			mapLock.Unlock()

			JobManager.mutex.Lock()
			heap.Push(JobManager.jobheap, nextjob)
			mlog.Debugf("Add to jobheap, nextTime: %s", nextjob.NextExecTime.String())
			newJob <- struct{}{}
			JobManager.mutex.Unlock()

		case 2:
			JobManager.mutex.Lock()
			heap.Push(JobManager.jobheap, job)
			mapLock.Lock()
			jobMap[job.jobid] = &idMap{
				job: job,
			}
			mapLock.Unlock()
			JobManager.mutex.Unlock()
			newJob <- struct{}{}
			mlog.Debugf("job %s successufully added to JobManager", jobname)
		case 3:
			var i interface{} = jobname
			_, err = DbClient.RPush(context.Background(), scheduleTime.String(), i)
			if err != nil {
				mlog.Error("Failed to rpush", zap.Error(err))
			}
		}

		// 将job加入redis表中
		hash := make(map[string]interface{})
		hash[field1] = method
		hash[field2] = begintime.String()
		hash[field3] = duration.String()
		hash["jobname"] = jobname
		jobCh <- hash

		//清空data
		mu.Lock()
		*data = ""
		mu.Unlock()

	}
}

func recordJobInfo(jobCh <-chan map[string]interface{}) {
	for hash := range jobCh {
		jobname := hash["jobname"].(string)
		err := DbClient.HMSet(context.Background(), jobname, hash)
		if err != nil {
			mlog.Error("Failed to hmset", zap.Error(err))
			return
		}
	}
}

// 记录job的执行结果（已交给worker来记录）
// func RecordResult(resultCh <-chan []string) {
// 	for execResult := range resultCh {
// 		jobname := execResult[0]
// 		executeResult := execResult[1]
// 		jobname = "result/" + jobname

// 		length, err := DbClient.LLen(context.Background(), executeResult)
// 		if err != nil {
// 			mlog.Error("", zap.Error(err))
// 		}
// 		if length == 5 {
// 			_, err = DbClient.LPop(context.Background(), executeResult)
// 			if err != nil {
// 				mlog.Error("", zap.Error(err))
// 			}
// 		}
// 		_, err = DbClient.RPush(context.Background(), jobname, executeResult)
// 		if err != nil {
// 			mlog.Error("", zap.Error(err))
// 		}

// 		mlog.Debugf("%s: %s", jobname, executeResult)
// 	}
// }

// 取出jobname的最新执行结果
// func FetchLatestResult(jobname string) (string, error) {
// 	resp, err := DbClient.LRange(context.Background(), jobname, -1, -1)
// 	if err != nil {
// 		return "", err
// 	}
// 	return resp.(string), err
// }
