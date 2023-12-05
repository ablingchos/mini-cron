package scheduler

import (
	"container/heap"
	"context"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	"git.code.oa.com/red/ms-go/pkg/mlog"
	"git.woa.com/kefuai/my-project/pkg/mypb"
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
	// jobCh     = make(chan map[string]interface{})
)

// 当有新的worker上线时，在调度器中对其进行注册,并为其开启一个心跳watcher
func registWorker(workerURI string) {
	newWorker := &WorkerInfo{
		workerURI: workerURI,
		jobList:   make(map[uint32]bool),
		// jobList:   make([]int32, 0),
	}

	conn, err := grpc.Dial(workerURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		mlog.Error("Can't connect to worker grpc", zap.Error(err))
		return
	}
	grpcClient := mypb.NewJobSchedulerClient(conn)

	go newWatcher(EtcdClient, workerURI)
	// workerWatcher[workerURI] = newWatcher

	workerLock.Lock()
	workerClient[workerURI] = grpcClient
	workerLock.Unlock()

	WorkerManager.mutex.Lock()
	heap.Push(WorkerManager.workerheap, newWorker)
	taskToSchedule.Inc()
	WorkerManager.mutex.Unlock()

	workerNumber.Inc()
	mlog.Infof("New worker %s added to schedule list", newWorker.workerURI)
	newworker <- struct{}{}
}

// 控制取任务
func startFetch() {
	go fetchJob()
	var timer *time.Ticker

	now := time.Now().In(Loc)
	nextScheduleTime := now.Truncate(Interval).Add(Interval - prefetch)

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

// 创建一个新的任务，并将其加入到调度表中
func makeJob(jobNum uint32, jobName string, nextExecTime time.Time, duration time.Duration) *JobInfo {
	job := &JobInfo{
		jobid:        jobNum,
		Jobname:      jobName,
		NextExecTime: nextExecTime,
		Interval:     duration,
	}

	// 添加jobname到[]*JobInfo的映射
	mapLock.Lock()
	jobMap[job.jobid] = &idMap{
		job: job,
	}
	mapLock.Unlock()
	// mapLock.Unlock()

	// 将job信息添加到调度表中
	JobManager.mutex.Lock()
	heap.Push(JobManager.jobheap, job)
	taskToSchedule.Inc()
	JobManager.mutex.Unlock()
	// mlog.Debugf("%s added to heap again, next time: %s, jobid: %d", job.Jobname, job.NextExecTime, job.jobid)

	return job
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
			mlog.Error("Can't Del joblist", zap.Error(err))
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

			numLock.Lock()
			id := jobNum
			jobNum = (jobNum + 1) % (math.MaxInt32 + 1)
			numLock.Unlock()

			makeJob(id, jobname, nextexectime, duration)
		}
		// 如果成功取出了任务，通知调度器开始分派
		if jobNumber > 0 {
			// mlog.Debugf("Get %d jobs", jobNumber)
			newJob <- struct{}{}
		}
	}
}

func deleteJob(job *JobInfo) {
	if job.status == 3 {
		taskDone.Inc()
		if job.reDispatch {
			taskRecovered.Inc()
		}
	}

	mapLock.Lock()
	worker := jobMap[job.jobid].worker
	delete(jobMap, job.jobid)
	mapLock.Unlock()

	worker.mutex.Lock()
	worker.jobnumber--
	delete(worker.jobList, job.jobid)
	worker.mutex.Unlock()
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
	if job.status != 3 && !job.reDispatch {
		taskOverTime.Inc()
		mlog.Debugf("job %s didn't receive result, jobid: %d, status: %d", job.Jobname, job.jobid, job.status)
		numLock.Lock()
		num := jobNum
		incNum()
		numLock.Unlock()

		nextJob := &JobInfo{
			jobid:        num,
			Jobname:      job.Jobname,
			NextExecTime: job.NextExecTime,
			Interval:     job.Interval,
			reDispatch:   true,
		}

		mapLock.Lock()
		jobMap[num] = &idMap{
			job: nextJob,
		}
		mapLock.Unlock()

		go dispatch(nextJob)
		// mlog.Debugf("Redispatch jobid: %d, jobname: %s,begintime: %s", nextJob.jobid, nextJob.Jobname, nextJob.NextExecTime)
	}

	deleteJob(job)
}

func dispatch(job *JobInfo) {
	// 取出worker堆顶的worker信息，其所执行的工作数+1，并重新整堆
	if WorkerManager.workerheap.Len() == 0 {
		mlog.Debugf("No worker joined, stop dispatch")
		JobManager.mutex.Lock()
		job.reDispatch = true
		heap.Push(JobManager.jobheap, job)
		taskToSchedule.Inc()
		JobManager.mutex.Unlock()
		return
	}

	WorkerManager.mutex.Lock()
	worker := (*WorkerManager.workerheap)[0]
	worker.mutex.Lock()
	worker.jobnumber++
	worker.jobList[job.jobid] = true
	worker.mutex.Unlock()

	heap.Fix(WorkerManager.workerheap, 0)
	WorkerManager.mutex.Unlock()
	req := &mypb.JobInfo{
		Jobid:        job.jobid,
		Jobname:      job.Jobname,
		NextExecTime: job.NextExecTime.String(),
		Interval:     job.Interval.String(),
	}

	mapLock.Lock()
	jobMap[job.jobid].worker = worker
	mapLock.Unlock()
	// mlog.Debugf("workload of %s: %d", worker.workerURI, worker.jobnumber)

	// 修改任务状态为1
	job.status = 1
	// grpc派发任务
	go func() {
		_, err := workerClient[worker.workerURI].DispatchJob(context.Background(), &mypb.DispatchJobRequest{
			JobInfo: req,
		})
		if err != nil {
			mlog.Errorf("DispatchJob error", zap.Error(err))
		}
	}()
	// mlog.Debugf("job %s assigned to worker %s, jobid: %d", job.Jobname, worker.workerURI, job.jobid)
	go jobWatcher(job)
}

func incNum() {
	if jobNum == math.MaxUint32 {
		jobNum = 0
	} else {
		jobNum++
	}
}

// 取worker最小堆的最小元素，即最小负载worker，对其进行任务分派
func assignJob() {
	// 等待至少一个worker到达后才能开始分配任务
	for {
		if WorkerManager.workerheap.Len() == 0 {
			mlog.Debugf("No worker joined, scheduler fall asleep")
			<-newworker
			mlog.Debugf("New worker arrived, start to assign job")
		}
		for JobManager.jobheap.Len() > 0 && time.Now().In(Loc).After((*JobManager.jobheap)[0].NextExecTime.Add(-100*time.Microsecond)) {
			// job := heap.Pop(JobManager.jobheap)
			// 获取worker堆的堆顶元素
			// mlog.Debugf("size of the jobheap is: %d", JobManager.jobheap.Len())
			if WorkerManager.workerheap.Len() == 0 {
				mlog.Debugf("No worker joined, scheduler fall asleep")
				break
			}

			JobManager.mutex.Lock()
			job := heap.Pop(JobManager.jobheap).(*JobInfo)
			taskToSchedule.Dec()
			JobManager.mutex.Unlock()
			go dispatch(job)

			nextTime := job.NextExecTime.Add(job.Interval)

			// 如果job是单次任务，则无需判断其下一次循环时间，直接开始分配下一个任务
			if job.Interval == 0 {
				delete(jobList, job.Jobname)
				taskNumber.Dec()
				continue
			}
			if job.reDispatch {
				continue
			}

			switch judgeScheduleTime(nextTime) {
			case 0:
				mlog.Debugf("Job %s time expired, id: %d, nextExecTime: %s", job.Jobname, job.jobid, nextTime.String())
			case 1:
				// 如果该任务的间隔时间小于调度时间，说明其下一次时间已经加入过队列，防止重复添加，直接开始分配下一个任务
				if job.Interval < Interval {
					continue
				}
				numLock.Lock()
				id := jobNum
				incNum()
				numLock.Unlock()

				durationSinceStart := time.Now().In(Loc).Sub(job.NextExecTime)
				intervalSinceStart := float64(durationSinceStart) / float64(job.Interval)
				nextTime = job.NextExecTime.Add(time.Duration(math.Ceil(intervalSinceStart)) * job.Interval)

				switch judgeScheduleTime(nextTime) {
				case 2:
					// nextjob := makeJob(id, job.Jobname, job.NextExecTime.Add((pastIntervals+1)*job.Interval), job.Interval)
					nextjob := makeJob(id, job.Jobname, nextTime, job.Interval)
					mlog.Debugf("id: %d, jobname: %s, begintime: %s, nextExecTime: %s", job.jobid, job.Jobname, job.NextExecTime, nextjob.NextExecTime)
				case 3:
					var i interface{} = job.Jobname
					var tmp interface{} = nextTime.Truncate(time.Second).Format("2006-01-02 15:04:05 -0700 MST")
					err := DbClient.HSet(context.Background(), job.Jobname, field2, tmp)
					if err != nil {
						mlog.Errorf("Failed to change %s time", job.Jobname, zap.Error(err))
						continue
					}
					// mlog.Debugf("%s added to redis, next time: %s", job.Jobname, nextTime)
					_, err = DbClient.RPush(context.Background(), nextTime.Truncate(Interval).String(), i)
					if err != nil {
						mlog.Error("Failed to rpush", zap.Error(err))
						continue
					}
				}
			case 2:
				numLock.Lock()
				id := jobNum
				incNum()
				numLock.Unlock()

				makeJob(id, job.Jobname, nextTime, job.Interval)
			case 3:
				var i interface{} = job.Jobname
				var tmp interface{} = nextTime.Truncate(time.Second).Format("2006-01-02 15:04:05 -0700 MST")
				err := DbClient.HSet(context.Background(), job.Jobname, field2, tmp)
				if err != nil {
					mlog.Errorf("Failed to change %s time", job.Jobname, zap.Error(err))
					continue
				}
				// mlog.Debugf("%s added to redis, next time: %s", job.Jobname, nextTime)
				_, err = DbClient.RPush(context.Background(), nextTime.Truncate(Interval).String(), i)
				if err != nil {
					mlog.Error("Failed to rpush", zap.Error(err))
					continue
				}
			}
		}
		ticker := time.After(100 * time.Microsecond)
		now := time.Now().In(Loc)
		if JobManager.jobheap.Len() > 0 {
			// 如果当前时间在队首job下次执行时间的100ms之前，立即调度，否则等待到100ms前
			if now.Before((*JobManager.jobheap)[0].NextExecTime.Add(-100 * time.Microsecond)) {
				continue
			} else {
				// duration := (*JobManager.jobheap)[0].NextExecTime.Sub(now)
				ticker = time.After((*JobManager.jobheap)[0].NextExecTime.Sub(now) - 100*time.Microsecond)
			}
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
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "Error reading request body", http.StatusInternalServerError)
				mlog.Error("Error reading request body", zap.Error(err))
				return
			}

			data := string(body)

			// 启动处理协程
			go processRequest(&data)
			// mlog.Infof("New request received")
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
	now := time.Now().In(Loc)
	current := time.Now().In(Loc).Truncate(Interval)
	scheduleTime := nextExecTime.Truncate(Interval)

	// 如果job的调度时间离当前时间已经过了Interval，直接丢弃
	if nextExecTime.Add(Interval).Before(now) {
		return 0
	} else if nextExecTime.Before(now) {
		return 1
	} else if scheduleTime == current || now.After(scheduleTime.Add(-1*prefetch-2*time.Second)) {
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
func processRequest(data *string) {
	// job的编号
	// num := 1
	// numLock := sync.Mutex{}
	// go recordJobInfo()

	// 解析请求内容
	parts := strings.Split(*data, ",")
	var cronexpr, begintime_str, method, targetURI string
	cronexpr = parts[0]

	// 解析cron表达式
	schedule, err := cron.Parse(cronexpr)
	if err != nil {
		mlog.Error("Failed to parse cronexpr", zap.Error(err))
		return
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
	// mlog.Debugf("jobname: %s, begintime: %s", jobname, begintime.String())

	if _, ok := jobList[jobname]; !ok {
		taskNumber.Inc()
	}

	scheduleTime := begintime.Truncate(Interval)

	job := &JobInfo{
		Jobname:      jobname,
		NextExecTime: begintime,
		Interval:     duration,
	}
	numLock.Lock()
	job.jobid = jobNum
	incNum()
	numLock.Unlock()

	// 如果job的下一次调度时间在当前时刻之前，直接丢弃掉该任务
	switch judgeScheduleTime(begintime) {
	case 0:
		mlog.Debugf("Job %s time expired", jobname)
		return
	case 1:
		// 立即执行一次，然后把下一次调度信息放入heap中
		mapLock.Lock()
		jobMap[job.jobid] = &idMap{
			job: job,
		}
		mapLock.Unlock()

		JobManager.mutex.Lock()
		heap.Push(JobManager.jobheap, job)
		taskToSchedule.Inc()
		JobManager.mutex.Unlock()

		newJob <- struct{}{}
	case 2:
		mapLock.Lock()
		jobMap[job.jobid] = &idMap{
			job: job,
		}
		mapLock.Unlock()

		JobManager.mutex.Lock()
		heap.Push(JobManager.jobheap, job)
		taskToSchedule.Inc()
		JobManager.mutex.Unlock()

		newJob <- struct{}{}
		// mlog.Debugf("job %s successufully added to JobManager", jobname)
	case 3:
		var i interface{} = jobname
		_, err = DbClient.RPush(context.Background(), scheduleTime.String(), i)
		if err != nil {
			mlog.Error("Failed to rpush", zap.Error(err))
		}
	}
	addToRedis(job)
}

func addToRedis(job *JobInfo) {
	// 将job加入redis表中
	hash := make(map[string]interface{})
	hash[field1] = ""
	hash[field2] = job.NextExecTime.String()
	hash[field3] = job.Interval.String()
	hash["jobname"] = job.Jobname
	err := DbClient.HMSet(context.Background(), job.Jobname, hash)
	if err != nil {
		mlog.Errorf("Failed to hmset")
	}
}

// func recordJobInfo() {
// 	for hash := range jobCh {
// 		jobname := hash["jobname"].(string)
// 		err := DbClient.HMSet(context.Background(), jobname, hash)
// 		if err != nil {
// 			mlog.Error("Failed to hmset", zap.Error(err))
// 			return
// 		}
// 	}
// }

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
