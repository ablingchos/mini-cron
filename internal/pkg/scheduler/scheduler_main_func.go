package scheduler

import (
	"container/heap"
	"context"
	"io"
	"math"
	"net/http"
	"strconv"
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
	field1      = "method"
	field2      = "nextexectime"
	field3      = "Interval"
	field4      = "worker"
	field5      = "jobname"
	field6      = "joblist"
	fetchCh     = make(chan time.Time)
	parseLayout = "2006-01-02,15:04:05"
	storeLayout = "2006-01-02 15:04:05 -0700 MST"
	prefetch    time.Duration
)

// 当worker超时未答复时，将其从worker堆中删除
func checkWorkerTimeout(hw *heartbeatWatcher) {
	<-hw.Offline

	WorkerManager.mutex.Lock()
	for i, value := range *WorkerManager.workerheap {
		str := hw.Key
		index := strings.LastIndex(hw.Key, "/")
		str = strings.TrimPrefix(str, str[:index+1])

		if value.workerURI == str {
			// err := dbClient.HDel(context.Background(), field4, str)
			// if err != nil {
			// 	mlog.Errorf("Failed to HDEL %s from redis", str, zap.Error(err))
			// }
			value.online = false
			err := dbClient.HDel(context.Background(), field4, str)
			if err != nil {
				mlog.Errorf("Failed to remove worker %s from redis", str, zap.Error(err))
			}
			heap.Remove(WorkerManager.workerheap, i)
			delete(workerClient, str)
			mlog.Infof("worker %s offline, removed from worker list", str)
			workerNumber.Dec()
			break
		}
	}
	WorkerManager.mutex.Unlock()
}

// 当有新的worker上线时，在调度器中对其进行注册,并为其开启一个心跳watcher
func registWorker(workerURI string) *WorkerInfo {
	newWorker := &WorkerInfo{
		workerURI: workerURI,
		jobList:   make(map[uint32]bool),
		online:    true,
		// jobList:   make([]int32, 0),
	}

	mlog.Debugf("before grpc.dial")
	conn, err := grpc.Dial(workerURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		mlog.Error("Can't connect to worker grpc", zap.Error(err))
		return nil
	}
	grpcClient := mypb.NewJobSchedulerClient(conn)

	var tmp interface{} = "online"
	err = dbClient.HSet(context.Background(), field4, workerURI, tmp)
	if err != nil {
		mlog.Error("Can't set workerURI to redis", zap.Error(err))
	}

	hw := newWatcher(EtcdClient, workerURI, workerTimeout)
	hw.worker = newWorker
	go checkWorkerTimeout(hw)
	// workerWatcher[workerURI] = newWatcher

	mlog.Debugf("before workerLock")
	workerLock.Lock()
	workerClient[workerURI] = grpcClient
	workerLock.Unlock()

	mlog.Debugf("before WorkerManager.mutex.Lock")
	WorkerManager.mutex.Lock()
	heap.Push(WorkerManager.workerheap, newWorker)
	WorkerManager.mutex.Unlock()
	mlog.Debugf("after WorkerManager.mutex.Lock")

	workerNumber.Inc()

	mlog.Debugf("before newworker")
	select {
	case newworker <- struct{}{}:
	default:
	}
	mlog.Infof("New worker %s added to schedule list", newWorker.workerURI)

	return newWorker
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

// 每隔Interval时间，从db中取出下一个周期将要执行的任务存入调度器内存中
func fetchJob() {
	// 在第一次开始取job前，休眠到begintime-Interval/10的时间，再开始启动取job的工作
	for scheduleTime := range fetchCh {
		// 同一个时间段（1分钟）的任务id放在同一个list中，所以直接全部取出
		// now := time.Now().In(Loc).Truncate(Interval).Add(Interval)
		mlog.Debugf("Start fetch time %s", scheduleTime.String())
		resp, err := dbClient.LRange(context.Background(), scheduleTime.String(), 0, -1)
		if err != nil {
			mlog.Error("Can't get from db", zap.Error(err))
			continue
		}
		// 取出该scheduTime的所有任务后删除这个list，回收无效数据
		_, err = dbClient.Del(context.Background(), scheduleTime.String())
		if err != nil {
			mlog.Error("Can't Del joblist", zap.Error(err))
		}

		jobNum := len(resp.([]string))
		// 根据任务id，从hashmap中获取所有任务的任务信息
		for _, jobname := range resp.([]string) {
			ret, err := dbClient.HMGet(context.Background(), jobname, field1, field2, field3)
			if err != nil {
				mlog.Error("Can't get from db", zap.Error(err))
				return
			}

			// 把string解析成time类型
			nextexectime, err := time.ParseInLocation(storeLayout, ret[1], Loc)
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
			id := jobNumber
			incNum()
			numLock.Unlock()

			makeJob(id, jobname, nextexectime, duration)
		}
		// 如果成功取出了任务，通知调度器开始分派
		if jobNum > 0 {
			// mlog.Debugf("Get %d jobs", jobNumber)
			newJob <- struct{}{}
		}
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

	// var name interface{} = jobName + "," + job.NextExecTime.Truncate(time.Second).Format(storeLayout)
	idstr := strconv.Itoa(int(jobNum))
	err := dbClient.HSet(context.Background(), field6, idstr, "")
	if err != nil {
		mlog.Errorf("Failed to add jobid %d to redis", jobNum, zap.Error(err))
	}

	key := "job" + idstr
	insertMap := make(map[string]interface{})
	insertMap[field2] = nextExecTime.Truncate(time.Second).String()
	insertMap[field3] = duration.String()
	insertMap[field5] = jobName
	err = dbClient.HMSet(context.Background(), key, insertMap)
	if err != nil {
		mlog.Errorf("Failed to add %s's information to redis", key, zap.Error(err))
	}

	// 将job信息添加到调度表中
	JobManager.mutex.Lock()
	heap.Push(JobManager.jobheap, job)
	taskToSchedule.Inc()
	JobManager.mutex.Unlock()
	// mlog.Debugf("%s added to heap again, next time: %s, jobid: %d", job.Jobname, job.NextExecTime, job.jobid)

	return job
}

func deleteJob(job *JobInfo) {
	if job.status == 3 {
		taskDone.Inc()
		if job.reDispatch {
			taskRecovered.Inc()
		}
	}

	idstr := strconv.Itoa(int(job.jobid))
	err := dbClient.HDel(context.Background(), field6, idstr)
	if err != nil {
		mlog.Errorf("Failed to HDEL %s:%d", job.Jobname, job.jobid, zap.Error(err))
	}

	key := "job" + idstr
	_, err = dbClient.Del(context.Background(), key)
	if err != nil {
		mlog.Errorf("Failed to DEL %s from redis", idstr, zap.Error(err))
	}

	mapLock.Lock()
	worker := jobMap[job.jobid].worker
	delete(jobMap, job.jobid)
	mapLock.Unlock()

	if worker == nil || !worker.online {
		return
	}
	worker.mutex.Lock()
	worker.jobnumber--
	delete(worker.jobList, job.jobid)
	worker.mutex.Unlock()
}

func jobWatch(job *JobInfo) {
	// mlog.Debugf("now: %s, nextexectime: %s", time.Now().In(Loc), job.NextExecTime)
	var interval time.Duration
	if job.Interval == 0 {
		interval = Interval
	} else {
		interval = job.Interval
	}
	timer := time.NewTimer(interval / 2)

	<-timer.C
	timer.Stop()
	if job.status != 3 {
		mapLock.RLock()
		if !jobMap[job.jobid].worker.online {
			deleteJob(job)
			mapLock.RUnlock()
			return
		}
		mapLock.RUnlock()

		taskOverTime.Inc()
		mlog.Debugf("job %s didn't receive result, jobid: %d, status: %d", job.Jobname, job.jobid, job.status)
		numLock.Lock()
		num := jobNumber
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

	go deleteJob(job)
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
	if !worker.online {
		if !job.reDispatch {
			taskOverTime.Inc()
			job.reDispatch = true
		}
		worker.jobnumber += math.MaxUint32 / 2
		worker.mutex.Unlock()
		heap.Fix(WorkerManager.workerheap, 0)
		WorkerManager.mutex.Unlock()
		JobManager.mutex.Lock()
		heap.Push(JobManager.jobheap, job)
		JobManager.mutex.Unlock()
		return
	}
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

	key := "job" + strconv.Itoa(int(job.jobid))
	var tmp interface{} = worker.workerURI
	err := dbClient.HSet(context.Background(), key, field4, tmp)
	if err != nil {
		mlog.Errorf("Failed to add worker of job %d", job.jobid, zap.Error(err))
	}
	// mlog.Debugf("workload of %s: %d", worker.workerURI, worker.jobnumber)

	// 修改任务状态为1
	job.status = 1
	// grpc派发任务
	_, err = workerClient[worker.workerURI].DispatchJob(context.Background(), &mypb.DispatchJobRequest{
		JobInfo: req,
	})
	if err != nil {
		// taskOverTime.Inc()
		worker.online = false
		mlog.Errorf("DispatchJob error, jobid: %d", job.jobid, zap.Error(err))
		job.reDispatch = true
		JobManager.mutex.Lock()
		heap.Push(JobManager.jobheap, job)
		JobManager.mutex.Unlock()
		<-newJob
		return
	}

	jobWatch(job)
	// mlog.Debugf("job %s assigned to worker %s, jobid: %d", job.Jobname, worker.workerURI, job.jobid)
}

func incNum() {
	if jobNumber == math.MaxUint32 {
		jobNumber = 0
	} else {
		jobNumber++
	}
	err := dbClient.Set(context.Background(), "jobnumber", strconv.Itoa(int(jobNumber)))
	if err != nil {
		mlog.Errorf("Failed to update jobnumber to redis", zap.Error(err))
	}
}

// 取worker最小堆的最小元素，即最小负载worker，对其进行任务分派
func assignJob() {
	var ticker <-chan time.Time
	// 等待至少一个worker到达后才能开始分配任务
	for {
		if WorkerManager.workerheap.Len() == 0 {
			mlog.Debugf("No worker joined, scheduler fall asleep")
			<-newworker
			mlog.Debugf("New worker arrived, start to assign job")
		}
		for WorkerManager.workerheap.Len() > 0 && JobManager.jobheap.Len() > 0 && time.Now().In(Loc).After((*JobManager.jobheap)[0].NextExecTime.Add(-100*time.Microsecond)) {
			// job := heap.Pop(JobManager.jobheap)
			// 获取worker堆的堆顶元素
			// mlog.Debugf("size of the jobheap is: %d", JobManager.jobheap.Len())
			// if WorkerManager.workerheap.Len() == 0 {
			// 	mlog.Debugf("No worker joined, scheduler fall asleep")
			// 	break
			// }

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
				id := jobNumber
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
					err := dbClient.HSet(context.Background(), job.Jobname, field2, tmp)
					if err != nil {
						mlog.Errorf("Failed to change %s time", job.Jobname, zap.Error(err))
						continue
					}
					// mlog.Debugf("%s added to redis, next time: %s", job.Jobname, nextTime)
					_, err = dbClient.RPush(context.Background(), nextTime.Truncate(Interval).String(), i)
					if err != nil {
						mlog.Error("Failed to rpush", zap.Error(err))
						continue
					}
				}
			case 2:
				numLock.Lock()
				id := jobNumber
				incNum()
				numLock.Unlock()

				makeJob(id, job.Jobname, nextTime, job.Interval)
			case 3:
				var i interface{} = job.Jobname
				var tmp interface{} = nextTime.Truncate(time.Second).Format("2006-01-02 15:04:05 -0700 MST")
				err := dbClient.HSet(context.Background(), job.Jobname, field2, tmp)
				if err != nil {
					mlog.Errorf("Failed to change %s time", job.Jobname, zap.Error(err))
					continue
				}
				// mlog.Debugf("%s added to redis, next time: %s", job.Jobname, nextTime)
				_, err = dbClient.RPush(context.Background(), nextTime.Truncate(Interval).String(), i)
				if err != nil {
					mlog.Error("Failed to rpush", zap.Error(err))
					continue
				}
			}
		}
		ticker = time.After(100 * time.Microsecond)
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
func httpListener(httpPort string) {
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

	mlog.Infof("httplistener start on port%s", httpPort)
	if err := http.ListenAndServe(httpPort, nil); err != nil {
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
// curl -X POST -d "5 * * * * *,2023-11-16,15:00:00,get,http://Localhost:8080/" URI
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
		begintime_str = time.Now().In(Loc).Format(parseLayout)
		method = parts[1]
		targetURI = parts[2]
	} else {
		begintime_str = parts[1] + "," + parts[2]
		method = parts[3]
		targetURI = parts[4]
	}

	// 将begintime解析为time.Time格式
	begintime, err := time.ParseInLocation(parseLayout, begintime_str, Loc)
	if err != nil {
		mlog.Error("Failed to parse begintime", zap.Error(err))
		return
	}

	jobname := method + "@" + targetURI + "@" + duration.String()
	// mlog.Debugf("jobname: %s, begintime: %s", jobname, begintime.String())

	if _, ok := jobList[jobname]; !ok {
		taskNumber.Inc()
	}

	// 如果job的下一次调度时间在当前时刻之前，直接丢弃掉该任务
	switch judgeScheduleTime(begintime) {
	case 0:
		mlog.Debugf("Job %s time expired", jobname)
		return
	case 1, 2:
		numLock.Lock()
		id := jobNumber
		incNum()
		numLock.Unlock()

		makeJob(id, jobname, begintime, duration)
		newJob <- struct{}{}
	case 3:
		scheduleTime := begintime.Truncate(Interval)
		var i interface{} = jobname
		_, err = dbClient.RPush(context.Background(), scheduleTime.String(), i)
		if err != nil {
			mlog.Error("Failed to rpush", zap.Error(err))
		}
	}
	addToRedis(jobname, begintime.String(), duration.String())
}

func addToRedis(jobname, nextExecTime, interval string) {
	// 将job加入redis表中
	hash := make(map[string]interface{})
	hash[field1] = ""
	hash[field2] = nextExecTime
	hash[field3] = interval
	hash["jobname"] = jobname
	err := dbClient.HMSet(context.Background(), jobname, hash)
	if err != nil {
		mlog.Errorf("Failed to hmset")
	}
}

// 记录job的执行结果（已交给worker来记录）
// func RecordResult(resultCh <-chan []string) {
// 	for execResult := range resultCh {
// 		jobname := execResult[0]
// 		executeResult := execResult[1]
// 		jobname = "result/" + jobname

// 		length, err := dbClient.LLen(context.Background(), executeResult)
// 		if err != nil {
// 			mlog.Error("", zap.Error(err))
// 		}
// 		if length == 5 {
// 			_, err = dbClient.LPop(context.Background(), executeResult)
// 			if err != nil {
// 				mlog.Error("", zap.Error(err))
// 			}
// 		}
// 		_, err = dbClient.RPush(context.Background(), jobname, executeResult)
// 		if err != nil {
// 			mlog.Error("", zap.Error(err))
// 		}

// 		mlog.Debugf("%s: %s", jobname, executeResult)
// 	}
// }
