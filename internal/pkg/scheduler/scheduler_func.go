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
	field2    = "createtime"
	field3    = "Interval"
	newworker = make(chan struct{})
)

// 当有新的worker上线时，在调度器中对其进行注册,并为其开启一个心跳watcher
func registWorker() {
	for workerURI := range newWorker {
		newWorker := &WorkerInfo{
			workerURI: workerURI,
			jobnumber: 0,
			jobList:   make([]*JobInfo, 0),
			status:    "online",
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
	fetchCh := make(chan time.Time)
	go fetchJob(fetchCh)
	var timer *time.Ticker

	now := time.Now().In(Loc).Truncate(Interval)
	nextScheduleTime := now.Add(Interval - Interval/10)
	// 下面的代码功能是用来规划调度器首次调度的开始时间，并从此时间开始初始化定时器
	// 如果时间用调度尺度换算后大于下一次的调度时间，立即开始调度
	if nextScheduleTime.Before(now) {
		fetchCh <- nextScheduleTime
		nextScheduleTime = nextScheduleTime.Add(Interval)
		timer = time.NewTicker(Interval)
	} else {
		time.Sleep(time.Until(nextScheduleTime))
		fetchCh <- nextScheduleTime.Add(Interval / 10)
		nextScheduleTime = nextScheduleTime.Add(Interval)
		timer = time.NewTicker(Interval)
	}

	for range timer.C {
		fetchCh <- nextScheduleTime.Add(Interval / 10)
		nextScheduleTime = nextScheduleTime.Add(Interval)
	}

}

// 每隔Interval时间，从db中取出下一个周期将要执行的任务存入调度器内存中
func fetchJob(fetchCh <-chan time.Time) {
	// 在第一次开始取job前，休眠到begintime-Interval/10的时间，再开始启动取job的工作
	for scheduleTime := range fetchCh {
		// 同一个时间段（1分钟）的任务id放在同一个list中，所以直接全部取出
		// now := time.Now().In(Loc).Truncate(Interval).Add(Interval)
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

			// 判断jobname代表的任务是否已经存在
			// jobmu.Lock()
			// if _, ok := jobMap[ret[0]]; ok {

			// }
			// jobmu.Unlock()

			job := &JobInfo{
				Jobname:      jobname,
				NextExecTime: nextexectime,
				Interval:     duration,
				// Operation:    ret[],
			}

			// 添加jobname到*JobInfo的映射
			jobmu.Lock()
			jobMap[jobname] = job
			jobmu.Unlock()

			// 将job信息添加到调度表中
			JobManager.mutex.Lock()
			heap.Push(JobManager.jobheap, job)
			JobManager.mutex.Unlock()
		}
		// 如果成功取出了任务，通知调度器开始分派
		if jobNumber > 0 {
			newJob <- struct{}{}
		}
	}
}

func jobWatcher(job *JobInfo) {
	timer := time.NewTicker(job.Interval / 2)
	<-timer.C
	job.mutex.Lock()
	defer job.mutex.Unlock()
	if job.status != 4 {
		mlog.Debugf("job %s didn't receive result", job.Jobname)
		go dispatch(job)
		return
	}
	// jobmu.Lock()
	// delete(jobMap, job.Jobname)
	// jobmu.Unlock()
}

func dispatch(job *JobInfo) {
	// 取出worker堆顶的worker信息，其所执行的工作数+1，并重新整堆
	WorkerManager.mutex.Lock()
	worker := (*WorkerManager.workerheap)[0]
	worker.jobnumber = worker.jobnumber + 1
	heap.Fix(WorkerManager.workerheap, 0)
	req := &mypb.JobInfo{
		Jobname:      job.Jobname,
		NextExecTime: job.NextExecTime.String(),
		Interval:     job.Interval.String(),
	}

	// 将job添加到worker执行的任务列表中
	worker.jobList = append(worker.jobList, job)
	WorkerManager.mutex.Unlock()

	// grpc派发任务
	go workerClient[worker.workerURI].DispatchJob(context.Background(), &mypb.DispatchJobRequest{
		JobInfo: req,
	})
	mlog.Debugf("job %s assigned to worker %s", job.Jobname, worker.workerURI)

	// 修改任务状态为1
	job.mutex.Lock()
	job.status = 1
	job.mutex.Unlock()
}

// 取worker最小堆的最小元素，即最小负载worker，对其进行任务分派
func assignJob() {
	// 等待至少一个worker到达后才能开始分配任务
	<-newworker
	mlog.Infof("New worker arrived, start to assign job")
	for {
		JobManager.mutex.Lock()
		var now time.Time
		// for len(*JobManager.jobheap) > 0 && now.After((*JobManager.jobheap)[0].NextExecTime.Add(-Interval/20)) {
		for len(*JobManager.jobheap) > 0 {
			// job := heap.Pop(JobManager.jobheap)
			// 获取worker堆的堆顶元素
			if len(*WorkerManager.workerheap) == 0 {
				mlog.Debugf("No worker joined, scheduler fall asleep")
				break
			}
			job := heap.Pop(JobManager.jobheap).(*JobInfo)
			dispatch(job)
			go jobWatcher(job)

			job.NextExecTime = job.NextExecTime.Add(job.Interval)
			switch judgeScheduleTime(job.NextExecTime) {
			case 2:
				heap.Push(JobManager.jobheap, job)
			case 3:
				var i interface{} = job.Jobname
				DbClient.RPush(context.Background(), job.NextExecTime.String(), i)
			}
		}

		now = time.Now().In(Loc)
		ticker := time.After(Interval)
		if len(*JobManager.jobheap) > 0 {
			ticker = time.After((*JobManager.jobheap)[0].NextExecTime.Sub(now))
		}
		JobManager.mutex.Unlock()
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
	} else if nextExecTime.Before(time.Now().In(Loc)) {
		return 1
	} else if scheduleTime == now || scheduleTime == now.Add(Interval) {
		// 如果该job的下一次调度时间段在当前时间段内，或者其所在的时间段的job已经取出了，直接将其加入JobManager中
		return 2
	} else {
		// 该job的下一次调度时间段在当前时间段之外，放入redis中等待调度
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
			return
		}

		Loc, _ := time.LoadLocation("Asia/Shanghai")
		randomtime := time.Date(2025, 1, 1, 0, 0, 0, 0, Loc)
		nexttime := schedule.Next(randomtime)
		duration := nexttime.Sub(randomtime)

		layout := "2006-01-02,15:04:05"
		// 开始时间为空时，默认为time.Now()+duration时刻开始调度
		if len(parts) == 3 {
			begintime_str = time.Now().Add(duration).Format(layout)
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
		mlog.Infof("jobname: %s, begintime: %s, now: %s", jobname, begintime.String(), time.Now().Truncate(time.Second).String())

		scheduleTime := begintime.Truncate(Interval)
		job := &JobInfo{
			Jobname:      jobname,
			NextExecTime: begintime,
			Interval:     duration,
		}
		// 如果job的下一次调度时间在当前时刻之前，直接丢弃掉该任务
		switch judgeScheduleTime(begintime) {
		case 0:
			mlog.Debugf("Job %s time expired", jobname)
			continue
		case 1:
			// 立即执行一次
			dispatch(job)
			// 计算任务从开始时间到现在过去了多少个interval
			pastIntervals := time.Now().In(Loc).Sub(job.NextExecTime) / job.Interval
			// 计算任务的正确的下一次执行时间
			job.NextExecTime = job.NextExecTime.Add((pastIntervals + 1) * job.Interval)

			JobManager.mutex.Lock()
			heap.Push(JobManager.jobheap, job)
			newJob <- struct{}{}
			JobManager.mutex.Unlock()
			mlog.Debugf("job %s successufully added to JobManager", jobname)
		case 2:
			JobManager.mutex.Lock()
			heap.Push(JobManager.jobheap, job)
			newJob <- struct{}{}
			JobManager.mutex.Unlock()
			mlog.Debugf("job %s successufully added to JobManager", jobname)
		case 3:
			var i interface{} = jobname
			DbClient.RPush(context.Background(), scheduleTime.String(), i)
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
