package worker

import (
	"container/heap"
	"context"

	"git.code.oa.com/red/ms-go/pkg/mlog"
	"git.woa.com/kefuai/my-project/pkg/mypb"
	"go.uber.org/zap"
)

// var (
// 	field1 = "method"
// 	field2 = "createtime"
// 	field3 = "Interval"
// )

// func RecordJob(newjobCh <-chan *JobInfo) {
// 	for job := range newjobCh {
// 		JobManager.mutex.Lock()
// 		heap.Push(JobManager.jobheap, &JobInfo{
// 			JobName:      job.JobName,
// 			NextExecTime: job.NextExecTime,
// 			Interval:     job.Interval,
// 		})
// 		JobManager.mutex.Unlock()
// 	}
// }

// var (
// 	jobCh = make(chan *JobInfo)
// 	resultCh = make(chan resultStruct)
// )

// type resultStruct struct {
// 	jobid     int32
// 	resultName   string
// 	jobresult string
// }

// func workLoop() {
// 	var nextTick <-chan time.Time
// 	for {
// 		JobManager.mutex.Lock()
// 		now := time.Now().In(Loc)
// 		for len(*JobManager.jobheap) > 0 && now.After((*JobManager.jobheap)[0].NextExecTime) {
// 			// job := heap.Pop(JobManager.jobheap).(*JobInfo)
// 			// 取出堆顶元素（即最早执行元素），调整其下一次执行时间后整堆
// 			job := heap.Pop(JobManager.jobheap).(*JobInfo)
// 			jobCh <- job
// 		}
// 		if len(*JobManager.jobheap) > 0 {
// 			nextTick = time.After((*JobManager.jobheap)[0].NextExecTime.Sub(now))
// 		}
// 		JobManager.mutex.Unlock()

// 		select {
// 		case <-nextTick:
// 		case job := <-newjobCh:
// 			JobManager.mutex.Lock()
// 			heap.Push(JobManager.jobheap, job)
// 			JobManager.mutex.Unlock()
// 		}
// 	}
// }

// func workLoop() {
// 	var nextTick <-chan time.Time
// 	for {
// 		JobManager.mutex.Lock()
// 		now := time.Now().In(Loc)
// 		for len(*JobManager.jobheap) > 0 && now.After((*JobManager.jobheap)[0].NextExecTime) {
// 			// job := heap.Pop(JobManager.jobheap).(*JobInfo)
// 			// 取出堆顶元素（即最早执行元素），调整其下一次执行时间后整堆
// 			job := (*JobManager.jobheap)[0]
// 			if job.Interval == 0 {
// 				heap.Pop(JobManager.jobheap)
// 			} else {
// 				job.NextExecTime = job.NextExecTime.Add(job.Interval)
// 				heap.Fix(JobManager.jobheap, 0)
// 			}

// 			jobStatusClient.JobStarted(context.Background(), &mypb.JobStartedRequest{
// 				Jobname: job.JobName,
// 			})
// 			// go execJob(job)
// 			jobCh <- job
// 		}
// 		if len(*JobManager.jobheap) > 0 {
// 			nextTick = time.After((*JobManager.jobheap)[0].NextExecTime.Sub(now))
// 		}
// 		JobManager.mutex.Unlock()

// 		select {
// 		case <-nextTick:
// 		case job := <-newjobCh:
// 			JobManager.mutex.Lock()
// 			heap.Push(JobManager.jobheap, job)
// 			JobManager.mutex.Unlock()
// 		}
// 	}
// }

func workerLoop() {
	for {
		// for len(*JobManager.jobheap) > 0 && time.Now().In(Loc).After((*JobManager.jobheap)[0].NextExecTime) {
		JobManager.mutex.Lock()
		for len(*JobManager.jobheap) > 0 {
			// job := heap.Pop(JobManager.jobheap).(*JobInfo)
			// 取出堆顶元素（即最早执行元素），调整其下一次执行时间后整堆
			job := heap.Pop(JobManager.jobheap).(*JobInfo)
			go func() {
				_, err := jobStatusClient.JobStarted(context.Background(), &mypb.JobStartedRequest{
					Jobid: job.jobid,
				})
				if err != nil {
					mlog.Errorf("JobStarted error", zap.Error(err))
				}
				job.status <- struct{}{}
			}()
			go execJob(job)
			// 通知scheduler，job开始执行
		}
		JobManager.mutex.Unlock()

		<-newjobCh
		// ticker := time.After(100 * time.Microsecond)
		// now := time.Now().In(Loc)
		// if len(*JobManager.jobheap) > 0 {
		// 	// 如果当前时间在队首job下次执行时间的100ms之前，立即执行，否则等待到100ms前
		// 	if now.Before((*JobManager.jobheap)[0].NextExecTime.Add(-100 * time.Microsecond)) {
		// 		ticker = time.After(0)
		// 	} else {
		// 		duration := (*JobManager.jobheap)[0].NextExecTime.Sub(now)
		// 		ticker = time.After(duration - 100*time.Microsecond)
		// 	}
		// }
		// select {
		// case <-ticker:
		// case <-newjobCh:
		// }
	}
}

func execJob(job *JobInfo) {
	// parts := strings.Split(job.JobName, "@")
	// uri := "http://" + parts[1]
	// var resp *http.Response
	// switch parts[0] {
	// case "get":
	// 	var err error
	// 	resp, err = http.Get(uri)
	// 	if err != nil {
	// 		mlog.Errorf("job: %s, failed to get HttpResponse", job.JobName, zap.Error(err))
	// 		// resultCh <- []string{job.JobName, "error"}
	// 	}
	// 	// defer resp.Body.Close()
	// 	// body, err := io.ReadAll(resp.Body)
	// 	if err != nil {
	// 		mlog.Errorf("job: %s, failed to read HttpResponse", job.JobName, zap.Error(err))
	// 		// resultCh <- []string{job.JobName, "error"}
	// 	}

	// 	// mlog.Infof("Successfully get result of job: %s", job.JobName)
	// 	// resultCh <- []string{job.JobName, string(body)}
	// 	// resultCh <- resultStruct{
	// 	// 	jobid:     job.jobid,
	// 	// 	resultName:   job.JobName,
	// 	// 	jobresult: resp.Status,
	// 	// }
	// default:
	// 	mlog.Error("Func incomplete")
	// 	// resultCh <- []string{job.JobName, "error"}
	// }
	mlog.Debugf("result of %s is: %s, id: %d", job.JobName, "200 OK", job.jobid)
	go recordResult(job, "200 OK")
}

func recordResult(job *JobInfo, jobresult string) {
	go func() {
		<-job.status
		_, err := jobStatusClient.JobCompleted(context.Background(), &mypb.JobCompletedRequest{
			Jobid:     job.jobid,
			Jobresult: jobresult,
		})
		if err != nil {
			mlog.Errorf("JobCompleted error", zap.Error(err))
		}
	}()
	// 将执行结果写入redis中
	resultName := "result/" + job.JobName
	// 判断记录的结果是否大于5条
	length, err := DbClient.LLen(context.Background(), resultName)
	if err != nil {
		mlog.Error("", zap.Error(err))
	}
	if length == 5 {
		_, err = DbClient.LPop(context.Background(), resultName)
		if err != nil {
			mlog.Error("", zap.Error(err))
		}
	}
	_, err = DbClient.RPush(context.Background(), resultName, jobresult)
	if err != nil {
		mlog.Error("", zap.Error(err))
	}

	// mlog.Debugf("%s: %s", resultName, executeResult)
	// for result := range resultCh {
	// 	// 通知scheduler执行成功
	// 	jobStatusClient.JobCompleted(context.Background(), &mypb.JobCompletedRequest{
	// 		Jobid:     result.jobid,
	// 		Jobresult: result.jobresult,
	// 	})

	// 	// 将执行结果写入redis中
	// 	resultName := "result/" + result.resultName
	// 	// 判断记录的结果是否大于5条
	// 	length, err := DbClient.LLen(context.Background(), resultName)
	// 	if err != nil {
	// 		mlog.Error("", zap.Error(err))
	// 	}
	// 	if length == 5 {
	// 		_, err = DbClient.LPop(context.Background(), resultName)
	// 		if err != nil {
	// 			mlog.Error("", zap.Error(err))
	// 		}
	// 	}
	// 	_, err = DbClient.RPush(context.Background(), resultName, result.jobresult)
	// 	if err != nil {
	// 		mlog.Error("", zap.Error(err))
	// 	}

	// 	// mlog.Debugf("%s: %s", resultName, executeResult)
	// 	mlog.Debugf("result of %s is: %s", result.resultName, result.jobresult)
	// }
}
