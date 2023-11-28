package worker

import (
	"container/heap"
	"context"
	"net/http"
	"strings"
	"time"

	"git.code.oa.com/red/ms-go/pkg/mlog"
	"github.com/ablingchos/my-project/pkg/mypb"
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

var (
	jobCh = make(chan *JobInfo)
	// resultCh = make(chan resultStruct)
)

// type resultStruct struct {
// 	jobid     int32
// 	jobname   string
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
		for len(*JobManager.jobheap) > 0 && time.Now().In(Loc).After((*JobManager.jobheap)[0].NextExecTime) {
			JobManager.mutex.Lock()
			// job := heap.Pop(JobManager.jobheap).(*JobInfo)
			// 取出堆顶元素（即最早执行元素），调整其下一次执行时间后整堆
			job := heap.Pop(JobManager.jobheap).(*JobInfo)
			JobManager.mutex.Unlock()
			jobCh <- job
			// 通知scheduler，job开始执行
			jobStatusClient.JobStarted(context.Background(), &mypb.JobStartedRequest{
				Jobid: job.jobid,
			})
		}

		ticker := time.After(300 * time.Microsecond)
		if len(*JobManager.jobheap) > 0 {
			ticker = time.After((*JobManager.jobheap)[0].NextExecTime.Sub(time.Now().In(Loc)))
		}
		select {
		case <-ticker:
		case <-newjobCh:
		}
	}
}

func execJob() {
	for job := range jobCh {
		parts := strings.Split(job.JobName, "@")
		uri := "http://" + parts[1]
		var resp *http.Response
		switch parts[0] {
		case "get":
			var err error
			resp, err = http.Get(uri)
			if err != nil {
				mlog.Errorf("job: %s, failed to get HttpResponse", job.JobName, zap.Error(err))
				// resultCh <- []string{job.JobName, "error"}
			}
			// defer resp.Body.Close()
			// body, err := io.ReadAll(resp.Body)
			if err != nil {
				mlog.Errorf("job: %s, failed to read HttpResponse", job.JobName, zap.Error(err))
				// resultCh <- []string{job.JobName, "error"}
			}

			// mlog.Infof("Successfully get result of job: %s", job.JobName)
			// resultCh <- []string{job.JobName, string(body)}
			// resultCh <- resultStruct{
			// 	jobid:     job.jobid,
			// 	jobname:   job.JobName,
			// 	jobresult: resp.Status,
			// }
		default:
			mlog.Error("Func incomplete")
			// resultCh <- []string{job.JobName, "error"}
		}
		go recordResult(job.jobid, job.JobName, resp.Status)
	}
}

// func reportResult() {
// 	for result := range resultCh {
// 		jobStatusClient.JobCompleted(context.TODO(), &mypb.JobCompletedRequest{
// 			Jobname:   result[0],
// 			Jobresult: result[1],
// 		})
// 		mlog.Debugf("result of %s is: %s", result[0], result[1])
// 	}
// }

func recordResult(jobid int32, jobname, jobresult string) {
	jobStatusClient.JobCompleted(context.Background(), &mypb.JobCompletedRequest{
		Jobid:     jobid,
		Jobresult: jobresult,
	})

	// 将执行结果写入redis中
	jobname = "result/" + jobname
	// 判断记录的结果是否大于5条
	length, err := DbClient.LLen(context.Background(), jobname)
	if err != nil {
		mlog.Error("", zap.Error(err))
	}
	if length == 5 {
		_, err = DbClient.LPop(context.Background(), jobname)
		if err != nil {
			mlog.Error("", zap.Error(err))
		}
	}
	_, err = DbClient.RPush(context.Background(), jobname, jobresult)
	if err != nil {
		mlog.Error("", zap.Error(err))
	}

	// mlog.Debugf("%s: %s", jobname, executeResult)
	mlog.Debugf("result of %s is: %s", jobname, jobresult)
	// for result := range resultCh {
	// 	// 通知scheduler执行成功
	// 	jobStatusClient.JobCompleted(context.Background(), &mypb.JobCompletedRequest{
	// 		Jobid:     result.jobid,
	// 		Jobresult: result.jobresult,
	// 	})

	// 	// 将执行结果写入redis中
	// 	jobname := "result/" + result.jobname
	// 	// 判断记录的结果是否大于5条
	// 	length, err := DbClient.LLen(context.Background(), jobname)
	// 	if err != nil {
	// 		mlog.Error("", zap.Error(err))
	// 	}
	// 	if length == 5 {
	// 		_, err = DbClient.LPop(context.Background(), jobname)
	// 		if err != nil {
	// 			mlog.Error("", zap.Error(err))
	// 		}
	// 	}
	// 	_, err = DbClient.RPush(context.Background(), jobname, result.jobresult)
	// 	if err != nil {
	// 		mlog.Error("", zap.Error(err))
	// 	}

	// 	// mlog.Debugf("%s: %s", jobname, executeResult)
	// 	mlog.Debugf("result of %s is: %s", result.jobname, result.jobresult)
	// }
}
