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
	jobCh    = make(chan *JobInfo)
	resultCh = make(chan []string)
)

func workLoop() {
	var nextTick <-chan time.Time
	for {
		JobManager.mutex.Lock()
		now := time.Now().In(Loc)
		for len(*JobManager.jobheap) > 0 && now.After((*JobManager.jobheap)[0].NextExecTime) {
			// job := heap.Pop(JobManager.jobheap).(*JobInfo)
			// 取出堆顶元素（即最早执行元素），调整其下一次执行时间后整堆
			job := (*JobManager.jobheap)[0]
			if job.Interval == 0 {
				heap.Pop(JobManager.jobheap)
			} else {
				job.NextExecTime = job.NextExecTime.Add(job.Interval)
				heap.Fix(JobManager.jobheap, 0)
			}

			jobStatusClient.JobStarted(context.Background(), &mypb.JobStartedRequest{
				Jobname: job.JobName,
			})
			// go execJob(job)
			jobCh <- job
		}
		if len(*JobManager.jobheap) > 0 {
			nextTick = time.After((*JobManager.jobheap)[0].NextExecTime.Sub(now))
		}
		JobManager.mutex.Unlock()

		select {
		case <-nextTick:
		case job := <-newjobCh:
			JobManager.mutex.Lock()
			heap.Push(JobManager.jobheap, job)
			JobManager.mutex.Unlock()
		}
	}
}

func execJob() {
	for job := range jobCh {
		parts := strings.Split(job.JobName, "@")
		uri := "http://" + parts[1]
		switch parts[0] {
		case "get":
			resp, err := http.Get(uri)
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
			resultCh <- []string{job.JobName, resp.Status}
		default:
			mlog.Error("Func incomplete")
			// resultCh <- []string{job.JobName, "error"}
		}
	}
}

func reportResult() {
	for result := range resultCh {
		jobStatusClient.JobCompleted(context.TODO(), &mypb.JobCompletedRequest{
			Jobname:   result[0],
			Jobresult: result[1],
		})
		mlog.Infof("result of %s is: %s", result[0], result[1])
	}
}
