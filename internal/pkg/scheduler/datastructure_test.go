package scheduler

import (
	"container/heap"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestJobManager(t *testing.T) {
	jobM := &jobManager{
		jobheap: &jobHeap{},
	}
	time1 := time.Now()
	var i int32
	for i = 5000; i >= 1; i-- {
		heap.Push(jobM.jobheap, &JobInfo{
			jobid:        i,
			NextExecTime: time.Now(),
		})
	}

	for i = 5001; i <= 10000; i++ {
		heap.Push(jobM.jobheap, &JobInfo{
			jobid:        i,
			NextExecTime: time.Now(),
		})
	}

	time2 := (heap.Pop(jobM.jobheap)).(*JobInfo).NextExecTime
	for j := 1; j <= 9999; j++ {
		assert.EqualValues(t, true, time1.Before(time2))
		if j == 9999 {
			break
		}
		time1, time2 = time2, heap.Pop(jobM.jobheap).(*JobInfo).NextExecTime
	}
}

func TestWorkerManager(t *testing.T) {
	workerM := &workerManager{
		workerheap: &workerHeap{},
	}

	heap.Init(workerM.workerheap)

	for i := 5000; i >= 1; i-- {
		heap.Push(workerM.workerheap, &WorkerInfo{
			jobnumber: i,
		})
	}

	for i := 5001; i <= 10000; i++ {
		heap.Push(workerM.workerheap, &WorkerInfo{
			jobnumber: i,
		})
	}

	i := 0
	k := (heap.Pop(workerM.workerheap)).(*WorkerInfo).jobnumber
	for j := 0; j < 9999; j++ {
		assert.EqualValues(t, true, i < k)
		if j == 9999 {
			break
		}
		i, k = k, (heap.Pop(workerM.workerheap)).(*WorkerInfo).jobnumber
	}
}
