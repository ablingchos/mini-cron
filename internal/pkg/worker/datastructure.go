package worker

import (
	"sync"
	"time"
)

type JobInfo struct {
	jobid        uint32
	JobName      string
	NextExecTime time.Time
	Interval     time.Duration
	status       chan struct{}
}

// 按job执行时间排序的小根堆
type jobHeap []*JobInfo

// 对job堆进行管理的结构体
type jobManager struct {
	jobheap *jobHeap
	mutex   sync.Mutex
	// newJob  chan struct{}
}

func (h jobHeap) Len() int {
	return len(h)
}

func (h jobHeap) Less(i, j int) bool {
	return h[i].NextExecTime.Before(h[j].NextExecTime)
}

func (h jobHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *jobHeap) Push(x interface{}) {
	*h = append(*h, x.(*JobInfo))
}

func (h *jobHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
