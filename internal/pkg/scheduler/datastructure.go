package scheduler

import (
	"sync"
	"time"
)

// 记录job信息的结构
// 0:待分配，1：已分配未工作，2：已分配已工作，3：已完成
type JobInfo struct {
	Jobname      string
	NextExecTime time.Time
	Interval     time.Duration
	JobStatus    int
	// Operation    string
}

// 按job执行时间排序的小根堆
type jobHeap []*JobInfo

// 对job堆进行管理的结构体
type jobManager struct {
	jobheap *jobHeap
	mutex   sync.Mutex
	newJob  chan struct{}
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

// func (h *jobHeap) Top() *JobInfo {
// 	return (*h)[0]
// }

// 记录worker状态
type WorkerInfo struct {
	workerURI string
	jobnumber int
	jobList   []*JobInfo
	status    string
}

type workerHeap []*WorkerInfo

type workerManager struct {
	workerheap *workerHeap
	mutex      sync.Mutex
	newWorker  chan string
}

func (w workerHeap) Len() int {
	return len(w)
}

func (w workerHeap) Less(i, j int) bool {
	return w[i].jobnumber < w[j].jobnumber
}

func (w workerHeap) Swap(i, j int) {
	w[i], w[j] = w[j], w[i]
}

func (w *workerHeap) Push(x interface{}) {
	*w = append(*w, x.(*WorkerInfo))
}

func (w *workerHeap) Pop() interface{} {
	old := *w
	n := len(old)
	x := old[n-1]
	*w = old[0 : n-1]
	return x
}
