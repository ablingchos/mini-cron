package scheduler

import (
	"container/heap"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMyheap(t *testing.T) {
	job1 := &JobInfo{Jobname: "1", NextExecTime: time.Now().Add(1 * time.Second), Interval: time.Minute}
	job2 := &JobInfo{Jobname: "2", NextExecTime: time.Now().Add(2 * time.Second), Interval: time.Minute}
	job3 := &JobInfo{Jobname: "3", NextExecTime: time.Now().Add(3 * time.Second), Interval: time.Minute}
	job4 := &JobInfo{Jobname: "4", NextExecTime: time.Now().Add(1 * time.Hour), Interval: time.Minute}
	job5 := &JobInfo{Jobname: "5", NextExecTime: time.Now().Add(1 * time.Minute), Interval: time.Minute}

	jobheap := &jobHeap{}

	heap.Push(jobheap, job2)
	heap.Push(jobheap, job4)
	heap.Push(jobheap, job1)
	heap.Push(jobheap, job5)
	heap.Push(jobheap, job3)

	check := []string{"1", "2", "3", "5", "4"}
	ans := make([]string, 0)

	for jobheap.Len() > 0 {
		id := heap.Pop(jobheap).(*JobInfo).Jobname
		ans = append(ans, id)
	}

	st := reflect.DeepEqual(check, ans)
	assert.EqualValues(t, true, st)
}
