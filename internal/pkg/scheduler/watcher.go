package scheduler

import (
	"container/heap"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"git.code.oa.com/red/ms-go/pkg/mlog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// func NewSchedulerClient(endpoints string) (*clientv3.Client, error) {
// 	// 建立新的客户端连接
// 	client, err := newClient([]string{endpoints})
// 	if err != nil {
// 		mlog.Fatal("Can't start etcd client", zap.Error(err))
// 	}

// 	// 向etcd注册新的scheduler
// 	key := "schedulerURI"
// 	value := "localhost:50051"
// 	err = registGrpcAddr(client, key, value)
// 	if err != nil {
// 		mlog.Fatal("Can't regist %s to etcd", zap.Error(err))
// 	}

// 	return client, err
// 	// clienID := 2
// 	// clientGrpcAddr := "localhost:50050"
// 	// registWorker(client, clienID, clientGrpcAddr)
// 	// NewWatcher(client, clienID)
// }

func newWatcher(client *clientv3.Client, clientURI string) *heartbeatWatcher {
	var wg sync.WaitGroup
	key := fmt.Sprintf("heartbeat/%s", clientURI)

	wg.Add(2)
	return newHeartbeatWatcher(client, key, 4*time.Second, &wg)
	// wg.Wait()
}

// 心跳观测结构体，包含了客户端信息，key、超时时间、上一次心跳时间、定时器以及一个互斥锁
type heartbeatWatcher struct {
	client        *clientv3.Client
	key           string
	timeout       time.Duration
	lastHeartbeat time.Time
	timer         *time.Timer
	timerLock     sync.Mutex
}

// @hw结构体中的函数，用于实现对指定key的监控操作
// Params：
//
//	    param1：普通的ctx
//		param2：监控的key值
//
// Returns：
//
//	none
func (hw *heartbeatWatcher) watch(ctx context.Context, key string, wg *sync.WaitGroup) {
	for {
		mlog.Debug("Getting initial systemopen before watch")
		resp, err := hw.client.KV.Get(ctx, key)
		if err != nil {
			mlog.Error("Failed to get from etcd", zap.Error(err))
			return
		}
		rev := resp.Header.Revision
		for etcdEvent := range hw.client.Watch(ctx, key, clientv3.WithRev(rev+1)) {
			for _, ev := range etcdEvent.Events {
				mlog.Debugf("Type: %s, key: %s, value: %s", ev.Type, ev.Kv.Key, ev.Kv.Value)
				hw.updateLastHeartbeat()
				if string(ev.Kv.Value) == "offline" {
					mlog.Debug("Watch over")
					// wg.Done()
					return
				}
			}
		}
	}
}

// 更新最后一次接收到心跳包的时间
func (hw *heartbeatWatcher) updateLastHeartbeat() {
	hw.timerLock.Lock()
	defer hw.timerLock.Unlock()

	hw.lastHeartbeat = time.Now()
	hw.timer.Reset(hw.timeout)
}

// 定期（timeout间隔）检查是否超时，如果超时就把key所对应的value值更新为offline，关闭定时器并退出函数
func (hw *heartbeatWatcher) checkTimeout(wg *sync.WaitGroup) {
	for {
		<-hw.timer.C
		hw.timerLock.Lock()
		defer hw.timerLock.Unlock()
		if time.Since(hw.lastHeartbeat) >= hw.timeout {
			WorkerManager.mutex.Lock()
			for i, value := range *WorkerManager.workerheap {
				str := hw.key
				index := strings.LastIndex(hw.key, "/")
				str = strings.TrimPrefix(str, str[:index+1])

				if value.workerURI == str {
					value.mutex.Lock()
					heap.Remove(WorkerManager.workerheap, i)
					value.status = "offline"
					value.mutex.Unlock()
					delete(workerClient, value.workerURI)
					mlog.Infof("worker %s offline, removed from worker list", str)
					break
				}
			}

			WorkerManager.mutex.Unlock()
			_, err := hw.client.Put(context.Background(), hw.key, "offline")
			if err != nil {
				mlog.Error("Failed to update key to offline: %v", zap.Error(err))
			} else {
				mlog.Debugf("Key updated to offline: %s", hw.key)
			}
			// 退出定时器
			hw.timer.Stop()
			// wg.Done()
			return
		}
		hw.timer.Reset(hw.timeout)
	}
}

// 创建一个新的心跳观测器，定时器的相关参数由形参决定
func newHeartbeatWatcher(client *clientv3.Client, key string, timeout time.Duration, wg *sync.WaitGroup) *heartbeatWatcher {
	hw := &heartbeatWatcher{
		client:        client,
		key:           key,
		timeout:       timeout,
		lastHeartbeat: time.Now(),
		timer:         time.NewTimer(timeout),
	}
	go hw.watch(context.Background(), hw.key, wg)
	go hw.checkTimeout(wg)

	return hw
}
