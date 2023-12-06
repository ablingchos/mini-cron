package myetcd

import (
	"context"
	"sync"
	"time"

	"git.code.oa.com/red/ms-go/pkg/mlog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func NewWatcher(client *clientv3.Client, clientURI string, timeout time.Duration) *HeartbeatWatcher {
	// var wg sync.WaitGroup
	key := "heartbeat/" + clientURI

	// wg.Add(2)
	hw := &HeartbeatWatcher{
		client:        client,
		Key:           key,
		timeout:       timeout,
		lastHeartbeat: time.Now(),
		timer:         time.NewTicker(timeout),
		Offline:       make(chan struct{}),
	}
	go hw.watch(context.Background(), hw.Key)
	go hw.checkTimeout()

	return hw
}

// 心跳观测结构体，包含了客户端信息，key、超时时间、上一次心跳时间以及一个定时器
type HeartbeatWatcher struct {
	client        *clientv3.Client
	Key           string
	timeout       time.Duration
	lastHeartbeat time.Time
	timerLock     sync.Mutex
	timer         *time.Ticker
	Offline       chan struct{}
}

// 更新最后一次接收到心跳包的时间
func (hw *HeartbeatWatcher) updateLastHeartbeat() {
	hw.timerLock.Lock()
	defer hw.timerLock.Unlock()

	hw.lastHeartbeat = time.Now()
	hw.timer.Reset(hw.timeout)
}

// hw结构体中的函数，用于实现对指定key的监控操作
func (hw *HeartbeatWatcher) watch(ctx context.Context, key string) {
	for {
		// mlog.Debug("Getting initial systemopen before watch")
		resp, err := hw.client.KV.Get(ctx, key)
		if err != nil {
			mlog.Error("Failed to get from etcd", zap.Error(err))
			return
		}
		rev := resp.Header.Revision
		for etcdEvent := range hw.client.Watch(ctx, key, clientv3.WithRev(rev+1)) {
			for _, ev := range etcdEvent.Events {
				hw.updateLastHeartbeat()
				if string(ev.Kv.Value) == "offline" {
					mlog.Debug("Watch over")
					return
				}
			}
		}
	}
}

// 定期（timeout间隔）检查是否超时，如果超时就把key所对应的value值更新为offline，关闭定时器并退出函数
func (hw *HeartbeatWatcher) checkTimeout() {
	for range hw.timer.C {
		if time.Since(hw.lastHeartbeat) >= hw.timeout {
			hw.Offline <- struct{}{}
			_, err := hw.client.Put(context.Background(), hw.Key, "offline")
			if err != nil {
				mlog.Error("Failed to update key to offline: %v", zap.Error(err))
			} else {
				mlog.Debugf("Key updated to offline: %s", hw.Key)
			}
			// 退出定时器
			hw.timer.Stop()
			return
		}
	}
}
