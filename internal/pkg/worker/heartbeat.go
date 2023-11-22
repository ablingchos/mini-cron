package worker

import (
	"context"
	"time"

	"git.code.oa.com/red/ms-go/pkg/mlog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// func NewWorker(endpoints string) {
// 	// 建立新的客户端连接
// 	client, err := newClient([]string{endpoints})
// 	if err != nil {
// 		mlog.Fatal("Can't start etcd client", zap.Error(err))
// 	}
// 	defer client.Close()

// 	// 获取scheduler的grpc服务地址
// 	schedulerURI, err := getSchedulerURI(client)
// 	if err != nil {
// 		mlog.Fatal("Can't get schedulerURI", zap.Error(err))
// 	} else {
// 		mlog.Infof("schedulerURI: %s", schedulerURI)
// 	}

// 	clienID := 2

// 	hbtest(client, clienID)
// }

// 客户端心跳包发送函数，每隔interval间隔向服务端发送一个心跳包，如果客户端终止了则停止发送
func heartBeat(etcdClient *clientv3.Client, key string, value string, interval time.Duration) {
	timer := time.NewTicker(interval)
	for range timer.C {
		_, err := etcdClient.Put(context.Background(), key, value)
		if err != nil {
			mlog.Error("heartBeat faile: %v", zap.Error(err))
		}
		mlog.Infof("Sent heartbeat: %s", key)
	}
}

// graceful shutdown
// func HeartBeat(etcdClient *clientv3.Client, key string, value string, interval time.Duration, stopCh <-chan struct{}) {
// 	for {
// 		select {
// 		case <-stopCh:
// 			return
// 		default:
// 			_, err := etcdClient.Put(context.Background(), key, value)
// 			if err != nil {
// 				mlog.Error("heartBeat faile: %v", zap.Error(err))
// 			}
// 			// mlog.Infof("Sent heartbeat: %s", key)
// 			time.Sleep(interval)
// 		}
// 	}
// }

// func hbtest(client *clientv3.Client, clientID int) {
// 	stopCh := make(chan struct{}) // 在指定时间结束后关闭通道，客户端停止发送心跳包
// 	defer close(stopCh)

// 	key := fmt.Sprintf("heartbeat/client%s", strconv.Itoa(clientID))
// 	value := "online"
// 	go heartBeat(client, key, value, 2*time.Second, stopCh)

// 	// time.Sleep(8 * time.Second)
// 	time.Sleep(8 * time.Second)
// }

// 获取scheduler的grpc服务地址
// func getSchedulerURI(client *clientv3.Client) (string, error) {
// 	resp, err := client.Get(context.Background(), "schedulerURI")
// 	if err != nil {
// 		return "", err
// 	}
// 	for _, ev := range resp.Kvs {
// 		return string(ev.Value), nil
// 	}
// 	return "", nil
// }
