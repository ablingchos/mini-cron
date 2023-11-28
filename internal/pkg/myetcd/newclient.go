package myetcd

import (
	"context"
	"time"

	"git.code.oa.com/red/ms-go/pkg/mlog"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// 创建一个新的etcd客户端，返回创建的客户端
func newClient(endpoints []string) (*clientv3.Client, error) {
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 3 * time.Second,
	})
	return etcdClient, err
}

// 注册新加入客户端的grpc服务地址
func registGrpcAddr(client *clientv3.Client, key, value string) error {
	_, err := client.Put(context.Background(), key, value)
	if err != nil {
		// mlog.Error("Can't regist to etcd", zap.Error(err))
		return err
	}
	mlog.Debugf("put: %s, value: %s", key, value)
	return nil
}

func ConnectToEtcd(endpoints, key, value string) (*clientv3.Client, error) {
	etcdclient, err := newClient([]string{endpoints})
	if err != nil {
		return nil, err
	}

	err = registGrpcAddr(etcdclient, key, value)
	if err != nil {
		return nil, err
	}

	return etcdclient, nil

}

// func RegisterNewClient(client *clientv3.Client, clientKey string) error {
// 	session, err := concurrency.NewSession(client)
// 	if err != nil {
// 		// mlog.Error("Failed to create session", zap.Error(err))
// 		return err
// 	}
// 	mlog.Infof("session begin")
// 	defer session.Close()

// 	// 创建关于key“client-number”互斥锁
// 	mutex := concurrency.NewMutex(session, clientKey)
// 	err = mutex.Lock(context.Background())
// 	if err != nil {
// 		// mlog.Error("Failed to acquire lock", zap.Error(err))
// 		return err
// 	}

// 	// 操作key，新客户端登录，value+1
// 	resp, err := client.Get(context.Background(), clientKey)
// 	if err != nil {
// 		// mlog.Error("Failed to get from etcd", zap.Error(err))
// 		return err
// 	}
// 	// 自增
// 	for _, ev := range resp.Kvs {
// 		mlog.Infof("key: %s, initial value: %s", ev.Key, ev.Value)
// 		_, err := client.Put(context.Background(), clientKey, string(binary.BigEndian.Uint32(ev.Value)+1))
// 		if err != nil {
// 			// mlog.Error("Failed to put to etcd", zap.Error(err))
// 			return err
// 		}
// 	}

// 	// 解锁
// 	err = mutex.Unlock(context.Background())
// 	if err != nil {
// 		// mlog.Error("Failed to release lock", zap.Error(err))
// 		return err
// 	}

// 	resp, err = client.Get(context.Background(), clientKey)
// 	if err != nil {
// 		// mlog.Error("Failed to get from etcd", zap.Error(err))
// 		return err
// 	}

// 	// 查询自增后的键值
// 	for _, ev := range resp.Kvs {
// 		mlog.Infof("key: %s, revision value: %s", ev.Key, ev.Value)
// 	}

// 	mlog.Infof("session end")
// 	return nil
// }
