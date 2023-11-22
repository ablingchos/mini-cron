package worker

import (
	"context"
	"time"

	"git.code.oa.com/red/ms-go/pkg/mlog"
	"github.com/ablingchos/my-project/internal/pkg/kvdb"
	"github.com/ablingchos/my-project/internal/pkg/myetcd"
	"github.com/ablingchos/my-project/pkg/mypb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	DbClient        kvdb.KVDb
	EtcdClient      *clientv3.Client
	Interval        time.Duration
	Loc             *time.Location
	etcdHelloClient mypb.EtcdHelloClient
	jobStatusClient mypb.JobStatusClient
	JobManager      *jobManager
	workerKey       = "workerURI"
)

func Initial(redisURI, endpoints, schedulerKey, workerURI string, loc *time.Location) error {
	// 连接到redis
	DbClient = &kvdb.RedisDB{}
	err := DbClient.Connect(redisURI)
	if err != nil {
		return err
	}

	// 连接到etcd
	EtcdClient, err := myetcd.ConnectToEtcd(endpoints, workerURI, workerURI)
	if err != nil {
		return err
	}

	// 获取scheduler的grpc服务地址
	var schedulerURI string
	resp, err := EtcdClient.Get(context.Background(), schedulerKey)
	if err != nil {
		return err
	}
	for _, v := range resp.Kvs {
		schedulerURI = string(v.Value)
	}
	mlog.Infof("Get schedulerURI: %s", schedulerURI)

	Loc = loc

	// 启动worker的grpc服务
	go StartWorkerGrpc()

	// 获取scheduler的etcd服务客户端
	etcdHelloClient, err = etcdHello(schedulerKey, schedulerURI)
	if err != nil {
		return err
	}

	// 向scheduler注册自身信息
	_, err = etcdHelloClient.WorkerHello(context.Background(), &mypb.WorkerHelloRequest{
		WorkerURI: workerURI,
	})
	if err != nil {
		return err
	}

	// 获取scheduler的job状态上报服务客户端
	jobStatusClient, err = jobStatus(schedulerKey)
	if err != nil {
		return err
	}

	JobManager = &jobManager{
		jobheap: &jobHeap{},
	}

	go workLoop()
	go execJob()
	go reportResult()

	return nil
}
