package worker

import (
	"context"
	"strings"
	"sync"
	"time"

	"git.code.oa.com/red/ms-go/pkg/mlog"
	"git.woa.com/kefuai/my-project/internal/pkg/kvdb"
	"git.woa.com/kefuai/my-project/internal/pkg/myetcd"
	"git.woa.com/kefuai/my-project/pkg/mypb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	dbClient          kvdb.KVDb
	etcdClient        *clientv3.Client
	Loc               *time.Location
	heartbeatInterval time.Duration
	etcdHelloClient   mypb.EtcdHelloClient
	jobStatusClient   mypb.JobStatusClient
	clientLock        sync.RWMutex
	JobManager        *jobManager
	port              string
	// schedulerOffline  = make(chan struct{})
	// ip                string
	// Interval          time.Duration
	// jobMap            = make(map[int]*JobInfo)
	// workerKey         = "workerURI"
)

func Initial(redisURI, endpoints, schedulerKey, workerURI string, loc *time.Location, hb time.Duration) error {

	idx := strings.LastIndex(workerURI, ":")
	// ip = workerURI[:idx-1]
	port = workerURI[idx:]

	// 连接到redis
	dbClient = &kvdb.RedisDB{}
	err := dbClient.Connect(redisURI)
	if err != nil {
		return err
	}

	// 连接到etcd
	etcdClient, err = myetcd.ConnectToEtcd(endpoints, workerURI, "online")
	if err != nil {
		return err
	}

	// 获取scheduler的grpc服务地址
	var schedulerURI string
	resp, err := etcdClient.Get(context.Background(), schedulerKey)
	if err != nil {
		return err
	}
	for _, v := range resp.Kvs {
		schedulerURI = string(v.Value)
	}
	// mlog.Infof("Get schedulerURI: %s", schedulerURI)

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
	// go schedulerTimeout()

	// 开始向etcd发送心跳包
	heartbeatInterval = hb
	go myetcd.HeartBeat(etcdClient, workerURI, "online", heartbeatInterval)

	// 获取scheduler的job状态上报服务客户端
	jobStatusClient, err = jobStatus(schedulerURI)
	if err != nil {
		return err
	}

	// 初始化工作堆
	JobManager = &jobManager{
		jobheap: &jobHeap{},
	}

	go workerLoop()

	mlog.Infof("Worker %s initial successfully, start to work", workerURI)
	return nil
}
