package scheduler

import (
	"sync"
	"time"

	"net/http"
	_ "net/http/pprof"

	"git.code.oa.com/red/ms-go/pkg/mlog"
	"github.com/ablingchos/my-project/internal/pkg/kvdb"
	"github.com/ablingchos/my-project/internal/pkg/myetcd"
	"github.com/ablingchos/my-project/pkg/mypb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type idMap struct {
	job    *JobInfo
	worker *WorkerInfo
}

var (
	DbClient      kvdb.KVDb
	EtcdClient    *clientv3.Client
	WorkerManager *workerManager
	JobManager    *jobManager
	Interval      time.Duration
	Loc           *time.Location
	ScheduleScale time.Duration
	jobNum        uint32
	numLock       sync.Mutex
	jobMap        = make(map[uint32]*idMap)
	mapLock       sync.Mutex
	workerClient  = make(map[string]mypb.JobSchedulerClient)
	workermu      sync.Mutex
	newJob        = make(chan struct{})
	newWorker     = make(chan string)
)

func Initial(redisURI, endpoints, schedulerKey, schedulerURI string, loc *time.Location, interval time.Duration) error {
	// 连接redis客户端
	DbClient = &kvdb.RedisDB{}
	err := DbClient.Connect(redisURI)
	if err != nil {
		mlog.Fatal("Failed to connect to redis", zap.Error(err))
		return err
	}

	// 连接etcd客户端
	EtcdClient, err = myetcd.ConnectToEtcd(endpoints, schedulerKey, schedulerURI)
	if err != nil {
		mlog.Fatal("Failed to connect to etcd", zap.Error(err))
		return err
	}

	// 设置调度的时间间隔
	Interval = interval
	prefetch = Interval / 5
	// 设置时区
	Loc = loc

	// 启动grpc服务
	go startSchedulerGrpc()

	// 启动pprof服务
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

	// 启动httplistener
	go httpListener()

	// 启动job调度
	JobManager = &jobManager{
		jobheap: &jobHeap{},
	}
	// mlog.Infof("length of JobManager.jobheap: %d", len(*JobManager.jobheap))
	go startFetch()
	go registWorker()

	// 启动worker调度
	WorkerManager = &workerManager{
		workerheap: &workerHeap{},
	}
	go assignJob()
	// go fetchJob(dbclient, time.Now().Truncate(time.Minute), time.Minute, jobmanager)

	// mlog.Infof("Scheduler initial successfully, start to work")
	return nil
}
