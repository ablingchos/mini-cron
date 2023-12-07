package scheduler

import (
	"strings"
	"sync"
	"time"

	"net/http"
	_ "net/http/pprof"

	"git.code.oa.com/red/ms-go/pkg/mlog"
	"git.woa.com/kefuai/my-project/internal/pkg/kvdb"
	"git.woa.com/kefuai/my-project/internal/pkg/myetcd"
	"git.woa.com/kefuai/my-project/pkg/mypb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type idMap struct {
	job    *JobInfo
	worker *WorkerInfo
}

var (
	port               string
	schedulerKey       = "schedulerURI"
	dbClient           kvdb.KVDb
	EtcdClient         *clientv3.Client
	WorkerManager      *workerManager
	JobManager         *jobManager
	schedulerHeartbeat time.Duration
	workerTimeout      time.Duration
	Interval           time.Duration
	Loc                *time.Location
	ScheduleScale      time.Duration
	jobNumber          uint32
	numLock            sync.Mutex
	jobMap             = make(map[uint32]*idMap)
	mapLock            sync.RWMutex
	workerClient       = make(map[string]mypb.JobSchedulerClient)
	workerLock         sync.Mutex
	newJob             = make(chan struct{})
	newworker          = make(chan struct{})
	jobList            = make(map[string]bool)
)

func Initial(redisURI, endpoints, schedulerURI string, loc *time.Location, interval, heartbeat, timeout time.Duration) error {
	idx := strings.LastIndex(schedulerURI, ":")
	// ip = workerURI[:idx-1]
	port = schedulerURI[idx:]

	// 连接redis客户端
	dbClient = &kvdb.RedisDB{}
	err := dbClient.Connect(redisURI)
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

	workerTimeout = timeout
	schedulerHeartbeat = heartbeat
	// 开始发送scheduler心跳包
	go myetcd.HeartBeat(EtcdClient, "scheduler", "online", schedulerHeartbeat)

	// 设置调度的时间间隔
	Interval = interval
	prefetch = Interval / 5
	// 设置时区
	Loc = loc

	// 启动grpc服务
	workerTimeout = timeout
	go startSchedulerGrpc()

	// 启动pprof服务
	go func() {
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			mlog.Errorf("Http server error", zap.Error(err))
		}
	}()

	initPrometheus(":4396")

	// 启动httplistener
	go httpListener(":8080")

	// 启动job调度
	JobManager = &jobManager{
		jobheap: &jobHeap{},
	}
	// mlog.Infof("length of JobManager.jobheap: %d", len(*JobManager.jobheap))
	go startFetch()

	// 启动worker调度
	WorkerManager = &workerManager{
		workerheap: &workerHeap{},
	}
	go assignJob()
	// go fetchJob(dbclient, time.Now().Truncate(time.Minute), time.Minute, jobmanager)

	mlog.Infof("Scheduler %s initial successfully, start to work", schedulerURI)
	return nil
}
