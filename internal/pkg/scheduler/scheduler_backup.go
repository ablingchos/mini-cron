package scheduler

import (
	"strings"
	"time"

	"git.woa.com/kefuai/my-project/internal/pkg/kvdb"
	"git.woa.com/kefuai/my-project/internal/pkg/myetcd"
)

var (
	// schedulerURI       string
	schedulerTimeout   time.Duration
	schedulerBackupKey = "schedulerBackupURI"
	schedulerBackupURI string
)

func InitialBackup(redisURI, endpoints, schBackupURI string, loc *time.Location, interval, schTimeout, wkTimeout time.Duration) error {
	schedulerBackupURI = schBackupURI
	// schedulerURI = schURI
	idx := strings.LastIndex(schedulerBackupURI, ":")
	// ip = workerURI[:idx-1]
	port = schedulerBackupURI[idx:]

	// 连接redis客户端
	dbClient = &kvdb.RedisDB{}
	err := dbClient.Connect(redisURI)
	if err != nil {
		// mlog.Fatal("Failed to connect to redis", zap.Error(err))
		return err
	}

	// 连接etcd客户端
	EtcdClient, err = myetcd.ConnectToEtcd(endpoints, schedulerBackupKey, schedulerBackupURI)
	if err != nil {
		// mlog.Fatal("Failed to connect to etcd", zap.Error(err))
		return err
	}

	// 设置调度的时间间隔
	Interval = interval
	prefetch = Interval / 5
	// 设置时区
	Loc = loc

	// 监听scheduler是否超时
	workerTimeout = wkTimeout
	schedulerTimeout = schTimeout
	hw := newWatcher(EtcdClient, "scheduler", schedulerTimeout)
	go checkSchedulerTimeout(hw)

	return nil
}
