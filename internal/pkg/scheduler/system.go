package scheduler

import (
	"strings"
	"time"

	"git.woa.com/kefuai/my-project/internal/pkg/config"
	"git.woa.com/kefuai/my-project/internal/pkg/kvdb"
	"git.woa.com/kefuai/my-project/internal/pkg/myetcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Scheduler struct {
	Port       string
	DbClient   kvdb.KVDb
	EtcdClient *clientv3.Client
	loc        *time.Location
}

func NewScheduler(name string) (*Scheduler, error) {
	newConfig, err := config.LoadConfig()
	if err != nil {
		return nil, err
	}

	schedulerConf := newConfig.SchedulerConf
	newScheduler := &Scheduler{}

	if _, ok := schedulerConf.GetUrl()[name]; !ok {
		return nil, err
	}

	schedulerUrl := schedulerConf.GetUrl()[name]
	idx := strings.LastIndex(schedulerUrl, ":")
	newScheduler.Port = schedulerUrl[idx:]

	// 连接db
	err = newScheduler.DbClient.Connect(schedulerConf.DbUrl)
	if err != nil {
		// mlog.Fatal("Failed to connect to redis", zap.Error(err))
		return nil, err
	}

	// 连接etcd
	newScheduler.EtcdClient, err = myetcd.ConnectToEtcd(schedulerConf.EtcdEndpoints, schedulerConf.SchedulerKey, schedulerUrl)
	if err != nil {
		return nil, util.
	}

	// 设置时区
	newScheduler.loc, err = time.LoadLocation(schedulerConf.Loc)
	if err != nil {
		return nil, err
	}

	return newScheduler, nil
	// scheduler.schedulerKey =
}
