package scheduler

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/ablingchos/my-project/internal/pkg/kvdb"
	"github.com/stretchr/testify/assert"
)

var (
	redisURI     = "redis://user:Aikefu0530@localhost:6379"
	endpoints    = "http://localhost:2379"
	schedulerKey = "schedulerURI"
	schedulerURI = "localhost:50051"
	workerURI    = "localhost:40051"
	interval     = 10 * time.Second
	loc          *time.Location
)

func TestFetchJob(t *testing.T) {
	var dbclient kvdb.KVDb = &kvdb.RedisDB{}
	// dbclient = &kvdb.RedisDB{}
	err := dbclient.Connect(redisURI)
	assert.NoError(t, err)
	DbClient = dbclient

	Loc, err = time.LoadLocation("Asia/Shanghai")
	assert.NoError(t, err)
	Interval = 10 * time.Second

	begintime := time.Now().In(Loc).Add(Interval).Truncate(time.Second)
	JobManager = &jobManager{
		jobheap: &jobHeap{},
	}
	assert.EqualValues(t, 0, len(*JobManager.jobheap))

	key := "get@www.baidu.com@5s"
	var i interface{} = key
	group := begintime.Truncate(Interval).String()
	_, err = dbclient.RPush(context.Background(), group, i)
	assert.NoError(t, err)

	begintime = begintime.In(Loc)
	hash := make(map[string]interface{})
	hash[field1] = "get"
	hash[field2] = begintime.String()
	hash[field3] = "5s"
	err = dbclient.HMSet(context.Background(), key, hash)
	assert.NoError(t, err)

	go startFetch()

	// 等待处理结果
	time.Sleep(Interval)

	assert.EqualValues(t, 1, len(*JobManager.jobheap))

	_, err = dbclient.Del(context.Background(), key)
	assert.NoError(t, err)
	_, err = dbclient.Del(context.Background(), group)
	assert.NoError(t, err)

}

func TestHttpListener(t *testing.T) {
	// url := "http://localhost:8080/"
	// data := "* * * * * *,2023-11-16,15:00:00,get,www.baidu.com"
	// req, err := http.NewRequest("POST", bytes.NewBufferString(data))
	// assert.NoError(t, err)
	var dbclient kvdb.KVDb = &kvdb.RedisDB{}
	err := dbclient.Connect(redisURI)
	assert.NoError(t, err)
	DbClient = dbclient

	go httpListener()

	cmd := exec.Command("curl", "-X", "POST", "-d", "5 * * * * *,2025-11-16,19:25:00,get,www.baidu.com", "http://localhost:8080/")
	_, err = cmd.CombinedOutput()
	assert.NoError(t, err)
	// 等待处理结果
	time.Sleep(time.Second)

	jobname := "get@www.baidu.com@5s"
	check, err := dbclient.HGetAll(context.Background(), jobname)
	assert.NoError(t, err)
	Loc, err := time.LoadLocation("Asia/Shanghai")
	assert.NoError(t, err)
	beginstr := "2025-11-16,19:25:00"
	layout := "2006-01-02,15:04:05"
	begintime, err := time.ParseInLocation(layout, beginstr, Loc)
	assert.NoError(t, err)

	assert.EqualValues(t, "get", check[field1])
	assert.EqualValues(t, begintime.String(), check[field2])
	assert.EqualValues(t, "5s", check[field3])

	_, err = dbclient.Del(context.Background(), jobname)
	assert.NoError(t, err)
}

func TestRecordResult(t *testing.T) {
	var dbclient kvdb.KVDb = &kvdb.RedisDB{}
	err := dbclient.Connect(redisURI)
	assert.NoError(t, err)
	DbClient = dbclient

	resultCh := make(chan []string)
	jobname := "get@www.baidu.com@5s"
	result := "OK"
	resultkey := "result/" + jobname

	resultCh <- []string{jobname, result}
	// 等待处理结果
	time.Sleep(1 * time.Second)

	length, err := dbclient.LLen(context.Background(), resultkey)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, length)

	_, err = dbclient.Del(context.Background(), resultkey)
	assert.NoError(t, err)
}

func TestRegistWorker(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Shanghai")
	assert.NoError(t, err)

	// 连接redis客户端
	DbClient = &kvdb.RedisDB{}
	err = DbClient.Connect(redisURI)
	assert.NoError(t, err)

	// 设置调度的时间间隔
	Initial(redisURI, endpoints, schedulerKey, schedulerURI, loc, interval)

	newWorker <- "localhost:40051"

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

	// 启动httplistener

	select {}
}
