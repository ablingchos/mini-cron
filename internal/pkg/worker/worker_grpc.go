package worker

import (
	"container/heap"
	"context"
	"net"
	"time"

	"git.code.oa.com/red/ms-go/pkg/mlog"
	"git.woa.com/kefuai/my-project/pkg/mypb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type worker struct {
	mypb.UnimplementedJobSchedulerServer
	mypb.UnimplementedSchedulerSwitchServer
}

var newjobCh = make(chan struct{})

func (w *worker) DispatchJob(ctx context.Context, req *mypb.DispatchJobRequest) (*mypb.DispatchJobResponse, error) {
	go parseJob(req)
	return &mypb.DispatchJobResponse{Message: "Job received"}, nil
}

func (w *worker) NewScheduler(ctx context.Context, req *mypb.SchedulerSwitchRequest) (*mypb.SchedulerSwitchResponse, error) {
	// 获取scheduler的job状态上报服务客户端
	var err error
	clientLock.Lock()
	jobStatusClient, err = jobStatus(req.SchedulerURI)
	clientLock.Unlock()

	if err != nil {
		mlog.Errorf("Failed to switch scheduler", zap.Error(err))
	}
	return &mypb.SchedulerSwitchResponse{Message: "Scheduler changed"}, err
}

func etcdHello(schedulerKey, schedulerURI string) (mypb.EtcdHelloClient, error) {
	conn, err := grpc.Dial(schedulerURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	grpcClient := mypb.NewEtcdHelloClient(conn)

	return grpcClient, nil
}

func StartWorkerGrpc() {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		mlog.Error("Failed to listen", zap.Error(err))
		return
	}

	svr := grpc.NewServer()
	mypb.RegisterJobSchedulerServer(svr, &worker{})
	mlog.Infof("Grpc server listening on port %s", port)

	if err := svr.Serve(listener); err != nil {
		mlog.Fatal("Failed to start worker server", zap.Error(err))
	}
}

func jobStatus(schedulerURI string) (mypb.JobStatusClient, error) {
	conn, err := grpc.Dial(schedulerURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	grpcClient := mypb.NewJobStatusClient(conn)

	return grpcClient, err
}

func parseJob(req *mypb.DispatchJobRequest) {
	nextexectime, err := time.ParseInLocation("2006-01-02 15:04:05 -0700 MST", req.JobInfo.NextExecTime, Loc)
	if err != nil {
		mlog.Error("Failed to parse time", zap.Error(err))
		return
	}
	duration, err := time.ParseDuration(req.JobInfo.Interval)
	if err != nil {
		mlog.Infof("Failed to parse duration", zap.Error(err))
		return
	}

	JobManager.mutex.Lock()
	heap.Push(JobManager.jobheap, &JobInfo{
		jobid:        req.JobInfo.Jobid,
		JobName:      req.JobInfo.Jobname,
		NextExecTime: nextexectime,
		Interval:     duration,
		status:       make(chan struct{}),
	})
	JobManager.mutex.Unlock()
	// mlog.Debugf("job %s received, begintime: %s, jobid: %d", req.JobInfo.Jobname, nextexectime.String(), req.JobInfo.Jobid)

	newjobCh <- struct{}{}
}
