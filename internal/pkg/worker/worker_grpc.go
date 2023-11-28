package worker

import (
	"context"
	"net"
	"time"

	"git.code.oa.com/red/ms-go/pkg/mlog"
	"github.com/ablingchos/my-project/pkg/mypb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type worker struct {
	mypb.UnimplementedJobSchedulerServer
}

var newjobCh = make(chan *JobInfo)

func parseTime(req *mypb.DispatchJobRequest) {
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
	newjobCh <- &JobInfo{
		jobid:        req.JobInfo.Jobid,
		JobName:      req.JobInfo.Jobname,
		NextExecTime: nextexectime,
		Interval:     duration,
	}
}

func (w *worker) DispatchJob(ctx context.Context, req *mypb.DispatchJobRequest) (*mypb.DispatchJobResponse, error) {
	mlog.Infof("job %s received, begintime: %s", req.JobInfo.Jobname, req.JobInfo.NextExecTime)
	go parseTime(req)
	return &mypb.DispatchJobResponse{Message: "Job received"}, nil
}

func StartWorkerGrpc() {
	listener, err := net.Listen("tcp", ":40051")
	if err != nil {
		mlog.Error("Failed to listen", zap.Error(err))
		return
	}

	svr := grpc.NewServer()
	mypb.RegisterJobSchedulerServer(svr, &worker{})
	mlog.Infof("Grpc server listening on port 40051")

	if err := svr.Serve(listener); err != nil {
		mlog.Fatal("Failed to start worker server", zap.Error(err))
	}
}

func etcdHello(schedulerKey, schedulerURI string) (mypb.EtcdHelloClient, error) {
	conn, err := grpc.Dial(schedulerURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	grpcClient := mypb.NewEtcdHelloClient(conn)

	return grpcClient, nil
}

func jobStatus(schedulerURI string) (mypb.JobStatusClient, error) {
	conn, err := grpc.Dial(schedulerURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	grpcClient := mypb.NewJobStatusClient(conn)

	return grpcClient, err
}
