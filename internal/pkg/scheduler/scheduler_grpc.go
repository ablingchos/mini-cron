package scheduler

import (
	"context"
	"net"

	"git.code.oa.com/red/ms-go/pkg/mlog"
	"github.com/ablingchos/my-project/pkg/mypb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// 添加新worker
// var addNewWorker = make(chan string)

type scheduler struct {
	mypb.UnimplementedJobStatusServer
	mypb.UnimplementedEtcdHelloServer
}

// 通知任务开始执行
// var startCh = make(chan []string)

func (s *scheduler) JobStarted(ctx context.Context, req *mypb.JobStartedRequest) (*mypb.JobStartedResponse, error) {
	// mlog.Debugf("job %s started\n", req.Jobname)
	// 获取JobInfo
	mapLock.Lock()
	if _, ok := jobMap[req.Jobid]; !ok {
		mlog.Errorf("Job %d deleted", req.Jobid)
		mapLock.Unlock()
		return &mypb.JobStartedResponse{Message: "Error"}, nil
	}
	job := jobMap[req.Jobid].job
	mapLock.Unlock()
	// 修改job的状态
	job.status = 2

	return &mypb.JobStartedResponse{Message: "Received"}, nil
}

func (s *scheduler) WorkerHello(ctx context.Context, req *mypb.WorkerHelloRequest) (*mypb.WorkerHelloResponse, error) {
	mlog.Infof("New worker registered, URI: %s", req.WorkerURI)
	newWorker <- req.WorkerURI
	return &mypb.WorkerHelloResponse{Message: "Received"}, nil
}

// 通知任务执行结果
// var resultCh = make(chan []string)

func (s *scheduler) JobCompleted(ctx context.Context, req *mypb.JobCompletedRequest) (*mypb.JobCompletedResponse, error) {
	mapLock.Lock()
	if _, ok := jobMap[req.Jobid]; !ok {
		mlog.Errorf("Job %d was deleted", req.Jobid)
		mapLock.Unlock()
		return &mypb.JobCompletedResponse{Message: "Error"}, nil
	}
	job := jobMap[req.Jobid].job
	mapLock.Unlock()
	mlog.Debugf("job %s completed, id: %d", job.Jobname, job.jobid)

	job.status = 3
	// 向scheduler.recordresult传递任务的执行结果
	// resultCh <- []string{req.Jobname, req.Jobresult}

	return &mypb.JobCompletedResponse{Message: "Received"}, nil
}

// 启动scheduler的grpc服务,将两个服务注册到同一端口上
func startSchedulerGrpc() {
	listner, err := net.Listen("tcp", ":50051")
	if err != nil {
		mlog.Fatal("Failed to listen", zap.Error(err))
		return
	}

	svr := grpc.NewServer()
	mypb.RegisterJobStatusServer(svr, &scheduler{})
	mypb.RegisterEtcdHelloServer(svr, &scheduler{})

	mlog.Infof("Scheduler grpc server listening on port 50051")
	// go RecordResult(resultCh)

	if err := svr.Serve(listner); err != nil {
		mlog.Fatal("Failed to start scheduler server", zap.Error(err))
	}

}

func GrpcSchedulerClient(workerKey, workerURI string) (mypb.EtcdHelloClient, error) {
	conn, err := grpc.Dial(workerURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		mlog.Fatal("Failed to connect to worker server", zap.Error(err))
		return nil, err
	}
	grpcClient := mypb.NewEtcdHelloClient(conn)

	return grpcClient, nil
}
