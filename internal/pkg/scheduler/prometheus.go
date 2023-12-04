package scheduler

import (
	"net/http"

	"git.code.oa.com/red/ms-go/pkg/mlog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var (
	jobDone = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "tasks_",
		Name:      "completed",
		Help:      "已完成任务的总数量",
	})

	jobOverTime = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "tasks_",
		Name:      "overtime",
		Help:      "超时的任务总数量",
	})

	jobRecovered = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "tasks_",
		Name:      "recovered",
		Help:      "超时任务的完成数量",
	})

	workerNumber = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "workers_",
		Name:      "number",
		Help:      "加入系统的worker数",
	})
)

func init() {
	prometheus.MustRegister(jobDone, jobOverTime, jobRecovered, workerNumber)
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(":4396", nil); err != nil {
			mlog.Errorf("", zap.Error(err))
		}
	}()
}
