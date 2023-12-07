package scheduler

import (
	"net/http"

	"git.code.oa.com/red/ms-go/pkg/mlog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var (
	taskNumber = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "tasks_",
		Name:      "number",
		Help:      "加入系统的所有任务类型的总数量",
	})

	taskToSchedule = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "tasks_",
		Name:      "to_schedule",
		Help:      "待调度的任务的总数量",
	})

	taskDone = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "tasks_",
		Name:      "completed",
		Help:      "已完成任务的总数量",
	})

	taskOverTime = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "tasks_",
		Name:      "overtime",
		Help:      "超时的任务总数量",
	})

	taskRecovered = prometheus.NewGauge(prometheus.GaugeOpts{
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

func initPrometheus(prometheusPort string) {
	prometheus.MustRegister(taskNumber, taskToSchedule, taskDone, taskOverTime, taskRecovered, workerNumber)
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(prometheusPort, nil); err != nil {
			mlog.Errorf("", zap.Error(err))
		}
	}()
}
