package config

import (
	"testing"

	"git.woa.com/kefuai/my-project/internal/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

func TestLoadConfig(t *testing.T) {
	config, err := LoadConfig()
	assert.NoError(t, err)
	testutil.AssertEqualJSON(t, &SchedulerConfig{
		Url: map[string]string{"scheduler": "localhost:50051",
			"scheduler_backup": "localhost:60051"},
		WorkerTimeout:      4,
		SchedulerTimeout:   2,
		SchedulerHeartbeat: 1,
		DbUrl:              "redis://user:Aikefu0530@localhost:6379",
		Interval:           60,
		EtcdEndpoints:      "http://localhost:2379",
		Loc:                "Asia/Shanghai",
		SchedulerKey:       "scheduler",
	}, config.SchedulerConf)
}
