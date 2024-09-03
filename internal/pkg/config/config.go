package config

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

const (
	path = "scheduler_config.json"
)

type ParsedConfig struct {
	SchedulerConf *SchedulerConfig
}

func LoadConfig() (*ParsedConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("failed to read file")
		return nil, err
	}

	config := &ParsedConfig{
		SchedulerConf: &SchedulerConfig{},
	}
	err = json.Unmarshal(data, config.SchedulerConf)
	if err != nil {
		log.Printf("failed to unmarshal JSON")
		return nil, err
	}

	fmt.Println(config)
	return config, nil
}
