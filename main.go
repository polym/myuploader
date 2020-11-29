package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/sirupsen/logrus"
)

type Config struct {
	Dir             string      `json:"directory"`
	ScanIntervalSec int         `json:"scanIntervalSec"`
	QueueSize       int         `json:"queueSize"`
	MinioOption     MinioOption `json:"minio"`
}

func parseConfig(path string) (*Config, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("ReadFile %s: %v", path, err)
	}
	cfg := &Config{
		Dir:             "./",
		ScanIntervalSec: 5,
		QueueSize:       100,
		MinioOption: MinioOption{
			Endpoint: "127.0.0.1:9090",
			Bucket:   "polym.xyz",
		},
	}
	err = json.Unmarshal(data, cfg)
	if err != nil {
		return nil, fmt.Errorf("Unmarshal: %v", err)
	}
	return cfg, nil
}

func main() {
	var conf string
	flag.StringVar(&conf, "conf", "config.json", "config path in JSON format")
	flag.Parse()

	cfg, err := parseConfig(conf)
	if err != nil {
		logrus.Fatalf("parseConfig %s: %v", conf, err)
	}

	mCli, err := NewMinioBucketCli(cfg.MinioOption)
	if err != nil {
		logrus.Fatalf("NewMinioBucketCli %+v: %v", cfg.MinioOption, err)
	}

	interval := time.Duration(cfg.ScanIntervalSec) * time.Second
	h, err := NewMyUploader(cfg.Dir, cfg.QueueSize, interval, mCli)
	if err != nil {
		logrus.Fatalf("NewMyUploader: %v", err)
	}

	h.Run()
}
