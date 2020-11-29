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
	Debug           bool        `json:"debug"`
	Dir             string      `json:"directory"`
	UploadWorkers   int         `json:"uploadWorkers"`
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
		UploadWorkers:   1,
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

	logrus.Infof("config: %+v", cfg)

	mCli, err := NewMinioBucketCli(cfg.MinioOption)
	if err != nil {
		logrus.Fatalf("NewMinioBucketCli %+v: %v", cfg.MinioOption, err)
	}

	if cfg.Debug {
		logrus.SetLevel(logrus.DebugLevel)
	}
	logrus.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		FullTimestamp:   true,
	})

	interval := time.Duration(cfg.ScanIntervalSec) * time.Second
	h, err := NewMyUploader(cfg.Dir, cfg.UploadWorkers, cfg.QueueSize, interval, mCli)
	if err != nil {
		logrus.Fatalf("NewMyUploader: %v", err)
	}

	h.Run()
}
