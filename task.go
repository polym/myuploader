package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
)

var (
	// test.user.00517.sql
	partitionReg         = regexp.MustCompile(`(.*)\.([0-9]+)\.sql`)
	errPartitionNotFound = fmt.Errorf("partition not found")
)

type MyUploader struct {
	dir          string
	scanInterval time.Duration
	minio        *MinioBucketCli

	queue      chan string
	doing      map[string]bool
	dumpFinish bool
}

func NewMyUploader(dir string, qsize int, scanInterval time.Duration, m *MinioBucketCli) (*MyUploader, error) {
	return &MyUploader{
		dir:          dir,
		scanInterval: scanInterval,
		minio:        m,

		queue:      make(chan string, qsize),
		doing:      make(map[string]bool),
		dumpFinish: false,
	}, nil
}

func (self *MyUploader) Run() {
	go self.doScan()
	self.doUpload()
}

func (self *MyUploader) doScan() {
	ticker := time.NewTicker(self.scanInterval)
	for _ = range ticker.C {
		finish, err := self.doOneScan()
		if err != nil {
			logrus.Errorf("doScan: %v", err)
		}
		if finish {
			return
		}
	}
}

func (self *MyUploader) doOneScan() (bool, error) {
	files, err := ioutil.ReadDir(self.dir)
	if err != nil {
		return false, fmt.Errorf("ReadDir %s: %v", self.dir, err)
	}

	todoList := make([]string, 0)
	for _, file := range files {
		filename := file.Name()
		if self.doing[filename] {
			continue
		}
		todoList = append(todoList, filename)
	}

	findMetadata := false
	for _, file := range todoList {
		if self.dumpFinish {
			self.doing[file] = true
			self.queue <- file
			continue
		}

		next, _ := findNextPartitionName(file)
		if next != "" {
			nextFile := path.Join(self.dir, next)
			if isFileExist(nextFile) {
				self.doing[file] = true
				self.queue <- file
			}
		}

		if file == "metadata" {
			findMetadata = true
		}
	}

	if findMetadata {
		// mark dump finish and return
		self.dumpFinish = true
		return false, nil
	}

	if self.dumpFinish {
		// close channel after dump finished
		close(self.queue)
	}

	return true, nil
}

func (self *MyUploader) doUpload() {
	for file := range self.queue {
		localPath := path.Join(self.dir, file)
		remotePath := localPath
		for {
			err := self.minio.PutFile(localPath, remotePath)
			if err == nil {
				logrus.Infof("put %s to %s ok", localPath, remotePath)
				// delete local file after upload successfully
				if err = os.RemoveAll(localPath); err != nil {
					logrus.Warnf("delete %s: %v", localPath, err)
				}
				break
			}
			// retry until upload successfully
			logrus.Errorf("put %s to %s: %v", localPath, remotePath, err)
			time.Sleep(time.Second)
		}
	}
}

func findNextPartitionName(name string) (string, error) {
	g := partitionReg.FindAllStringSubmatch(name, -1)
	if len(g) != 1 {
		return "", errPartitionNotFound
	}
	tablePrefix := g[0][1]
	partitionId := g[0][2]

	partitionID, _ := strconv.ParseInt(partitionId, 10, 0)

	strFmt := fmt.Sprintf("%%s.%%0%dd.sql", len(partitionId))
	return fmt.Sprintf(strFmt, tablePrefix, partitionID+1), nil
}

func isFileExist(name string) bool {
	fInfo, _ := os.Stat(name)
	return fInfo != nil
}