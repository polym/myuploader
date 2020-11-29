package main

import (
	"context"
	"fmt"
	"path"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MinioBucketCli struct {
	bucket string
	prefix string
	mCli   *minio.Client
}

type MinioOption struct {
	Endpoint string `json:"endpoint"`
	Bucket   string `json:"bucket"`
	Prefix   string `json:"prefix"`
	Key      string `json:"key"`
	Secret   string `json:"secret"`
}

func NewMinioBucketCli(opt MinioOption) (*MinioBucketCli, error) {
	ctx := context.TODO()

	minioClient, err := minio.New(opt.Endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(opt.Key, opt.Secret, ""),
	})
	if err != nil {
		return nil, fmt.Errorf("new minio client: %v", err)
	}

	ok, err := minioClient.BucketExists(ctx, opt.Bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket %s: %v", opt.Bucket, err)
	}
	if !ok {
		err = minioClient.MakeBucket(ctx, opt.Bucket, minio.MakeBucketOptions{})
		if err != nil {
			return nil, fmt.Errorf("create bucket %s: %v", opt.Bucket, err)
		}
	}

	return &MinioBucketCli{bucket: opt.Bucket, prefix: opt.Prefix, mCli: minioClient}, nil
}

func (self *MinioBucketCli) PutFile(localPath, remotePath string) error {
	ctx := context.TODO()
	// remove leading slash
	remotePath = path.Join("/", self.prefix, remotePath)[1:]
	_, err := self.mCli.FPutObject(ctx, self.bucket, remotePath, localPath, minio.PutObjectOptions{})
	return err
}
