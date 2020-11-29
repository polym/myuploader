package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMinioBucketCli(t *testing.T) {
	var (
		endpoint  = "127.0.0.1:9000"
		key       = "AKIAIOSFODNN7EXAMPLE"
		secret    = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
		bucket    = "polym.xyz"
		localPath = "/etc/hosts"
	)

	cli, err := NewMinioBucketCli(MinioOption{
		Endpoint: endpoint,
		Bucket:   bucket,
		Key:      key,
		Secret:   secret,
	})
	assert.NoError(t, err)

	err = cli.PutFile(localPath, localPath)
	assert.NoError(t, err)
}
