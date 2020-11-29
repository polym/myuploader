package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFindNextPartitionName(t *testing.T) {
	for input, output := range map[string]string{
		"test.user.00517.sql":  "test.user.00518.sql",
		"test.user.000517.sql": "test.user.000518.sql",
		"test.user.000999.sql": "test.user.001000.sql",
	} {
		s2, err := findNextPartitionName(input)
		assert.NoError(t, err)
		assert.Equal(t, output, s2)
	}
}
