package myetcd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	endpoints    = "http://localhost:2379"
	schedulerURI = "localhost:50051"
)

func TestEtcd(t *testing.T) {
	key := "test"
	value := "success"
	etcdclient, err := ConnectToEtcd(endpoints, key, schedulerURI)
	assert.NoError(t, err)

	resp, err := etcdclient.Get(context.Background(), key)
	assert.NoError(t, err)
	for _, v := range resp.Kvs {
		assert.EqualValues(t, value, v.Value)
	}

	_, err = etcdclient.Delete(context.Background(), key)
	assert.NoError(t, err)
}
