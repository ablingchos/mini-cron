package kvdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	redisURI = "redis://user:Aikefu0530@localhost:6379"
)

func TestConnectToDB(t *testing.T) {
	var dbclient KVDb = &RedisDB{}
	err := dbclient.Connect(redisURI)
	assert.NoError(t, err)

	key := "today"
	hash := make(map[string]interface{})
	hash["weather"] = "sunny"
	hash["name"] = "kefuai"
	err = dbclient.HMSet(context.Background(), key, hash)
	assert.NoError(t, err)

	resp, err := dbclient.HGetAll(context.Background(), key)
	assert.NoError(t, err)
	for k := range hash {
		assert.EqualValues(t, hash[k], resp[k])
	}

	st, err := dbclient.Del(context.Background(), key)
	assert.NoError(t, err)
	assert.EqualValues(t, true, st)
}
