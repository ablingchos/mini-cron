package myetcd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWatcher(t *testing.T) {
	etcdclient, err := newClient([]string{endpoints})
	assert.NoError(t, err)
	clientURI := "0.0.0.0:65535"

	NewWatcher(etcdclient, clientURI)
}
