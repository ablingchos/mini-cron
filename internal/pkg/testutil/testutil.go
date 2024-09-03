package testutil

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func AssertEqualJSON(t *testing.T, expected, actual interface{}, comments ...interface{}) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetIndent("", "  ")
	err := encoder.Encode(expected)
	assert.NoError(t, err)

	data1 := buffer.String()
	buffer.Reset()

	err = encoder.Encode(actual)
	assert.NoError(t, err)

	data2 := buffer.String()
	assert.EqualValues(t, data1, data2, comments...)
}
