package piazza

import (
	assert "github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
	"testing"
)

func HttpBody(t *testing.T, resp *http.Response) []byte {
	data, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err, "f")
	return data
}
