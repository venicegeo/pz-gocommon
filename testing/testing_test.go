package piazza

import (
	assert "github.com/stretchr/testify/assert"
	"net/http"
	"testing"
)

func TestOkay(t *testing.T) {
	resp, err := http.Get("http://www.google.com")
	if err != nil {
		t.Fatalf("admin get failed: %s", err)
	}
	data := HttpBody(t, resp)

	assert.NotEmpty(t, data)
	assert.True(t, len(data) > 100)
}
