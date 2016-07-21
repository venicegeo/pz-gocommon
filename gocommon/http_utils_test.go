// Copyright 2016, RadiantBlue Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package piazza

import (
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

//--------------------------

func TestHttp(t *testing.T) {
	assert := assert.New(t)

	// testing of Http{Get,Post,Put,Delete}Json covered by GenericServer_test.go
	// testing of HTTP{Put,Delete} covered by GenericServer_test.go

	assert.True(!false)
}

func TestQueryParams(t *testing.T) {
	assert := assert.New(t)

	addr, err := url.Parse("http://example.com/index.html?a=1&b=foo&c=&d=4")
	assert.NoError(err)

	req := http.Request{URL: addr}

	params := NewQueryParams(&req)

	a, err := params.AsInt("a", nil)
	assert.NoError(err)
	assert.NotNil(a)
	assert.Equal(1, *a)

	b, err := params.AsString("b", nil)
	assert.NoError(err)
	assert.NotNil(b)
	assert.EqualValues("foo", *b)

	bb, err := params.AsString("bb", nil)
	assert.NoError(err)
	assert.Nil(bb)

	s := "bar"
	bbb, err := params.AsString("bbb", &s)
	assert.NoError(err)
	assert.NotNil(bbb)
	assert.EqualValues("bar", *bbb)

	c, err := params.AsInt("c", nil)
	assert.NoError(err)
	assert.Nil(c)

	i := 7
	cc, err := params.AsInt("c", &i)
	assert.NoError(err)
	assert.NotNil(cc)
	assert.EqualValues(7, *cc)
}

func fileExists(s string) bool {
	if _, err := os.Stat(s); os.IsNotExist(err) {
		return false
	}
	return true
}

func TestApiKey(t *testing.T) {
	assert := assert.New(t)

	var err error
	var key string

	// will it read from $PZKEY?
	{
		err = os.Setenv("PZKEY", "yow")
		assert.NoError(err)

		key, err = GetApiKey()
		assert.NoError(err)
		assert.EqualValues(key, "yow")

		os.Unsetenv("PZKEY")
	}

	path := os.Getenv("HOME")
	assert.True(path != "")

	path += "/.pzkey"

	// will it read $HOME/.pzkey if $PZKEY not set?
	// (note the test can't control whether $HOME/.pzkey actually exists or not)

	if fileExists(path) {
		key, err = GetApiKey()
		assert.NoError(err)

		raw, err := ioutil.ReadFile(path)
		actual := strings.TrimSpace(string(raw))
		assert.NoError(err)

		assert.EqualValues(actual, key)
	} else {
		key, err = GetApiKey()
		assert.Error(err)
	}
}
