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
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

//--------------------------

func TestQueryParams(t *testing.T) {
	assert := assert.New(t)

	addr, err := url.Parse("http://example.com/index.html?a=1&b=foo&c=&d=4")
	assert.NoError(err)

	req := http.Request{URL: addr}

	params := NewQueryParams(&req)

	a, err := params.GetAsInt("a", nil)
	assert.NoError(err)
	assert.NotNil(a)
	assert.Equal(1, *a)

	b, err := params.GetAsString("b", nil)
	assert.NoError(err)
	assert.NotNil(b)
	assert.EqualValues("foo", *b)

	bb, err := params.GetAsString("bb", nil)
	assert.NoError(err)
	assert.Nil(bb)

	s := "bar"
	bbb, err := params.GetAsString("bbb", &s)
	assert.NoError(err)
	assert.NotNil(bbb)
	assert.EqualValues("bar", *bbb)

	c, err := params.GetAsInt("c", nil)
	assert.NoError(err)
	assert.Nil(c)

	i := 7
	cc, err := params.GetAsInt("c", &i)
	assert.NoError(err)
	assert.NotNil(cc)
	assert.EqualValues(7, *cc)
}
