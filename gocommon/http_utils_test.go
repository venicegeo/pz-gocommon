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

func TestHttp(t *testing.T) {
	assert := assert.New(t)

	// testing of Http{Get,Post,Put,Delete}Json covered by GenericServer_test.go
	// testing of HTTP{Put,Delete} covered by GenericServer_test.go

	assert.True(!false)
}

func TestQueryParams(t *testing.T) {
	assert := assert.New(t)

	addr, err := url.Parse("http://example.com/index.html?a=1&b=2&c=&d=4")
	assert.NoError(err)

	req := http.Request{URL: addr}

	params := NewQueryParams(&req)

	assert.EqualValues(params.Get("a"), "1")
	assert.EqualValues(params.Get("b"), "2")
	assert.EqualValues(params.Get("c"), "")
	assert.EqualValues(params.Get("d"), "4")
	assert.EqualValues(params.Get("e"), "")

	params.Set("f", "6")
	params.Set("g", "")
	assert.EqualValues(params.Get("f"), "6")
	assert.EqualValues(params.Get("g"), "")
}