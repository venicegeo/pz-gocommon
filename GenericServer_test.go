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
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

//--------------------------

func Test07Server(t *testing.T) {
	assert := assert.New(t)

	required := []ServiceName{}
	sys, err := NewSystemConfig(PzGoCommon, required)
	assert.NoError(err)

	server := GenericServer{Sys: sys}

	type T struct{ Id int }
	handleGet := func(c *gin.Context) {
		t := T{Id: 17}
		c.JSON(http.StatusOK, t)
	}
	handlePost := func(c *gin.Context) {
		var t T
		err := c.BindJSON(&t)
		assert.NoError(err)
		assert.Equal(7, t.Id)
		c.JSON(http.StatusCreated, T{Id: 13})
	}
	handlePut := func(c *gin.Context) {
		var t T
		err := c.BindJSON(&t)
		assert.NoError(err)
		assert.Equal(32, t.Id)
		c.JSON(http.StatusOK, T{Id: 63})
	}
	handleDelete := func(c *gin.Context) {
		c.JSON(http.StatusOK, T{Id: 45})
	}

	routeData := []RouteData{
		{"GET", "/", handleGet},
		{"POST", "/", handlePost},
		{"PUT", "/", handlePut},
		{"DELETE", "/", handleDelete},
	}

	url := ""

	{
		err = server.Configure(routeData)
		if err != nil {
			assert.FailNow("server failed to configure: %s", err.Error())
		}
		_, err = server.Start()
		if err != nil {
			assert.FailNow("server failed to start: %s", err.Error())
		}

		url = "http://" + sys.BindTo
	}

	{
		output := &T{}
		resp, err := SafeGet(url, output)
		assert.NoError(err)
		assert.Equal(200, resp.StatusCode)
		assert.Equal(17, output.Id)
	}

	{
		input := &T{Id: 7}
		output := &T{}
		resp, err := SafePost(url, input, output)
		assert.NoError(err)
		assert.Equal(201, resp.StatusCode)
		assert.Equal(13, output.Id)
	}

	{
		input := &T{Id: 32}
		output := &T{}
		resp, err := SafePut(url, input, output)
		assert.NoError(err)
		assert.Equal(200, resp.StatusCode)
		assert.Equal(63, output.Id)
	}

	{
		output := &T{}
		resp, err := SafeDelete(url, output)
		assert.NoError(err)
		assert.Equal(200, resp.StatusCode)
		assert.Equal(45, output.Id)
	}

	{
		err = server.Stop()
		assert.NoError(err)

		_, err := http.Get(url)
		assert.Error(err)
	}
}
