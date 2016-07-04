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
	"log"
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

//--------------------------

type Thing struct {
}

type T struct {
	Id int
}

/*
func (thing *Thing) Get(c *gin.Context, assert *assert.Assertions) *JsonResponse {
	r := &JsonResponse{StatusCode: http.StatusOK, Data: T{Id: 17}}
	return r

}

func (thing *Thing) Post(c *gin.Context, assert *assert.Assertions) *JsonResponse {
	var t T
	err := c.BindJSON(&t)
	assert.NoError(err)
	var r *JsonResponse
	if t.Id == 7 {
		r = &JsonResponse{StatusCode: http.StatusOK, Data: T{Id: 13}}
	} else {
		r = &JsonResponse{StatusCode: http.StatusBadRequest, Message: "eleven"}
	}
	return r
}

func (thing *Thing) Put(c *gin.Context, assert *assert.Assertions) *JsonResponse {
	var t T
	err := c.BindJSON(&t)
	assert.NoError(err)
	assert.Equal(32, t.Id)
	return &JsonResponse{StatusCode: http.StatusOK, Data: T{Id: 63}}
}

func (thing *Thing) Delete(c *gin.Context, assert *assert.Assertions) *JsonResponse {
	return &JsonResponse{StatusCode: http.StatusOK}
}
*/
func Test07Server(t *testing.T) {
	assert := assert.New(t)

	required := []ServiceName{}
	sys, err := NewSystemConfig(PzGoCommon, required)
	assert.NoError(err)

	server := GenericServer{Sys: sys}

	type T struct {
		Id int `json:"id"`
	}
	handleGet := func(c *gin.Context) {
		t := T{Id: 17}
		j := JsonResponse{StatusCode: http.StatusOK, Data: t}
		log.Printf("YYYY %#v     %#v", j, j.Data)
		c.JSON(200, &j)
	}
	handlePost := func(c *gin.Context) {
		var t T
		err := c.BindJSON(&t)
		assert.NoError(err)
		if t.Id == 7 {
			j := &JsonResponse{StatusCode: http.StatusCreated, Data: T{Id: 13}}
			c.JSON(201, j)
		} else {
			assert.Equal(1237, t.Id)
			j := &JsonResponse{StatusCode: http.StatusBadRequest, Data: T{Id: 12313}}
			j.Message = "oops"
			c.JSON(400, j)
		}

	}
	handlePut := func(c *gin.Context) {
		var t T
		err := c.BindJSON(&t)
		assert.NoError(err)
		assert.Equal(32, t.Id)
		j := &JsonResponse{StatusCode: http.StatusOK, Data: T{Id: 63}}
		c.JSON(200, j)

	}
	handleDelete := func(c *gin.Context) {
		j := &JsonResponse{StatusCode: http.StatusOK, Data: T{Id: 45}}
		c.JSON(200, j)

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
		jresp := HttpGetJson(url)
		assert.Equal(200, jresp.StatusCode)

		var out T
		err = SuperConverter(jresp.Data, &out)
		assert.NoError(err)
		assert.Equal(17, out.Id)

	}

	{
		input := &T{Id: 7}
		jresp := HttpPostJson(url, input)
		assert.Equal(201, jresp.StatusCode)

		var out T
		err = SuperConverter(jresp.Data, &out)
		assert.Equal(13, out.Id)
	}

	{
		in := &T{Id: 1237}
		jresp := HttpPostJson(url, in)
		assert.Equal(400, jresp.StatusCode)
		assert.EqualValues("oops", jresp.Message)

		var out T
		err = SuperConverter(jresp.Data, &out)
		assert.Equal(12313, out.Id)
	}

	{
		in := &T{Id: 32}
		jresp := HttpPutJson(url, in)
		assert.Equal(200, jresp.StatusCode)
		var out T
		err = SuperConverter(jresp.Data, &out)
		assert.Equal(63, out.Id)
	}

	{
		jresp := HttpDeleteJson(url)
		assert.Equal(200, jresp.StatusCode)
		var out T
		err = SuperConverter(jresp.Data, &out)
		assert.Equal(45, out.Id)
	}

	{
		err = server.Stop()
		assert.NoError(err)

		_, err := http.Get(url)
		assert.Error(err)
	}
}
