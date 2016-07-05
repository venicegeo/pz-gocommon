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
	"fmt"
	"log"
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

//--------------------------

type ThingService struct {
	assert  *assert.Assertions
	Data    map[string]string `json:"data"`
	IdCount int
}

type Thing struct {
	Id    string `json:"id"`
	Value string `json:"value"`
}

func (service *ThingService) GetThing(id string) *JsonResponse {
	val, ok := service.Data[id]
	if !ok {
		return &JsonResponse{StatusCode: http.StatusNotFound}
	}
	return &JsonResponse{StatusCode: http.StatusOK, Data: Thing{Id: id, Value: val}}
}

func (service *ThingService) PostThing(thing *Thing) *JsonResponse {
	if thing.Value == "NULL" {
		resp := &JsonResponse{StatusCode: http.StatusBadRequest, Message: "oops"}
		return resp
	}
	service.IdCount++
	thing.Id = fmt.Sprintf("%d", service.IdCount)
	service.Data[thing.Id] = thing.Value
	return &JsonResponse{StatusCode: http.StatusCreated, Data: thing}
}

func (service *ThingService) PutThing(id string, thing *Thing) *JsonResponse {
	if thing.Value == "NULL" {
		return &JsonResponse{StatusCode: http.StatusBadRequest, Message: "oops"}
	}
	if thing.Id != id {
		return &JsonResponse{StatusCode: http.StatusBadRequest, Message: "oops - id mismatch"}
	}
	service.Data[thing.Id] = thing.Value
	log.Printf("BBB %s %#v", id, thing)

	return &JsonResponse{StatusCode: http.StatusOK, Data: thing}
}

func (service *ThingService) DeleteThing(id string) *JsonResponse {
	_, ok := service.Data[id]
	if !ok {
		return &JsonResponse{StatusCode: http.StatusNotFound}
	}
	delete(service.Data, id)
	return &JsonResponse{StatusCode: http.StatusOK}
}

func Test07Server(t *testing.T) {
	assert := assert.New(t)

	required := []ServiceName{}
	sys, err := NewSystemConfig(PzGoCommon, required)
	assert.NoError(err)

	server := GenericServer{Sys: sys}

	service := ThingService{
		assert:  assert,
		IdCount: 0,
		Data:    make(map[string]string),
	}

	handleGetRoot := func(c *gin.Context) {
		type T struct {
			Message string
		}
		message := "Hi."
		resp := JsonResponse{StatusCode: http.StatusOK, Data: message}
		c.JSON(resp.StatusCode, resp)
	}
	handleGet := func(c *gin.Context) {
		id := c.Param("id")
		resp := service.GetThing(id)
		c.JSON(resp.StatusCode, resp)
	}
	handlePost := func(c *gin.Context) {
		var thing Thing
		err := c.BindJSON(&thing)
		assert.NoError(err)
		resp := service.PostThing(&thing)
		c.JSON(resp.StatusCode, resp)
	}
	handlePut := func(c *gin.Context) {
		id := c.Param("id")
		var thing Thing
		err := c.BindJSON(&thing)
		assert.NoError(err)
		thing.Id = id
		log.Printf("AAA %s %#v", id, thing)
		resp := service.PutThing(id, &thing)
		log.Printf("CCC %s %#v", id, resp.Data)

		c.JSON(resp.StatusCode, resp)
	}
	handleDelete := func(c *gin.Context) {
		id := c.Param("id")
		resp := service.DeleteThing(id)
		c.JSON(resp.StatusCode, resp)
	}

	routeData := []RouteData{
		{"GET", "/", handleGetRoot},
		{"GET", "/:id", handleGet},
		{"POST", "/", handlePost},
		{"PUT", "/:id", handlePut},
		{"DELETE", "/:id", handleDelete},
	}

	url := ""

	{
		err = server.Configure(routeData)
		if err != nil {
			assert.FailNow("server failed to configure: " + err.Error())
		}
		_, err = server.Start()
		if err != nil {
			assert.FailNow("server failed to start: " + err.Error())
		}

		url = "http://" + sys.BindTo
	}

	var input *Thing
	var output *Thing
	var jresp *JsonResponse

	// GET bad
	jresp = HttpGetJson(url + "/mpg")
	assert.Equal(404, jresp.StatusCode)

	// POST 1
	input = &Thing{Value: "17"}
	jresp = HttpPostJson(url, input)
	assert.Equal(201, jresp.StatusCode)

	err = SuperConverter(jresp.Data, &output)
	assert.EqualValues("1", output.Id)
	assert.EqualValues("17", output.Value)

	// POST bad
	input = &Thing{Value: "NULL"}
	jresp = HttpPostJson(url, input)
	assert.Equal(400, jresp.StatusCode)

	// POST 2
	input = &Thing{Value: "18"}
	jresp = HttpPostJson(url, input)
	assert.Equal(201, jresp.StatusCode)

	err = SuperConverter(jresp.Data, &output)
	assert.EqualValues("2", output.Id)
	assert.EqualValues("18", output.Value)

	// GET 2
	jresp = HttpGetJson(url + "/2")
	assert.Equal(200, jresp.StatusCode)

	err = SuperConverter(jresp.Data, &output)
	assert.NoError(err)
	assert.EqualValues("2", output.Id)
	assert.EqualValues("18", output.Value)

	// PUT 1
	input = &Thing{Value: "71"}
	jresp = HttpPutJson(url+"/1", input)
	assert.Equal(200, jresp.StatusCode)
	err = SuperConverter(jresp.Data, &output)
	assert.NoError(err)
	assert.EqualValues("71", output.Value)

	// GET 1
	jresp = HttpGetJson(url + "/1")
	assert.Equal(200, jresp.StatusCode)

	err = SuperConverter(jresp.Data, &output)
	assert.NoError(err)
	assert.EqualValues("1", output.Id)
	assert.EqualValues("71", output.Value)

	// DELETE 3
	jresp = HttpDeleteJson(url + "/3")
	assert.Equal(404, jresp.StatusCode)

	// DELETE 1
	jresp = HttpDeleteJson(url + "/1")
	assert.Equal(200, jresp.StatusCode)

	// GET 1
	jresp = HttpGetJson(url + "/1")
	assert.Equal(404, jresp.StatusCode)

	{
		err = server.Stop()
		assert.NoError(err)

		_, err := http.Get(url)
		assert.Error(err)
	}
}
