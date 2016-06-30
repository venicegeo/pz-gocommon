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

func Test07Server(t *testing.T) {
	assert := assert.New(t)

	required := []ServiceName{}
	sys, err := NewSystemConfig(PzGoCommon, required)
	assert.NoError(err)

	server := GenericServer{sys: sys}

	handleGetRoot := func(c *gin.Context) {
		log.Print("got health-check request")
		c.String(http.StatusOK, "Hi. I'm pz-gocommon.")
	}

	routeData := RouteData{
		Get: map[string]gin.HandlerFunc{
			"/": handleGetRoot,
		},
	}

	_, err = http.Get("http://" + sys.BindTo)
	assert.Error(err)

	err = server.Configure(&routeData)
	assert.NoError(err)
	_ = server.Start()

	resp, err := http.Get("http://" + sys.BindTo)
	assert.NoError(err)
	assert.Equal(200, resp.StatusCode)

	err = server.Stop()
	assert.NoError(err)

	_, err = http.Get("http://" + sys.BindTo)
	assert.Error(err)
}
