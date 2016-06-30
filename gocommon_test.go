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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type CommonTester struct {
	suite.Suite
	sys *SystemConfig
}

func TestRunSuite(t *testing.T) {
	s := &CommonTester{}
	suite.Run(t, s)
}

func (suite *CommonTester) SetupSuite() {
	//t := suite.T()
}

func (suite *CommonTester) TearDownSuite() {
}

//--------------------------

func (suite *CommonTester) Test00Nop() {
	t := suite.T()
	assert := assert.New(t)

	assert.True(!false)
}

func (suite *CommonTester) Test01ObjToString() {
	t := suite.T()
	assert := assert.New(t)

	type Foo struct {
		F float32 `json:"ff"`
		G float32 `json:"gg"`
	}
	type Bar struct {
		B bool `json:"bb"`
		N Foo  `json:"nn"`
	}
	obj := Bar{B: true, N: Foo{F: 1.2, G: 2.3}}

	jsn, err := ConvertObjectToJsonString(obj, false)
	assert.NoError(err)

	expected := "{\n    \"bb\": true,\n    \"nn\": {\n        \"ff\": 1.2,\n        \"gg\": 2.3\n    }\n}"
	assert.EqualValues(expected, jsn)

	jsn, err = ConvertObjectToJsonString(obj, true)
	assert.NoError(err)

	expected = "{\"bb\":true,\"nn\":{\"ff\":1.2,\"gg\":2.3}}"
	assert.EqualValues(expected, jsn)
}

func (suite *CommonTester) Test02CompactJson() {
	t := suite.T()
	assert := assert.New(t)

	src := JsonString("{\n    \"bb\": true,\n    \"nn\": {\n        \"ff\": 1.2,\n        \"gg\": 2.3\n    }\n}")
	expected := JsonString("{\"bb\":true,\"nn\":{\"ff\":1.2,\"gg\":2.3}}")

	jsn, err := src.ToCompactJson()
	assert.NoError(err)

	assert.EqualValues(expected, jsn)
}

func (suite *CommonTester) Test03SystemConfig() {
	t := suite.T()
	assert := assert.New(t)

	required := []ServiceName{}

	_, err := NewSystemConfig(PzGoCommon, required)
	assert.NoError(err)
}

func (suite *CommonTester) Test04Services() {
	t := suite.T()
	assert := assert.New(t)

	var err error
	required := []ServiceName{}

	{
		sys, err := NewSystemConfig(PzGoCommon, required)
		assert.NoError(err)

		actual := sys.GetDomain()
		assert.EqualValues(actual, DefaultDomain)

		addr := "1.2.3.4"
		sys.AddService(PzLogger, addr)

		actual, err = sys.GetAddress(PzLogger)
		assert.NoError(err)
		assert.EqualValues(addr, actual)

		actual, err = sys.GetURL(PzLogger)
		assert.NoError(err)
		assert.EqualValues(actual, "http://"+addr+"/v1")
	}

	{
		err = os.Setenv("DOMAIN", "abc.xyz")
		assert.NoError(err)
		defer os.Unsetenv("DOMAIN")
		sys, err := NewSystemConfig(PzGoCommon, required)
		assert.NoError(err)

		actual := sys.GetDomain()
		assert.EqualValues(actual, ".abc.xyz")

		addr := "1.2.3.4"
		sys.AddService(PzLogger, addr)

		actual, err = sys.GetAddress(PzLogger)
		assert.NoError(err)
		assert.EqualValues(addr, actual)

		actual, err = sys.GetURL(PzLogger)
		assert.NoError(err)
		assert.EqualValues(actual, "http://"+addr+"/v1")
	}
}

func (suite *CommonTester) Test05VcapApplication() {
	t := suite.T()
	assert := assert.New(t)

	os.Unsetenv("VCAP_APPLICATION")
	os.Unsetenv("PORT")

	vcap, err := NewVcapApplication()
	assert.NoError(err)

	assert.EqualValues("localhost:0", vcap.GetAddress())
	assert.EqualValues("localhost:0", vcap.GetBindToPort())
	assert.EqualValues("myapplicationname", vcap.GetName())

	env :=
		`{
         "application_id": "14fca253-8087-402e-abf5-8fd40ddda81f",
         "application_name": "pz-workflow",
         "application_uris": [
             "pz-workflow.int.geointservices.io"
         ],
         "application_version": "5f0ee99d-252c-4f8d-b241-bc3e22534afc",
         "limits": {
             "disk": 1024,
             "fds": 16384,
             "mem": 512
         },
         "name": "pz-workflow",
         "space_id": "d65a0987-df00-4d69-a50b-657e52cb2f8e",
         "space_name": "simulator-stage",
         "uris": [
             "pz-workflow.int.geointservices.io"
         ],
         "users": null,
         "version": "5f0ee99d-252c-4f8d-b241-bc3e22534afc"
     }
`
	err = os.Setenv("VCAP_APPLICATION", env)
	assert.NoError(err)
	defer os.Unsetenv("VCAP_APPLICATION")
	err = os.Setenv("PORT", "6280")
	assert.NoError(err)
	defer os.Unsetenv("PORT")

	vcap, err = NewVcapApplication()
	assert.NoError(err)

	assert.EqualValues("pz-workflow"+DefaultDomain, vcap.GetAddress())
	assert.EqualValues(":6280", vcap.GetBindToPort())
	assert.EqualValues("pz-workflow", vcap.GetName())
}

func (suite *CommonTester) Test06VcapServices() {
	t := suite.T()
	assert := assert.New(t)

	os.Unsetenv("VCAP_SERVICES")

	vcap, err := NewVcapServices()
	assert.NoError(err)

	assert.EqualValues("localhost:9092", vcap.Services["pz-kafka"])
	assert.EqualValues("pz-kafka", vcap.UserProvided[1].Name)

	env :=
		`{
			"user-provided": [
				{
					"credentials": {
						"host": "172.32.125.109:9200"
					},
					"label": "user-provided",
    				"name": "pz-elasticsearch",
    				"syslog_drain_url": "",
					"tags": []
				}
				]
			}`

	err = os.Setenv("VCAP_SERVICES", env)
	assert.NoError(err)
	defer os.Unsetenv("VCAP_SERVICES")

	vcap, err = NewVcapServices()
	assert.NoError(err)

	assert.EqualValues("172.32.125.109:9200", vcap.Services["pz-elasticsearch"])
	assert.EqualValues("pz-elasticsearch", vcap.UserProvided[0].Name)
}
