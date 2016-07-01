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
	"testing"

	"github.com/stretchr/testify/assert"
)

//--------------------------

func Test01ObjToString(t *testing.T) {
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

func Test02CompactJson(t *testing.T) {
	assert := assert.New(t)

	src := JsonString("{\n  \"bb\": true,\n  \"nn\": {\n        \"ff\": 1.2,\n     \"gg\": 2.3\n    }\n}")
	expected := JsonString("{\"bb\":true,\"nn\":{\"ff\":1.2,\"gg\":2.3}}")

	jsn, err := src.ToCompactJson()
	assert.NoError(err)

	assert.EqualValues(expected, jsn)
}
