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
	"encoding/json"
	"log"
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

func TestMarshalling(t *testing.T) {
	assert := assert.New(t)

	a := &JsonResponse{
		StatusCode: 10,
	}

	byts, err := json.Marshal(a)
	assert.NoError(err)
	log.Printf("%s", string(byts))
	assert.EqualValues("{\"statusCode\":10}", string(byts))

	b := &JsonResponse{}
	err = json.Unmarshal(byts, b)
	assert.NoError(err)
	assert.EqualValues(a, b)
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

func TestPagination(t *testing.T) {
	assert := assert.New(t)

	p := JsonPagination{
		PerPage: 10,
		Page:    32,
		Order:   PaginationOrderDescending,
		SortBy:  "id",
	}

	assert.Equal(320, p.StartIndex())
	assert.Equal(330, p.EndIndex())
}

func TestPaginationParams(t *testing.T) {
	assert := assert.New(t)

	defaults := &JsonPagination{
		PerPage: 10,
		Page:    0,
		Order:   PaginationOrderAscending,
		SortBy:  "id",
	}

	// with no params specified
	{
		expected := defaults

		params := &HttpQueryParams{}

		actual, err := NewJsonPagination(params, defaults)
		assert.NoError(err)
		assert.EqualValues(expected, actual)
	}

	// with some params specified
	{
		expected := &JsonPagination{
			PerPage: 100,
			Page:    17,
			Order:   PaginationOrderAscending,
			SortBy:  "id",
		}

		params := &HttpQueryParams{}
		params.Set("perPage", "100")
		params.Set("page", "17")

		actual, err := NewJsonPagination(params, defaults)
		assert.NoError(err)
		assert.EqualValues(expected, actual)
	}

	// with all params specified
	{
		expected := &JsonPagination{
			PerPage: 100,
			Page:    17,
			Order:   PaginationOrderDescending,
			SortBy:  "foo",
		}

		params := &HttpQueryParams{}
		params.Set("perPage", "100")
		params.Set("page", "17")
		params.Set("order", "desc")
		params.Set("sortBy", "foo")

		actual, err := NewJsonPagination(params, defaults)
		assert.NoError(err)
		assert.EqualValues(expected, actual)
	}

}
