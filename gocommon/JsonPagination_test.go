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

func TestPagination(t *testing.T) {
	assert := assert.New(t)

	p := JsonPagination{
		PerPage: 10,
		Page:    32,
		Order:   SortOrderDescending,
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
		Order:   SortOrderDescending,
		SortBy:  "createdOn",
	}

	// with no params specified
	{
		url, err := url.Parse("http://example.com")
		assert.NoError(err)
		req := http.Request{URL: url}

		params := NewQueryParams(&req)

		actual, err := NewJsonPagination(params)
		assert.NoError(err)
		expected := defaults
		assert.EqualValues(expected, actual)
	}

	// with some params specified
	{
		url, err := url.Parse("http://example.com?perPage=100&page=17")
		assert.NoError(err)
		req := http.Request{URL: url}

		params := NewQueryParams(&req)

		actual, err := NewJsonPagination(params)
		assert.NoError(err)

		expected := &JsonPagination{
			PerPage: 100,
			Page:    17,
			Order:   SortOrderDescending,
			SortBy:  "createdOn",
		}
		assert.EqualValues(expected, actual)
	}

	// with all params specified
	{
		url, err := url.Parse("http://example.com?perPage=100&page=17&order=asc&sortBy=foo")
		assert.NoError(err)
		req := http.Request{URL: url}

		params := NewQueryParams(&req)

		actual, err := NewJsonPagination(params)
		assert.NoError(err)

		expected := &JsonPagination{
			PerPage: 100,
			Page:    17,
			Order:   SortOrderAscending,
			SortBy:  "foo",
		}
		assert.EqualValues(expected, actual)
	}

}
