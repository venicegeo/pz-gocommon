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
		params := &HttpQueryParams{}

		actual, err := NewJsonPagination(params, defaults)
		assert.NoError(err)

		expected := defaults
		assert.EqualValues(expected, actual)
	}

	// with some params specified
	{
		params := &HttpQueryParams{}
		params.Set("perPage", "100")
		params.Set("page", "17")

		actual, err := NewJsonPagination(params, defaults)
		assert.NoError(err)

		expected := &JsonPagination{
			PerPage: 100,
			Page:    17,
			Order:   PaginationOrderAscending,
			SortBy:  "id",
		}
		assert.EqualValues(expected, actual)
	}

	// with all params specified
	{
		params := &HttpQueryParams{}
		params.Set("perPage", "100")
		params.Set("page", "17")
		params.Set("order", "desc")
		params.Set("sortBy", "foo")

		actual, err := NewJsonPagination(params, defaults)
		assert.NoError(err)

		expected := &JsonPagination{
			PerPage: 100,
			Page:    17,
			Order:   PaginationOrderDescending,
			SortBy:  "foo",
		}
		assert.EqualValues(expected, actual)
	}

}
