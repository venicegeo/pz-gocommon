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

func TestHttp(t *testing.T) {
	assert := assert.New(t)

	// testing of Http{Get,Post,Put,Delete}Json covered by GenericServer_test.go
	// testing of HTTP{Put,Delete} covered by GenericServer_test.go

	assert.True(!false)
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

		paramStrings := map[string]string{}

		actual := &JsonPagination{}
		err := actual.ReadParams(paramStrings, defaults)
		assert.NoError(err)
		assert.EqualValues(*expected, *actual)
	}

	// with some params specified
	{
		expected := JsonPagination{
			PerPage: 100,
			Page:    17,
			Order:   PaginationOrderAscending,
			SortBy:  "id",
		}

		paramStrings := map[string]string{
			"perPage": "100",
			"page":    "17",
		}

		actual := JsonPagination{}
		err := actual.ReadParams(paramStrings, defaults)
		assert.NoError(err)
		assert.EqualValues(expected, actual)
	}

	// with all params specified
	{
		expected := JsonPagination{
			PerPage: 100,
			Page:    17,
			Order:   PaginationOrderDescending,
			SortBy:  "foo",
		}

		paramStrings := map[string]string{
			"perPage": "100",
			"page":    "17",
			"order":   "desc",
			"sortBy":  "foo",
		}

		actual := JsonPagination{}
		err := actual.ReadParams(paramStrings, defaults)
		assert.NoError(err)
		assert.EqualValues(expected, actual)
	}

}
