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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
)

type JsonString string

// ReadFrom is a convenience function that returns the bytes taken from a Reader.
// The reader will be closed if necessary.
func ReadFrom(reader io.Reader) ([]byte, error) {
	switch reader.(type) {
	case io.Closer:
		defer reader.(io.Closer).Close()
	}

	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return data, err
}

func NewErrorf(format string, a ...interface{}) error {
	s := fmt.Sprintf(format, a)
	return errors.New(s)
}

type Pagination struct {
	Count int64 `json:"count" binding:"required"`
	Page int `json:"page" binding:"required"`
	PerPage int `json:"per_page" binding:"required"`	
	SortBy string `json:"sort_by,omitempty"`
	Order string `json:"order,omitempty"`
}
type Common18FListResponse struct {
	Data []interface{} `json:"data" binding:"required"`
	Pagination Pagination `json:"pagination" binding:"required"`	
}