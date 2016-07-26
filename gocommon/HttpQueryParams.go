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
	"net/http"
	"strconv"
	"strings"
	"time"
)

//----------------------------------------------------------

// We don't want to pass http.Request objects into the Services classes
// (and certainly not a gin.Context!), and we need this kind of information
// a lot, so we'll keep a special data structure which actually understands
// the semantics as well as the syntax.

type HttpQueryParams struct {
	raw map[string]string
}

func NewQueryParams(request *http.Request) *HttpQueryParams {
	params := HttpQueryParams{raw: make(map[string]string)}

	for k, v := range request.URL.Query() {
		params.raw[k] = v[0]
	}
	return &params
}

func (params *HttpQueryParams) AddString(key string, value string) {
	if params.raw == nil {
		params.raw = make(map[string]string)
	}
	params.raw[key] = value
}

func (params *HttpQueryParams) AddTime(key string, value time.Time) {
	if params.raw == nil {
		params.raw = make(map[string]string)
	}
	params.raw[key] = value.Format(time.RFC3339)
}

func (params *HttpQueryParams) GetAsInt(key string, defalt *int) (*int, error) {
	if key == "" {
		return defalt, nil
	}

	value, ok := params.raw[key]
	if !ok || value == "" {
		return defalt, nil
	}

	i, err := strconv.Atoi(value)
	if err != nil {
		return nil, err
	}

	return &i, nil
}

func (params *HttpQueryParams) GetAsString(key string, defalt *string) (*string, error) {
	if key == "" {
		return defalt, nil
	}

	value, ok := params.raw[key]
	if !ok || value == "" {
		return defalt, nil
	}

	s := value
	return &s, nil
}

func (params *HttpQueryParams) GetAsOrder(key string, defalt *PaginationOrder) (*PaginationOrder, error) {
	if key == "" {
		return defalt, nil
	}

	value, ok := params.raw[key]
	if !ok || value == "" {
		return defalt, nil
	}

	var order PaginationOrder
	switch strings.ToLower(value) {
	case "desc":
		order = PaginationOrderDescending
	case "asc":
		order = PaginationOrderDescending
	default:
		s := fmt.Sprintf("query argument for '?%s' must be \"asc\" or \"desc\"", value)
		err := errors.New(s)
		return nil, err
	}

	return &order, nil
}

func (params *HttpQueryParams) GetAsTime(key string, defalt *time.Time) (*time.Time, error) {
	if key == "" {
		return defalt, nil
	}

	value, ok := params.raw[key]
	if !ok || value == "" {
		return defalt, nil
	}
	//log.Printf("GETASTIME: %s", value)
	//log.Printf("         : %s", key)

	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, err
	}

	return &t, nil
}

func (params *HttpQueryParams) GetAfter(defalt *time.Time) (*time.Time, error) {
	return params.GetAsTime("after", defalt)
}

func (params *HttpQueryParams) GetBefore(defalt *time.Time) (*time.Time, error) {
	return params.GetAsTime("before", defalt)
}

func (params *HttpQueryParams) GetCount(defalt *int) (*int, error) {
	return params.GetAsInt("count", defalt)
}

func (params *HttpQueryParams) GetOrder(defalt *PaginationOrder) (*PaginationOrder, error) {
	return params.GetAsOrder("order", defalt)
}

func (params *HttpQueryParams) GetPage(defalt *int) (*int, error) {
	return params.GetAsInt("page", defalt)
}

func (params *HttpQueryParams) GetPerPage(defalt *int) (*int, error) {
	return params.GetAsInt("perPage", defalt)
}

func (params *HttpQueryParams) GetSortBy(defalt *string) (*string, error) {
	return params.GetAsString("sortBy", defalt)
}

func (params *HttpQueryParams) ToParamString() string {
	if params == nil || params.raw == nil {
		return ""
	}

	s := ""

	for key, value := range params.raw {
		if s != "" {
			s += "&"
		}
		s += fmt.Sprintf("%s=\"%s\"", key, value)
	}
	return s
}
