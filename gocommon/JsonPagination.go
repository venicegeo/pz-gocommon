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

import "log"

//----------------------------------------------------------

// Elasticsearch, at least, does it this way:
//  - perform the query, giving a huge result set
//  - sort the result set
//  - select out the page you want

type PaginationOrder string

const PaginationOrderAscending PaginationOrder = "asc" // (the default)
const PaginationOrderDescending PaginationOrder = "desc"

type JsonPagination struct {
	Count   int             `json:"count"` // only used when writing output
	Page    int             `json:"page"`
	PerPage int             `json:"perPage"`
	SortBy  string          `json:"sortBy"`
	Order   PaginationOrder `json:"order"`
}

func (p *JsonPagination) StartIndex() int {
	return p.Page * p.PerPage
}

func (p *JsonPagination) EndIndex() int {
	return p.StartIndex() + p.PerPage
}

func NewJsonPagination(params *HttpQueryParams, defalt *JsonPagination) (*JsonPagination, error) {
	log.Printf("NewJsonPagination/1: %#v", params)

	jp := &JsonPagination{}

	perPage, err := params.GetPerPage(&defalt.PerPage)
	if err != nil {
		return nil, err
	}
	if perPage != nil {
		jp.PerPage = *perPage
	}

	page, err := params.GetPage(&defalt.Page)
	if err != nil {
		return nil, err
	}
	if page != nil {
		jp.Page = *page
	}

	sortBy, err := params.GetSortBy(&defalt.SortBy)
	if err != nil {
		return nil, err
	}
	if sortBy != nil {
		jp.SortBy = *sortBy
	}

	order, err := params.GetOrder(&defalt.Order)
	if err != nil {
		return nil, err
	}
	if order != nil {
		jp.Order = *order
	}

	log.Printf("NewJsonPagination/2: %#v", jp)

	return jp, nil
}
