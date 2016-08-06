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

import "fmt"

//----------------------------------------------------------

// Elasticsearch, at least, does it this way:
//  - perform the query, giving a huge result set
//  - sort the result set
//  - select out the page you want

// Constants indicating ascending (1,2,3) or descending (3,2,1) order.
type SortOrder string

const (
	SortOrderAscending  SortOrder = "asc"
	SortOrderDescending SortOrder = "desc"
)

type JsonPagination struct {
	Count   int       `json:"count"` // only used when writing output
	Page    int       `json:"page"`
	PerPage int       `json:"perPage"`
	SortBy  string    `json:"sortBy"`
	Order   SortOrder `json:"order"`
}

func (p *JsonPagination) StartIndex() int {
	return p.Page * p.PerPage
}

func (p *JsonPagination) EndIndex() int {
	return p.StartIndex() + p.PerPage
}

func NewJsonPagination(params *HttpQueryParams, defalt *JsonPagination) (*JsonPagination, error) {

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

	order, err := params.GetSortOrder(&defalt.Order)
	if err != nil {
		return nil, err
	}
	if order != nil {
		jp.Order = *order
	}

	return jp, nil
}

func (format JsonPagination) Defaults() *JsonPagination {
	return &JsonPagination{
		PerPage: 10,
		Page:    0,
		Order:   SortOrderDescending,
		SortBy:  "createdOn",
	}
}

func (format *JsonPagination) ToParamString() string {
	s := fmt.Sprintf("perPage=%d&page=%d&sortBy=%s&order=%s",
		format.PerPage, format.Page, format.SortBy, format.Order)
	return s
}
