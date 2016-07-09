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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

//----------------------------------------------------------

type Ident string

const NoIdent Ident = ""

func (id Ident) String() string {
	return string(id)
}

//----------------------------------------------------------

const (
	// ContentTypeJSON is the http content-type for JSON.
	ContentTypeJSON = "application/json"

	// ContentTypeText is the http content-type for plain text.
	ContentTypeText = "text/plain"
)

//----------------------------------------------------------

// Put, because there is no http.Put.
func HTTPPut(url string, contentType string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", contentType)
	client := &http.Client{}
	return client.Do(req)
}

// Delete, because there is no http.Delete.
func HTTPDelete(url string) (*http.Response, error) {
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return nil, err
	}

	client := &http.Client{}
	return client.Do(req)
}

//----------------------------------------------------------

// We don't want to pass http.Request objects into the Services classes
// (and certainly not a gin.Context!), so we provide a simple map.

type HttpQueryParams map[string]string

func NewQueryParams(request *http.Request) *HttpQueryParams {
	params := make(HttpQueryParams)

	for k, v := range request.URL.Query() {
		params.Set(k, v[0])
	}

	return &params
}

func (params *HttpQueryParams) Set(key string, value string) {
	(*params)[key] = value
}

func (params *HttpQueryParams) IsPresent(key string) bool {
	return (*params)[key] == ""
}

// returns "" if key not present OR if value not given (e.g. "foo=1&key=&bar=2")
func (params *HttpQueryParams) Get(key string) string {
	return (*params)[key]
}

func (params *HttpQueryParams) GetInt(key string, defalt int) (int, error) {
	str := params.Get(key)
	if str == "" {
		return defalt, nil
	}

	value, err := strconv.Atoi(str)
	if err != nil {
		s := fmt.Sprintf("query argument for '?%s' is invalid: %s (%s)", key, str, err.Error())
		err := errors.New(s)
		return -1, err
	}

	return value, nil
}

func (params *HttpQueryParams) GetString(key string, defalt string) (string, error) {
	str := params.Get(key)
	if str == "" {
		return defalt, nil
	}
	return str, nil
}

func (params *HttpQueryParams) GetOrder(key string, defalt PaginationOrder) (PaginationOrder, error) {
	str := params.Get(key)
	if str == "" {
		return defalt, nil
	}

	switch strings.ToLower(str) {
	case "desc":
		return PaginationOrderDescending, nil
	case "asc":
		return PaginationOrderAscending, nil
	}

	s := fmt.Sprintf("query argument for '?%s' must be \"asc\" or \"desc\"", key)
	err := errors.New(s)
	return PaginationOrderAscending, err
}

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

	perPage, err := params.GetInt("perPage", defalt.PerPage)
	if err != nil {
		return nil, err
	}

	page, err := params.GetInt("page", defalt.Page)
	if err != nil {
		return nil, err
	}

	sortBy, err := params.GetString("sortBy", defalt.SortBy)
	if err != nil {
		return nil, err
	}

	order, err := params.GetOrder("order", defalt.Order)
	if err != nil {
		return nil, err
	}

	p := &JsonPagination{
		PerPage: perPage,
		Page:    page,
		SortBy:  sortBy,
		Order:   order,
	}

	return p, nil
}

//----------------------------------------------------------

type JsonString string

type JsonResponse struct {
	StatusCode int `json:"statusCode" binding:"required"`

	// only 2xxx -- Data is required
	Data       interface{}     `json:"data,omitempty" binding:"required"`
	Pagination *JsonPagination `json:"pagination,omitempty"` // optional

	// only 4xx and 5xx -- Message is required
	Message string        `json:"message,omitempty"`
	Origin  string        `json:"origin,omitempty"` // optional
	Inner   *JsonResponse `json:"inner,omitempty"`  // optional

	// optional
	Metadata interface{} `json:"metadata,omitempty"`
}

func (resp *JsonResponse) String() string {
	s := fmt.Sprintf("StatusCode: %d\nData: %#v\nMessage: %s",
		resp.StatusCode, resp.Data, resp.Message)
	return s
}

func (resp *JsonResponse) IsError() bool {
	return resp.StatusCode >= 400 && resp.StatusCode <= 599
}

func (resp *JsonResponse) ToError() error {
	if !resp.IsError() {
		return nil
	}
	return errors.New(resp.String())
}

func newJsonResponse500(err error) *JsonResponse {
	return &JsonResponse{StatusCode: http.StatusInternalServerError, Message: err.Error()}
}

func toJsonResponse(resp *http.Response) *JsonResponse {
	if resp.ContentLength == 0 {
		return &JsonResponse{StatusCode: resp.StatusCode}
	}

	var err error
	jresp := JsonResponse{}

	raw := make([]byte, resp.ContentLength)
	_, err = resp.Body.Read(raw)
	if err != nil && err != io.EOF {
		return newJsonResponse500(err)
	}
	err = json.Unmarshal(raw, &jresp)
	if err != nil {
		return newJsonResponse500(err)
	}

	if jresp.StatusCode != resp.StatusCode {
		s := fmt.Sprintf("Unmatched status codes: expected %d, got %d",
			resp.StatusCode, jresp.StatusCode)
		return newJsonResponse500(errors.New(s))
	}

	return &jresp
}

//----------------------------------------------------------

// given an input which is some messy type like "map[string]interface{}",
// convert it to the given output type
func SuperConverter(input interface{}, output interface{}) error {
	raw, err := json.Marshal(input)
	if err != nil {
		return nil
	}

	err = json.Unmarshal(raw, output)
	if err != nil {
		return nil
	}

	return nil
}

//----------------------------------------------------------

func HttpGetJson(url string) *JsonResponse {
	resp, err := http.Get(url)
	if err != nil {
		return newJsonResponse500(err)
	}

	return toJsonResponse(resp)
}

func httpPostOrPutJson(doPost bool, url string, in interface{}) *JsonResponse {

	byts, err := json.Marshal(in)
	if err != nil {
		return newJsonResponse500(err)
	}

	reader := bytes.NewReader(byts)
	var resp *http.Response
	if doPost {
		resp, err = http.Post(url, ContentTypeJSON, reader)

	} else {
		resp, err = HTTPPut(url, ContentTypeJSON, reader)
	}
	if err != nil {
		return newJsonResponse500(err)
	}

	return toJsonResponse(resp)
}

func HttpPostJson(url string, in interface{}) *JsonResponse {
	return httpPostOrPutJson(true, url, in)
}

func HttpPutJson(url string, in interface{}) *JsonResponse {
	return httpPostOrPutJson(false, url, in)
}

func HttpDeleteJson(url string) *JsonResponse {
	resp, err := HTTPDelete(url)
	if err != nil {
		return newJsonResponse500(err)
	}

	return toJsonResponse(resp)
}
