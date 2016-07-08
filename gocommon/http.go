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

func (p *JsonPagination) ReadParams(params map[string]string, defalt *JsonPagination) error {

	getIntParam := func(key string, defalt int) (int, error) {
		str := params[key]
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

	getStringParam := func(key string, defalt string) (string, error) {
		str := params[key]
		if str == "" {
			return defalt, nil
		}
		return str, nil
	}

	getOrderParam := func(key string, defalt PaginationOrder) (PaginationOrder, error) {
		str := params[key]
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

	perPage, err := getIntParam("perPage", defalt.PerPage)
	if err != nil {
		return err
	}

	page, err := getIntParam("page", defalt.Page)
	if err != nil {
		return err
	}

	sortBy, err := getStringParam("sortBy", defalt.SortBy)
	if err != nil {
		return err
	}

	order, err := getOrderParam("order", defalt.Order)
	if err != nil {
		return err
	}

	p.PerPage = perPage
	p.Page = page
	p.SortBy = sortBy
	p.Order = order

	return nil
}

//----------------------------------------------------------

// TODO: get rid of these, pass in the array of query params instead
type QueryFunc func(string) string
type GetQueryFunc func(string) (string, bool)

type JsonString string

type JsonResponse struct {
	StatusCode int `json:"statusCode" binding:"required"`

	// only 2xxx
	Data       interface{}     `json:"data"`
	Pagination *JsonPagination `json:"pagination,omitempty"` // optional

	// only 4xx and 5xx
	Message string        `json:"message" binding:"required"`
	Origin  string        `json:"origin,omitempty"` // optional
	Inner   *JsonResponse `json:"inner,omitempty"`

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
