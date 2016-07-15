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
	"time"
)

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

func responseToObject(resp *http.Response, output interface{}) error {
	// no content is perfectly valid, not an error
	if resp.ContentLength == 0 {
		return nil
	}

	var err error

	raw := make([]byte, resp.ContentLength)
	_, err = resp.Body.Read(raw)
	if err != nil && err != io.EOF {
		return err
	}

	err = json.Unmarshal(raw, output)
	if err != nil {
		return err
	}

	return nil
}

func processInput(input interface{}) (io.Reader, error) {
	byts, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}

	reader := bytes.NewReader(byts)
	return reader, nil
}

func processOutput(resp *http.Response, err error, output interface{}) (int, error) {
	if err != nil {
		return 0, err
	}

	if output != nil {
		// note we decode the result even if not a 2xx status
		err = responseToObject(resp, output)
		if err != nil {
			return 0, err
		}
	}

	return resp.StatusCode, nil
}

// expects endpoint to return JSON
func HttpJsonGetObject(url string, output interface{}) (int, error) {
	resp, err := http.Get(url)
	if err != nil {
		return 0, err
	}

	code, err := processOutput(resp, err, output)
	if err != nil {
		return 0, err
	}

	return code, nil
}

// expects endpoint to take in and return JSON
func HttpJsonPostObject(url string, input interface{}, output interface{}) (int, error) {
	reader, err := processInput(input)
	if err != nil {
		return 0, err
	}

	resp, err := http.Post(url, ContentTypeJSON, reader)
	if err != nil {
		return 0, err
	}

	code, err := processOutput(resp, err, output)
	if err != nil {
		return 0, err
	}

	return code, nil
}

// expects endpoint to take in and return JSON
func HttpJsonPutObject(url string, input interface{}, output interface{}) (int, error) {
	reader, err := processInput(input)
	if err != nil {
		return 0, err
	}

	resp, err := HTTPPut(url, ContentTypeJSON, reader)
	if err != nil {
		return 0, err
	}

	code, err := processOutput(resp, err, output)
	if err != nil {
		return 0, err
	}

	return code, nil
}

// expects endpoint to return JSON (or empty)
func HttpJsonDeleteObject(url string) (int, error) {
	resp, err := HTTPDelete(url)
	if err != nil {
		return 0, err
	}

	code, err := processOutput(resp, err, nil)
	if err != nil {
		return 0, err
	}

	return code, nil
}

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

func (params *HttpQueryParams) AsInt(key string, defalt *int) (*int, error) {
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

func (params *HttpQueryParams) AsString(key string, defalt *string) (*string, error) {
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

func (params *HttpQueryParams) AsOrder(key string, defalt *PaginationOrder) (*PaginationOrder, error) {
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

func (params *HttpQueryParams) AsTime(key string, defalt *time.Time) (*time.Time, error) {
	if key == "" {
		return defalt, nil
	}

	value, ok := params.raw[key]
	if !ok || value == "" {
		return defalt, nil
	}

	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, err
	}

	return &t, nil
}

func (params *HttpQueryParams) GetAfter(defalt *time.Time) (*time.Time, error) {
	return params.AsTime("after", defalt)
}

func (params *HttpQueryParams) GetBefore(defalt *time.Time) (*time.Time, error) {
	return params.AsTime("before", defalt)
}

func (params *HttpQueryParams) GetCount(defalt *int) (*int, error) {
	return params.AsInt("count", defalt)
}

func (params *HttpQueryParams) GetOrder(defalt *PaginationOrder) (*PaginationOrder, error) {
	return params.AsOrder("order", defalt)
}

func (params *HttpQueryParams) GetPage(defalt *int) (*int, error) {
	return params.AsInt("page", defalt)
}

func (params *HttpQueryParams) GetPerPage(defalt *int) (*int, error) {
	return params.AsInt("perPage", defalt)
}

func (params *HttpQueryParams) GetSortBy(defalt *string) (*string, error) {
	return params.AsString("sortBy", defalt)
}
