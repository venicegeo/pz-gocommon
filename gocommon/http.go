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

type QueryFunc func(string) string
type GetQueryFunc func(string) (string, bool)

type JsonPaginationResponse struct {
	Count   int    `json:"count" binding:"required"`
	Page    int    `json:"page" binding:"required"`
	PerPage int    `json:"perPage" binding:"required"`
	SortBy  string `json:"sortBy" binding:"required"`
	Order   string `json:"order" binding:"required"` // "asc" or "desc"
}

type JsonResponse struct {
	StatusCode int `json:"statusCode" binding:"required"`

	// only 2xxx
	Data       interface{}            `json:"data"`
	Pagination JsonPaginationResponse `json:"pagination,omitempty"` // optional

	// only 4xx and 5xx
	Message string        `json:"message" binding:"required"`
	Origin  string        `json:"origin,omitempty"` // optional
	Inner   *JsonResponse `json:"inner,omitempty"`

	// optional
	Metadata interface{} `json:"metadata,omitempty"`
}

func newJsonResponse500(err error) *JsonResponse {
	return &JsonResponse{StatusCode: http.StatusInternalServerError, Message: err.Error()}
}

func ToTypedJsonResponse(resp *http.Response, obj interface{}) *JsonResponse {
	jresp := ToJsonResponse(resp)

	err := SuperConverter(jresp.Data, obj)
	if err != nil {
		j := newJsonResponse500(err)
		j.Inner = jresp
		return j
	}

	return jresp
}

func ToJsonResponse(resp *http.Response) *JsonResponse {
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

	return ToJsonResponse(resp)
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

	return ToJsonResponse(resp)
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

	return ToJsonResponse(resp)
}
