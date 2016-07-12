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

func (params *HttpQueryParams) GetOrder(key string, defalt string) (string, error) {
	str := params.Get(key)
	if str == "" {
		return defalt, nil
	}

	switch strings.ToLower(str) {
	case "desc":
		return "desc", nil
	case "asc":
		return "asc", nil
	}

	s := fmt.Sprintf("query argument for '?%s' must be \"asc\" or \"desc\"", key)
	err := errors.New(s)
	return "", err
}
