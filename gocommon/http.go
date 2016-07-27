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
	"fmt"
	"io"
	"net/http"
)

//----------------------------------------------------------

type Http struct {
	Preflight  func(verb string, url string, json string)
	Postflight func(statusCode int, json string)
	ApiKey     string
	BaseUrl    string
}

//----------------------------------------------------------

// note we decode the result even if not a 2xx status
func (h *Http) convertResponseBodyToObject(resp *http.Response, output interface{}) error {
	if output == nil {
		return nil
	}

	if resp.ContentLength < 0 {
		////		return errors.New(fmt.Sprintf("ContentLength is %d", resp.ContentLength))
	}

	// no content is perfectly valid, not an error
	if resp.ContentLength == 0 {
		//////	return nil
	}

	var err error

	readAll := func(body io.ReadCloser) ([]byte, error) {
		raw := make([]byte, 0)
		tmp := make([]byte, 1)
		for {
			n, err := body.Read(tmp)
			if err != nil && err != io.EOF {
				return nil, err
			}
			if n == 1 {
				raw = append(raw, tmp[0])
			}
			if err == io.EOF {
				break
			}
		}
		return raw, nil
	}

	defer resp.Body.Close()
	raw, err := readAll(resp.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(raw, output)
	if err != nil {
		return err
	}

	return nil
}

func (h *Http) convertObjectToReader(input interface{}) (io.Reader, error) {
	if input == nil {
		return nil, nil
	}

	byts, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}

	reader := bytes.NewReader(byts)
	return reader, nil
}

func (h *Http) toJsonString(obj interface{}) string {
	if obj == nil {
		return "{}"
	}

	byts, err := json.Marshal(obj)
	if err != nil {
		return "internal error: unable to marshall into json"
	}
	return string(byts)
}

func (h *Http) doPreflight(verb string, url string, obj interface{}) {
	if h.Preflight != nil {
		jsn := h.toJsonString(obj)
		h.Preflight(verb, url, jsn)
	}
}

func (h *Http) doPostflight(statusCode int, obj interface{}) {
	if h.Postflight != nil {
		jsn := h.toJsonString(obj)
		h.Postflight(statusCode, jsn)
	}
}

func (h *Http) doVerb(verb string, endpoint string, input interface{}, output interface{}) (int, error) {
	url := h.BaseUrl + endpoint

	reader, err := h.convertObjectToReader(input)
	if err != nil {
		return 0, err
	}

	h.doPreflight(verb, url, input)

	var resp *http.Response
	{
		req, err := http.NewRequest(verb, url, reader)
		if err != nil {
			return 0, err
		}

		if h.ApiKey != "" {
			req.SetBasicAuth(h.ApiKey, "")
		}

		client := &http.Client{}
		resp, err = client.Do(req)
		if err != nil {
			return 0, err
		}
	}

	err = h.convertResponseBodyToObject(resp, output)
	if err != nil {
		s, err := fmt.Printf("failed/1: %#v\nfailed/2: %#v\nfailed: %#v\n", err, resp, output)
		if err != nil {
			return 0, err
		}
		h.doPostflight(resp.StatusCode, s)
	}

	h.doPostflight(resp.StatusCode, output)

	return resp.StatusCode, nil
}

//----------------------------------------------------------

// Use these when doing HTTP requests where the inputs and outputs
// are supposed to be JSON strings (for which the caller supplies
// a Go object).

// expects endpoint to return JSON
func (h *Http) Get(endpoint string, output interface{}) (int, error) {
	return h.doVerb("GET", endpoint, nil, output)
}

// expects endpoint to take in and return JSON
func (h *Http) Post(endpoint string, input interface{}, output interface{}) (int, error) {
	return h.doVerb("POST", endpoint, input, output)
}

// expects endpoint to take in and return JSON
func (h *Http) Put(endpoint string, input interface{}, output interface{}) (int, error) {
	return h.doVerb("PUT", endpoint, input, output)
}

// expects endpoint to return nothing
func (h *Http) Delete(endpoint string) (int, error) {
	return h.doVerb("DELETE", endpoint, nil, nil)
}

//----------------------------------------------------------

// Use these when doing HTTP requests where the inputs and outputs
// are supposed to be JSON strings, and the output is in the form
// of a JsonResponse

func (h *Http) PzGet(endpoint string) *JsonResponse {
	output := &JsonResponse{}

	code, err := h.Get(endpoint, output)
	if err != nil {
		return newJsonResponse500(err)
	}

	output.StatusCode = code

	return output
}

func (h *Http) PzPost(endpoint string, input interface{}) *JsonResponse {
	output := &JsonResponse{}

	code, err := h.Post(endpoint, input, output)
	if err != nil {
		return newJsonResponse500(err)
	}

	output.StatusCode = code

	return output
}

func (h *Http) PzPut(endpoint string, input interface{}) *JsonResponse {
	output := &JsonResponse{}

	code, err := h.Put(endpoint, input, output)
	if err != nil {
		return newJsonResponse500(err)
	}

	output.StatusCode = code

	return output
}

func (h *Http) PzDelete(endpoint string) *JsonResponse {
	code, err := h.Delete(endpoint)
	if err != nil {
		return newJsonResponse500(err)
	}

	return &JsonResponse{StatusCode: code}
}
