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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
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
/*
// expects endpoint to return JSON
func HttpJsonGetObject(url string, output interface{}) (int, error) {
	h := &Http{}
	return h.Get(url, output)
}

// expects endpoint to take in and return JSON
func HttpJsonPostObject(url string, input interface{}, output interface{}) (int, error) {
	h := &Http{}
	return h.Post(url, input, output)
}

// expects endpoint to take in and return JSON
func HttpJsonPutObject(url string, input interface{}, output interface{}) (int, error) {
	h := &Http{}
	return h.Put(url, input, output)
}

// expects endpoint to return JSON (or empty)
func HttpJsonDeleteObject(url string) (int, error) {
	h := &Http{}
	return h.Delete(url)
}
*/
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

//---------------------------------------------------------------------

func GinReturnJson(c *gin.Context, resp *JsonResponse) {
	raw, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		log.Fatalf("Internal Error: marshalling of %#v", resp)
	}
	log.Printf("%d %s", len(raw), string(raw))
	c.Data(resp.StatusCode, ContentTypeJSON, raw)
}

// Get the Pz API key, in this order:
//
// (1) if $PZKEY present, use that
// (2) if ~/.pzkey exists, use that
// (3) error
//
// And no, we don't uspport Windows.
func GetApiKey(space string) (string, error) {

	fileExists := func(s string) bool {
		if _, err := os.Stat(s); os.IsNotExist(err) {
			return false
		}
		return true
	}

	key := os.Getenv("PZKEY")
	if key != "" {
		key = strings.TrimSpace(key)
		return key, nil
	}

	home := os.Getenv("HOME")
	if home == "" {
		return "", errors.New("Unable read $HOME")
	}

	path := home + "/.pzkey"
	if !fileExists(path) {
		return "", errors.New("Unable to find env var $PZKEY or file $HOME/.pzkey")
	}

	raw, err := ioutil.ReadFile(path)
	if err != nil {
		return "", err
	}

	data := map[string]string{}
	err = json.Unmarshal(raw, &data)
	if err != nil {
		return "", err
	}

	key, ok := data[space]
	if !ok {
		return "", errors.New("No API key for deployment space " + space)
	}

	return key, nil
}
