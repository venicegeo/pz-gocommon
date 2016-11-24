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
	"strings"

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

//---------------------------------------------------------------------

func GinReturnJson(c *gin.Context, resp *JsonResponse) {
	raw, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		log.Fatalf("Internal Error: marshalling of %#v", resp)
	}
	c.Data(resp.StatusCode, ContentTypeJSON, raw)

	// If things get worse, try this:
	//    c.Writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	//    c.Writer.Header().Set("Content-Length", str(len(raw))
}

// GetApiKey retrieves the Pz API key for the given server, in this order:
//
// (1) if $PZKEY present, use that
// (2) if ~/.pzkey exists, use that
// (3) error
//
// And no, we don't uspport Windows.
func GetApiKey(pzserver string) (string, error) {

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

	key, ok := data[pzserver]
	if !ok {
		return "", fmt.Errorf("No API key for server %s", pzserver)
	}

	return key, nil
}

// GetApiServer gets the $PZSERVER host.
func GetApiServer() (string, error) {
	pzserver := os.Getenv("PZSERVER")
	if pzserver == "" {
		return "", fmt.Errorf("$PZSERVER not set")
	}
	return pzserver, nil
}
