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
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/fvbock/endless"
	"github.com/gin-gonic/gin"
)

const (
	// ContentTypeJSON is the http content-type for JSON.
	ContentTypeJSON = "application/json"

	// ContentTypeText is the http content-type for plain text.
	ContentTypeText = "text/plain"
)

// ServerLogHandler adds traditional logging support to the http server handlers.
func ServerLogHandler(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s %s", r.RemoteAddr, r.Method, r.URL)
		handler.ServeHTTP(w, r)
	})
}

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

func HandlePostAdminShutdown(c *gin.Context) {
	type shutdownRequest struct {
		Reason string `json:"reason"`
	}
	var reason shutdownRequest

	err := c.BindJSON(&reason)
	if err != nil {
		c.String(http.StatusBadRequest, fmt.Sprintf("%v", err))
		return
	}
	if reason.Reason == "" {
		c.String(http.StatusBadRequest, "no reason supplied")
		return
	}
	//pzService.Log(SeverityFatal, "Shutdown requested: "+reason.Reason)
	log.Fatalf("Shutdown requested: %s", reason.Reason)

	// TODO: need a graceful shutdown method
	// need to ACK to the HTTP caller, then call exit
	os.Exit(0)
}

// converts an arbitrary object to a real json string
func ConvertObjectToJsonString(jsn interface{}, compact bool) (JsonString, error) {
	var byts []byte
	var err error

	if compact {
		byts, err = json.Marshal(jsn)
	} else {
		byts, err = json.MarshalIndent(jsn, "", "    ")
	}
	if err != nil {
		return "", err
	}

	return JsonString(byts), nil
}

// removes excess whitespace
func (input JsonString) ToCompactJson() (JsonString, error) {
	dst := new(bytes.Buffer)
	err := json.Compact(dst, []byte(input))
	if err != nil {
		return "", err
	}
	return JsonString(dst.String()), nil
}

//---------------------------------------------------------------------------

const waitTimeout = 1000
const waitSleep = 100
const hammerTime = 3

func (sys *SystemConfig) StartServer(routes http.Handler) chan error {
	done := make(chan error)

	ready := make(chan bool)

	endless.DefaultHammerTime = hammerTime * time.Second
	server := endless.NewServer(sys.BindTo, routes)
	server.BeforeBegin = func(_ string) {
		sys.BindTo = server.EndlessListener.Addr().(*net.TCPAddr).String()
		ready <- true
	}
	go func() {
		err := server.ListenAndServe()
		done <- err
	}()

	<-ready

	err := WaitForService(sys.Name, sys.BindTo)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Server %s started on %s (%s)", sys.Name, sys.Address, sys.BindTo)

	sys.AddService(sys.Name, sys.BindTo)

	return done
}

func WaitForService(name ServiceName, address string) error {
	url := fmt.Sprintf("http://%s", address)

	msTime := 0

	for {
		resp, err := http.Get(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			//log.Printf("found service %s", name)
			return nil
		}
		if msTime >= waitTimeout {
			return fmt.Errorf("timed out waiting for service: %s at %s", name, url)
		}
		time.Sleep(waitSleep * time.Millisecond)
		msTime += waitSleep
	}
	/* notreached */
}
