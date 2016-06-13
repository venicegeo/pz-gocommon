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
	"io"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/fvbock/endless"
)

const (
	// ContentTypeJSON is the http content-type for JSON.
	ContentTypeJSON = "application/json"

	// ContentTypeText is the http content-type for plain text.
	ContentTypeText = "text/plain"
)

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

//---------------------------------------------------------------------------

const waitTimeout = 1000
const waitSleep = 100
const hammerTime = 3

// ServerLogHandler adds traditional logging support to the http server handlers.
func ServerLogHandler(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s %s", r.RemoteAddr, r.Method, r.URL)
		handler.ServeHTTP(w, r)
	})
}

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

	err := sys.WaitForServiceByAddress(sys.Name, sys.BindTo)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Server %s started on %s (%s)", sys.Name, sys.Address, sys.BindTo)

	sys.AddService(sys.Name, sys.BindTo)

	return done
}
