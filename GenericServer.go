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
	"errors"
	"log"
	"net"
	"net/http"
	"syscall"
	"time"

	"github.com/fvbock/endless"
	"github.com/gin-gonic/gin"
)

//const waitTimeout = 1000
//const waitSleep = 100
const ginHammerTime = 3

type GenericServer struct {
	Sys    *SystemConfig
	pid    int
	router http.Handler
}

type RouteData struct {
	Verb    string
	Path    string
	Handler gin.HandlerFunc
}

// ServerLogHandler adds traditional logging support to the http server handlers.
func (server *GenericServer) LogHandler(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s %s", r.RemoteAddr, r.Method, r.URL)
		handler.ServeHTTP(w, r)
	})
}

func (server *GenericServer) Stop() error {
	sys := server.Sys

	err := syscall.Kill(server.pid, syscall.SIGINT)
	if err != nil {
		return err
	}

	err = sys.WaitForServiceToDieByAddress(sys.Name, sys.BindTo)
	if err != nil {
		return err
	}

	return nil
}

func (server *GenericServer) Start() (chan error, error) {

	sys := server.Sys

	done := make(chan error)

	ready := make(chan bool)

	endless.DefaultHammerTime = ginHammerTime * time.Second

	ginServer := endless.NewServer(server.Sys.BindTo, server.router)

	ginServer.BeforeBegin = func(_ string) {
		server.pid = syscall.Getpid()
		log.Printf("Actual pid is %d", server.pid)

		sys.BindTo = ginServer.EndlessListener.Addr().(*net.TCPAddr).String()

		ready <- true
	}

	go func() {
		err := ginServer.ListenAndServe()
		done <- err
	}()

	<-ready

	err := sys.WaitForServiceByAddress(sys.Name, sys.BindTo)
	if err != nil {
		return nil, err
	}

	log.Printf("Server %s started on %s (%s)", sys.Name, sys.Address, sys.BindTo)

	sys.AddService(sys.Name, sys.BindTo)

	return done, nil
}

func (server *GenericServer) Configure(routeData []RouteData) error {
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()

	//router.Use(gin.Logger())
	//router.Use(gin.Recovery())

	for _, data := range routeData {
		switch data.Verb {
		case "GET":
			router.GET(data.Path, data.Handler)
		case "POST":
			router.POST(data.Path, data.Handler)
		case "PUT":
			router.PUT(data.Path, data.Handler)
		case "DELETE":
			router.DELETE(data.Path, data.Handler)
		default:
			return errors.New("Invalid verb: " + data.Verb)
		}
	}

	server.router = router

	return nil
}
