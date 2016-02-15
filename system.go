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
	"fmt"
	"github.com/fvbock/endless"
	"log"
	"net"
	"net/http"
	"time"
)

type ServiceName string

const (
	PzDiscover      ServiceName = "pz-discover"
	PzLogger        ServiceName = "pz-logger"
	PzUuidgen       ServiceName = "pz-uuidgen"
	PzWorkflow      ServiceName = "pz-workflow"
	PzElasticSearch ServiceName = "elastic-search"
)

type System struct {
	Config *Config

	ElasticSearchService *ElasticSearchService

	DiscoverService IDiscoverService
	Services        map[ServiceName]IService
}

const waitTimeout = 1000
const waitSleep = 100
const hammerTime = 3

func NewSystem(config *Config) (*System, error) {
	var err error

	sys := &System{
		Config:   config,
		Services: make(map[ServiceName]IService),
	}

	testMode := false

	switch sys.Config.mode {
	case ConfigModeCloud, ConfigModeLocal:
		sys.DiscoverService, err = NewPzDiscoverService(sys)
		if err != nil {
			return nil, err
		}
	case ConfigModeTest:
		testMode = true
		sys.DiscoverService, err = NewMockDiscoverService(sys)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("Invalid config mode: %s", sys.Config.mode)
	}
	sys.Services[PzDiscover] = sys.DiscoverService

	sys.ElasticSearchService, err = newElasticSearchService(testMode)
	if err != nil {
		return nil, err
	}
	sys.Services[PzElasticSearch] = sys.ElasticSearchService

	return sys, nil
}

func (sys *System) StartServer(routes http.Handler) chan error {
	done := make(chan error)

	ready := make(chan bool)

	endless.DefaultHammerTime = hammerTime * time.Second
	server := endless.NewServer(sys.Config.GetBindToAddress(), routes)
	server.BeforeBegin = func(_ string) {
		sys.Config.bindtoAddress = server.EndlessListener.Addr().(*net.TCPAddr).String()
		ready <- true
	}
	go func() {
		err := server.ListenAndServe()
		done <- err
	}()

	<-ready

	err := sys.WaitForServiceByName(sys.Config.GetName(), sys.Config.GetBindToAddress())
	if err != nil {
		log.Fatal(err)
	}

	err = sys.DiscoverService.RegisterService(sys.Config)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Config.serviceAddress: %s", sys.Config.GetAddress())
	log.Printf("Config.bindtoAddress: %s", sys.Config.GetBindToAddress())

	return done
}

func (sys *System) WaitForServiceByName(name ServiceName, address string) error {

	var url string
	switch name {
	case PzDiscover:
		url = fmt.Sprintf("http://%s/health-check", address)
	default:
		url = fmt.Sprintf("http://%s", address)
	}
	return sys.waitForServiceByURL(name, url)
}

func (sys *System) WaitForService(service IService) error {
	return sys.WaitForServiceByName(service.GetName(), service.GetAddress())
}

func (sys *System) waitForServiceByURL(name ServiceName, url string) error {
	msTime := 0

	for {
		resp, err := http.Get(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			log.Printf("found service %s", name)
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