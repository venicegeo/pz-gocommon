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
	"log"
	"net/http"
)

const LocalElasticsearchURL = "http://localhost:9200"

const (
	PzTestBed       ServiceName = "PZ-TESTBED"
	PzDiscover      ServiceName = "pz-discover"
	PzLogger        ServiceName = "pz-logger"
	PzUuidgen       ServiceName = "pz-uuidgen"
	PzWorkflow      ServiceName = "pz-workflow"
	PzElasticSearch ServiceName = "elasticsearch"
	PzGateway       ServiceName = "pa-gateway"
)

type ServiceName string

type ServicesMap map[ServiceName]string

type SystemConfig struct {
	// our own service
	Name    ServiceName
	Address string
	BindTo  string

	// our external services
	endpoints ServicesMap

	vcapApplication *VcapApplication
	vcapServices    *VcapServices
	domain          string
}

func NewSystemConfig(serviceName ServiceName,
	endpointOverrides *ServicesMap) (*SystemConfig, error) {

	var err error

	sys := &SystemConfig{endpoints: make(ServicesMap)}

	sys.vcapApplication, err = NewVcapApplication()
	if err != nil {
		return nil, err
	}

	sys.vcapServices, err = NewVcapServices()
	if err != nil {
		return nil, err
	}

	if sys.vcapApplication != nil {
		sys.domain = sys.vcapApplication.GetDomain()
	}

	err = sys.registerThisService(serviceName)
	if err != nil {
		return nil, nil
	}

	err = sys.registerOtherServices()
	if err != nil {
		return nil, err
	}

	err = sys.registerOverrides(endpointOverrides)
	if err != nil {
		return nil, err
	}

	err = sys.runHealthChecks()
	if err != nil {
		return nil, err
	}

	return sys, nil
}

func (sys *SystemConfig) registerOverrides(overrides *ServicesMap) error {
	// override/extend endpoints list with whatever the caller supplied for us:
	// this allows us to test various configurations of upstream services

	if overrides == nil {
		return nil
	}

	// the user must give us a complete address (with domain)
	for nam, addr := range *overrides {
		sys.AddService(nam, addr)
	}

	return nil
}

func (sys *SystemConfig) registerOtherServices() error {

	// initialize the endpoints list with the VCAP data

	if sys.vcapServices == nil {
		return nil
	}

	for k, v := range sys.vcapServices.Services {
		sys.AddService(k, v)
	}

	return nil
}

func (sys *SystemConfig) registerThisService(name ServiceName) error {

	// get information on our own service

	sys.Name = name

	if sys.vcapApplication == nil {
		// no VCAP present, so we'll assume we're in testing mode runing locally
		sys.Address = "localhost:0"
		sys.BindTo = "localhost:0"
	} else {
		sys.Address = sys.vcapApplication.GetAddress()
		sys.BindTo = sys.vcapApplication.GetBindToPort()
	}

	// remember to register ourself, of course
	// (when the server is strarted, this will be updated to correspond to the "bind to" address)
	sys.AddService(name, sys.Address)

	return nil
}

func (sys *SystemConfig) runHealthChecks() error {
	for nam, addr := range sys.endpoints {
		if nam == sys.Name {
			continue
		}

		resp, err := http.Get("http://" + addr)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return NewErrorf("Health check failed for service: %s at %s", nam, addr)
		}
		log.Printf("Service healthy: %s at %s", nam, addr)
	}

	return nil
}

// it is explicitly allowed for outsiders to update an existing service, but we'll log it just to be safe
func (sys *SystemConfig) AddService(name ServiceName, address string) {
	old, ok := sys.endpoints[name]
	if ok {
		log.Printf("updating registered service %s from %s to %s", name, old, address)
	}

	sys.endpoints[name] = address
}

func (sys *SystemConfig) GetService(name ServiceName) string {
	addr, ok := sys.endpoints[name]
	if !ok {
		return ""
	}

	return addr
}

func (sys *SystemConfig) GetDomain() string {
	return sys.domain
}

func (sys *SystemConfig) Testing() bool {
	return sys.Name == PzTestBed
}

func (sys *SystemConfig) String() string {
	log.Printf("SystemConfig.Name: %s", sys.Name)
	log.Printf("SystemConfig.eAddress: %s", sys.Address)
	log.Printf("SystemConfig.BindTo: %s", sys.BindTo)
	return "-config-"
}

/*
func IsLocalConfig() bool {
	localFlag := flag.Bool("local", false, "use localhost ports")
	flag.Parse()
	return *localFlag
}
*/
