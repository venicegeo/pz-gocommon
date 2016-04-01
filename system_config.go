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
	"strings"
)

const DefaultElasticsearchAddress = "localhost:9200"
const DefaultDomain = ".stage.geointservices.io"

const (
	PzGoCommon      ServiceName = "PZ-GOCOMMON" // not a real service, just for testing
	PzDiscover      ServiceName = "pz-discover"
	PzLogger        ServiceName = "pz-logger"
	PzUuidgen       ServiceName = "pz-uuidgen"
	PzWorkflow      ServiceName = "pz-workflow"
        PzGateway       ServiceName = "pz-gateway"
	PzElasticSearch ServiceName = "elasticsearch"
	PzGateway       ServiceName = "pz-gateway"
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
	debug           bool
}

func NewSystemConfig(serviceName ServiceName,
	requiredServices []ServiceName,
	debug bool) (*SystemConfig, error) {

	var err error

	sys := &SystemConfig{endpoints: make(ServicesMap), debug: debug}

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
	} else {
		sys.domain = DefaultDomain
	}

	// set some data about our own service first
	sys.Name = serviceName
	sys.Address = sys.vcapApplication.GetAddress()
	sys.BindTo = sys.vcapApplication.GetBindToPort()

	// set the services table with the services we require,
	// using VcapServices to get the addresses
	err = sys.checkRequirements(requiredServices)
	if err != nil {
		return nil, err
	}

	err = sys.runHealthChecks()
	if err != nil {
		return nil, err
	}

	return sys, nil
}

func (sys *SystemConfig) checkRequirements(requirements []ServiceName) error {

	for _, name := range requirements {

		if name == sys.Name {
			sys.AddService(name, sys.Address)

		} else {
			if addr, ok := sys.vcapServices.Services[name]; !ok {
				sys.AddService(name, string(name)+DefaultDomain)

			} else {

				if strings.HasPrefix(string(addr), "localhost") {
					// special cases, e.g. for elasticsearch: do nothing
					sys.AddService(name, addr)

				} else {
					sys.AddService(name, addr+DefaultDomain)
				}
			}
		}

		log.Printf("Required service %s: %s", name, sys.GetService(name))
	}

	return nil
}

func (sys *SystemConfig) runHealthChecks() error {
	//log.Printf("SystemConfig.runHealthChecks: start")

	for name, addr := range sys.endpoints {
		if name != sys.Name {
			//log.Printf("Service healthy? %s at %s", nam, addr)

			url := "http://" + addr

			resp, err := http.Get(url)
			if err != nil {
				return err
			}

			if resp.StatusCode != http.StatusOK {
				if name == PzGateway {
					// TODO: Patrick doesn't have a healthcheck endpoint yet
				} else {
					return NewErrorf("Health check failed for service: %s at %s", name, url)
				}
			}

			/*log.Printf("Service healthy: %s at %s", name, addr)
			body, err := ReadFrom(resp.Body)
			if err != nil {
				return err
			}
			log.Printf(">>> %s <<<", string(body))*/
		}
	}

	//log.Printf("SystemConfig.runHealthChecks: end")
	return nil
}

// it is explicitly allowed for outsiders to update an existing service, but we'll log it just to be safe
func (sys *SystemConfig) AddService(name ServiceName, address string) {
	old, ok := sys.endpoints[name]
	sys.endpoints[name] = address
	if ok {
		log.Printf("SystemConfig.AddService: updated %s from %s to %s", name, old, address)
	}
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
	return sys.debug
}

/*
func IsLocalConfig() bool {
	localFlag := flag.Bool("local", false, "use localhost ports")
	flag.Parse()
	return *localFlag
}
*/
