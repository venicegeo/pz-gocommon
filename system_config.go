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
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

const DefaultElasticsearchAddress = "localhost:9200"
const DefaultKafkaAddress = "localhost:9092"
const DefaultDomain = ".stage.geointservices.io"
const DefaultProtocol = "http"

type ServiceName string

const (
	PzDiscover      ServiceName = "pz-discover"
	PzElasticSearch ServiceName = "pz-elasticsearch"
	PzGoCommon      ServiceName = "PZ-GOCOMMON" // not a real service, just for testing
	PzKafka         ServiceName = "pz-kafka"
	PzLogger        ServiceName = "pz-logger"
	PzUuidgen       ServiceName = "pz-uuidgen"
	PzWorkflow      ServiceName = "pz-workflow"
	PzsvcHello      ServiceName = "pzsvc-hello"
)

var EndpointPrefixes = map[ServiceName]string{
	PzDiscover:      "",
	PzElasticSearch: "",
	PzKafka:         "",
	PzLogger:        "/v1",
	PzUuidgen:       "/v1",
	PzWorkflow:      "/v1",
	PzsvcHello:      "/v1",
}

var HealthcheckEndpoints = map[ServiceName]string{
	PzDiscover:      "",
	PzElasticSearch: "",
	PzKafka:         "",
	PzLogger:        "/",
	PzUuidgen:       "/",
	PzWorkflow:      "/",
	PzsvcHello:      "/",
}

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
	requiredServices []ServiceName) (*SystemConfig, error) {

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
	} else {
		sys.domain = DefaultDomain
	}

	if os.Getenv("DOMAIN") != "" {
		sys.domain = os.Getenv("DOMAIN")
		if !strings.HasPrefix(sys.domain, ".") {
			sys.domain = "." + sys.domain
		}
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
			//log.Printf("check requirements for %s: case 1", name)
			sys.AddService(name, sys.Address)

		} else {
			if addr, ok := sys.vcapServices.Services[name]; !ok {
				// the service we want is not in VCAP, so fake it
				//log.Printf("check requirements for %s: case 2", name)
				sys.AddService(name, string(name)+sys.domain)

			} else {
				// the service we want is in VCAP, with a full and valid address
				//log.Printf("check requirements for %s: case 3", name)
				sys.AddService(name, addr)
			}
		}

		newaddr, err := sys.GetAddress(name)
		if err != nil {
			return err
		}
		log.Printf("Required service: %s at %s", name, newaddr)
	}

	return nil
}

func (sys *SystemConfig) runHealthChecks() error {
	//log.Printf("SystemConfig.runHealthChecks: start")

	for name, addr := range sys.endpoints {
		if name == sys.Name || name == PzKafka {
			continue
		}

		url := fmt.Sprintf("%s://%s%s", DefaultProtocol, addr, HealthcheckEndpoints[name])

		//log.Printf("Service healthy? %s at %s (%s)", name, addr, url)

		resp, err := http.Get(url)
		if err != nil {
			return errors.New(fmt.Sprintf("Health check errored for service: %s at %s <%#v>", name, url, resp))
		}

		if resp.StatusCode != http.StatusOK {
			return errors.New(fmt.Sprintf("Health check failed for service: %s at %s <%#v>", name, url, resp))
		}

		log.Printf("Service healthy: %s at %s", name, url)
		/*body, err := ReadFrom(resp.Body)
		if err != nil {
			return err
		}
		log.Printf(">>> %s <<<", string(body))*/
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

func (sys *SystemConfig) GetAddress(name ServiceName) (string, error) {
	addr, ok := sys.endpoints[name]
	if !ok {
		return "", errors.New(fmt.Sprintf("Unknown service: %s", name))
	}

	return addr, nil
}

func (sys *SystemConfig) GetURL(name ServiceName) (string, error) {
	addr, err := sys.GetAddress(name)
	if err != nil {
		return "", err
	}

	url := fmt.Sprintf("%s://%s%s", DefaultProtocol, addr, EndpointPrefixes[name])

	return url, nil
}

func (sys *SystemConfig) GetDomain() string {
	return sys.domain
}

func (sys *SystemConfig) WaitForService(name ServiceName) error {
	addr, err := sys.GetAddress(name)
	if err != nil {
		return err
	}

	err = sys.WaitForServiceByAddress(name, addr)
	if err != nil {
		return err
	}

	return nil
}

func (sys *SystemConfig) WaitForServiceByAddress(name ServiceName, address string) error {
	url := fmt.Sprintf("%s://%s", DefaultProtocol, address)

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
