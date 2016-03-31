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

import "log"

const (
	PzTest          ServiceName = "PZ-TEST"
	PzDiscover      ServiceName = "pz-discover"
	PzLogger        ServiceName = "pz-logger"
	PzUuidgen       ServiceName = "pz-uuidgen"
	PzWorkflow      ServiceName = "pz-workflow"
	PzElasticSearch ServiceName = "elasticsearch"
	PzGateway       ServiceName = "pa-gateway"
)

// TODO: this should be derived from VCAP_APPLICATION?
const defaultDomain = ".stage.geointservices.io"

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
}

func NewSystemConfig(serviceName ServiceName,
	endpointOverrides *ServicesMap) (*SystemConfig, error) {

	var err error

	sys := &SystemConfig{}
	sys.endpoints = make(ServicesMap)

	sys.vcapApplication, err = NewVcapApplication()
	if err != nil {
		return nil, err
	}

	sys.vcapServices, err = NewVcapServices()
	if err != nil {
		return nil, err
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

	return sys, nil
}

func (sys *SystemConfig) registerOverrides(overrides *ServicesMap) error {
	// override/extend endpoints list with whatever the caller supplied for us:
	// this allows us to test various configurations of upstream services

	if overrides == nil {
		return nil
	}

	for k, v := range *overrides {
		if v != "" {
			sys.AddService(k, v)
		} else {
			// if they didn't give us an address, we'll default to using
			// the service name itself with whatever domain we're in
			sys.AddService(k, string(k)+defaultDomain)
		}
	}

	return nil
}

func (sys *SystemConfig) registerOtherServices() error {

	// initialize the endpoints list with the VCAP data

	if sys.vcapServices == nil {
		return nil
	}

	for k, v := range sys.vcapServices.Map {
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
		sys.Address = sys.vcapApplication.Address
		sys.BindTo = sys.vcapApplication.BindToPort
	}

	// remember to register ourself, of course
	// (when the server is strarted, this will be updated to correspond to the "bind to" address)
	sys.AddService(name, sys.Address)

	return nil
}

// it is explicitly allowed to update an existing service, but we'll log it just to be safe
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
