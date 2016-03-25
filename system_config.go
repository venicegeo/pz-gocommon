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

type ServiceName string

const (
	PzDiscover      ServiceName = "pz-discover"
	PzLogger        ServiceName = "pz-logger"
	PzUuidgen       ServiceName = "pz-uuidgen"
	PzWorkflow      ServiceName = "pz-workflow"
	PzElasticSearch ServiceName = "elasticsearch"
)

type SystemConfig struct {
	Name      ServiceName
	Address   string
	BindTo    string
	Endpoints map[ServiceName]string // name -> address

	vcapApplication *VcapApplication
	vcapServices    *VcapServices
}

func NewSystemConfig(serviceName ServiceName, endpointOverrides *map[ServiceName]string) (*SystemConfig, error) {
	var err error

	sys := &SystemConfig{
		Name:      serviceName,
		Endpoints: make(map[ServiceName]string),
	}

	// get information on our own service
	sys.vcapApplication, err = NewVcapApplication()
	if err != nil {
		return nil, err
	}
	if sys.vcapApplication == nil {
		// no VCAP present, so we'll assume we're in testing mode runing locally
		sys.Address = "localhost:0"
		sys.BindTo = "localhost:0"
	} else {
		sys.Address = sys.vcapApplication.Address
		sys.BindTo = sys.vcapApplication.BindToPort
	}

	// initialize the endpoints list with the VCAP data
	sys.vcapServices, err = NewVcapServices()
	if err != nil {
		return nil, err
	}
	if sys.vcapServices != nil {
		for k, v := range sys.vcapServices.Map {
			sys.Endpoints[k] = v
		}
	}

	// override/extend endpoints list with whatever the caller supplied for us
	if endpointOverrides != nil {
		for k, v := range *endpointOverrides {
			if v != "" {
				sys.Endpoints[k] = v
			} else {
				sys.Endpoints[k] = string(k) + ".cf.piazzageo.io"
			}
		}
	}

	return sys, nil
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
