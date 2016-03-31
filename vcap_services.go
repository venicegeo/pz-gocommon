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

/*
"VCAP_SERVICES": {
  "user-provided": [
   {
    "credentials": {
     "host": "172.32.125.109:9200"
    },
    "label": "user-provided",
    "name": "pz-elasticsearch",
    "syslog_drain_url": "",
    "tags": []
   }
  ]
 }
}
*/

package piazza

import (
	"log"
	"os"
)

type VcapCredentials struct {
	Host string `json:"host"`
}

type VcapServiceEntry struct {
	Credentials    VcapCredentials `json:"credentials"`
	Label          string          `json:"label"`
	Name           string          `json:"string"`
	SyslogDrainUrl string          `json:"syslog_drain_url"`
	Tags           []string        `json:"tags"`
}

type VcapServices struct {
	UserProvided []VcapServiceEntry `json:"user-provided"`

	Services map[ServiceName]string
}

func NewVcapServices() (*VcapServices, error) {

	str := os.Getenv("VCAP_SERVICES")
	if str == "" {
		return nil, nil
	}

	log.Printf("VCAP_SERVICES:\n%s", str)

	vcap := &VcapServices{Services: make(ServicesMap)}

	for _, serviceEntry := range vcap.UserProvided {
		name := ServiceName(serviceEntry.Name)
		addr := serviceEntry.Credentials.Host
		vcap.Services[name] = addr
	}

	return vcap, nil
}
