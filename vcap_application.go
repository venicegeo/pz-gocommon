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
	"encoding/json"
	"errors"
	"os"
)

type VcapApplication struct {
	Name            ServiceName
	Address         string
	BindToPort      string
	ApplicationID   string   `json:"application_id"`
	ApplicationName string   `json:"application_name"`
	ApplicationURIs []string `json:"application_uris"`
}

func NewVcapApplication() (*VcapApplication, error) {

	str := os.Getenv("VCAP_APPLICATION")
	if str == "" {
		return nil, nil
	}

	vcap := &VcapApplication{}

	err := json.Unmarshal([]byte(str), vcap)
	if err != nil {
		return nil, err
	}

	vcap.Name = ServiceName(vcap.ApplicationName)
	vcap.Address = vcap.ApplicationURIs[0]

	port := os.Getenv("PORT")
	if str == "" {
		return nil, errors.New("Unable to read $PORT for PCF deployment")
	}

	vcap.BindToPort = ":" + port

	return vcap, nil
}
