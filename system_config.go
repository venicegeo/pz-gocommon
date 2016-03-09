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
	"flag"
	"log"
	"os"
)

type ConfigMode string

const (
	ConfigModeLocal ConfigMode = "local"
	ConfigModeTest  ConfigMode = "test"
	ConfigModeCloud ConfigMode = "cloud"
)

type Config struct {
	mode            ConfigMode
	serviceName     ServiceName
	serviceAddress  string
	discoverAddress string
	bindtoAddress   string
}

func NewConfig(serviceName ServiceName, configType ConfigMode) (*Config, error) {

	var config *Config
	var err error

	switch configType {
	case ConfigModeLocal:
		config = getLocalConfig(serviceName)
	case ConfigModeTest:
		config = getTestConfig(serviceName)
	case ConfigModeCloud:
		config, err = getPCFConfig(serviceName)
		if err != nil {
			return nil, err
		}
	}

	log.Printf("Config.mode: %s", string(config.mode))
	log.Printf("Config.serviceName: %s", config.GetName())
	log.Printf("Config.serviceAddress: %s", config.GetAddress())
	log.Printf("Config.discoverAddress: %s", config.discoverAddress)
	log.Printf("Config.bindtoAddress: %s", config.bindtoAddress)

	return config, err
}

func (config Config) GetName() ServiceName {
	return config.serviceName
}

func (config Config) GetAddress() string {
	return config.serviceAddress
}

func (config Config) GetBindToAddress() string {
	return config.bindtoAddress
}

func IsLocalConfig() bool {
	localFlag := flag.Bool("local", false, "use localhost ports")
	flag.Parse()
	return *localFlag
}

func getLocalConfig(serviceName ServiceName) *Config {

	var localHosts = map[ServiceName]string{
		PzLogger:   "pz-logger.cf.piazzageo.io/",
		PzUuidgen:  "localhost:12340",
		PzWorkflow: "localhost:12342",
		PzDiscover: "pz-discover.cf.piazzageo.io",
	}

	config := Config{
		mode:            ConfigModeLocal,
		serviceName:     serviceName,
		serviceAddress:  localHosts[serviceName],
		discoverAddress: localHosts[PzDiscover],
		bindtoAddress:   localHosts[serviceName],
	}

	return &config
}

func getTestConfig(serviceName ServiceName) *Config {

	config := Config{
		mode:            ConfigModeTest,
		serviceName:     serviceName,
		serviceAddress:  "localhost:0",
		discoverAddress: "",
		bindtoAddress:   "localhost:0",
	}

	return &config
}

func getPCFConfig(serviceName ServiceName) (*Config, error) {

	const nonlocalDiscoverHost = "pz-discover.cf.piazzageo.io"

	var config Config
	var err error

	config.mode = ConfigModeCloud

	config.serviceName, config.serviceAddress, err = determineVcapServerAddress()
	if err != nil {
		return nil, err
	}

	log.Printf("got config.ServerAddress: %s", config.serviceAddress)

	config.discoverAddress = nonlocalDiscoverHost

	port := os.Getenv("PORT")
	if port == "" {
		return nil, errors.New("$PORT not found: unable to determine bindto address")
	}
	log.Printf("got port: %s", port)
	config.bindtoAddress = ":" + port
	if err != nil {
		return nil, err
	}
	log.Printf("got config.bindtoAddress: %s", config.bindtoAddress)

	return &config, nil
}

func determineVcapServerAddress() (serviceName ServiceName, serverAddress string, err error) {

	vcapString := os.Getenv("VCAP_APPLICATION")
	if vcapString == "" {
		return "", "", errors.New("$VCAP_APPLICATION not found: unable to determine server address")
	}
	type VcapData struct {
		ApplicationID   string   `json:"application_id"`
		ApplicationName string   `json:"application_name"`
		ApplicationURIs []string `json:"application_uris"`
	}
	var vcap VcapData
	err = json.Unmarshal([]byte(vcapString), &vcap)
	if err != nil {
		return "", "", err
	}
	serviceName = ServiceName(vcap.ApplicationName)
	serverAddress = vcap.ApplicationURIs[0]
	return serviceName, serverAddress, nil
}
