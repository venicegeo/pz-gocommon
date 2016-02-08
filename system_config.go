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
		PzLogger:   "localhost:12341",
		PzUuidgen:  "localhost:12340",
		PzAlerter:  "localhost:12342",
		PzDiscover: "localhost:3000",
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
