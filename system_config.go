package piazza

import (
	"encoding/json"
	"errors"
	"flag"
	"log"
	"os"
)

type ConfigMode int

const (
	ConfigModeLocal = iota
	ConfigModeTest
	ConfigModeCloud
)

type SystemConfig struct {
	Mode            ConfigMode
	ServiceName     string
	ServerAddress   string
	BindTo          string
	DiscoverAddress string
}

func NewConfig(serviceName string, configType ConfigMode) (*SystemConfig, error) {

	var config *SystemConfig
	var err error

	switch configType {
	case ConfigModeLocal:
		config = getLocalConfig(serviceName)
	case ConfigModeTest:
		config = getLocalConfig(serviceName)
	case ConfigModeCloud:
		config, err = getPCFConfig(serviceName)
		if err != nil {
			return nil, err
		}
	}

	log.Printf("Config.Mode: %s", string(config.Mode))
	log.Printf("Config.ServerAddress: %s", config.ServerAddress)
	log.Printf("Config.ServiceName: %s", config.ServiceName)
	log.Printf("Config.DiscoverAddress: %s", config.DiscoverAddress)
	log.Printf("Config.BindTo: %s", config.BindTo)

	return config, err
}

func IsLocalConfig() bool {
	localFlag := flag.Bool("local", false, "use localhost ports")
	flag.Parse()
	return *localFlag
}

func getLocalConfig(serviceName string) *SystemConfig {

	var localHosts = map[string]string{
		"pz-logger":   "localhost:12341",
		"pz-uuidgen":  "localhost:12340",
		"pz-alerter":  "localhost:12342",
		"pz-discover": "localhost:3000",
	}

	config := SystemConfig{
		Mode:            ConfigModeLocal,
		ServiceName:     serviceName,
		ServerAddress:   localHosts[serviceName],
		DiscoverAddress: localHosts["pz-discover"],
		BindTo:          localHosts[serviceName],
	}

	return &config
}

func getPCFConfig(serviceName string) (*SystemConfig, error) {

	const nonlocalDiscoverHost = "pz-discover.cf.piazzageo.io"

	var config SystemConfig
	var err error

	config.Mode = ConfigModeCloud

	config.ServiceName, config.ServerAddress, err = determineVcapServerAddress()
	if err != nil {
		return nil, err
	}

	config.DiscoverAddress = nonlocalDiscoverHost

	port := os.Getenv("PORT")
	if port == "" {
		return nil, errors.New("$PORT not found: unable to determine bindto address")
	}
	config.BindTo = ":" + port
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func determineVcapServerAddress() (serviceName string, serverAddress string, err error) {

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
	serviceName = vcap.ApplicationName
	serverAddress = vcap.ApplicationURIs[0]
	return serviceName, serverAddress, nil
}
