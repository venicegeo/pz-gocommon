package piazza

import (
	"encoding/json"
	"errors"
	"flag"
	"log"
	"os"
)

// (1) determine what my own address is
//
//     if -local, then
//         set serverAddress to "localhost:1234x"
//     else if $VCAP_APPLICATION is set, then
//         parse the value as JSON
//         set serverAddress to the "application_uris" string
//     else
//         panic
//     endif
//
// (2) determine where pz-discover lives
//
//     if -local, then
//         set discoverAddress to "localhost:3000"
//     else
//         set discoverAddress to "pz-discover.cf.piazzageo.io"
//     endif
//
// (3) register myself with pz-discover
//
//     do a POST to discoverAddress:
//         {
//             "name": "pz-myname",
//             "data": {
//                 "type": "core-service",
//                 "address": serverAddress,
//                 # other per-service stuff
//              }
//          }
//
// (4) start server
//
//     if -local, then
//         start server on "localhost:1234x"   # from mpg's dev settings
//     else if $PORT set, then
//         start server on ":$PORT"
//     else
//         panic
//     endif

type ServiceConfig struct {
	Local           bool
	ServiceName     string
	ServerAddress   string
	BindTo          string
	DiscoverAddress string
	ElasticSearch    *ElasticSearch
}

func GetConfig(serviceName string, local bool) (*ServiceConfig, error) {

	var config *ServiceConfig
	var err error

	if local {
		config = getLocalConfig(serviceName)
	} else {
		config, err = getPCFConfig(serviceName)
		if err != nil {
			return nil, err
		}
	}

	log.Printf("Config.Local: %t", config.Local)
	log.Printf("Config.ServerAddress: %s", config.ServerAddress)
	log.Printf("Config.ServiceName: %s", config.ServiceName)
	log.Printf("Config.DiscoverAddress: %s", config.DiscoverAddress)
	log.Printf("Config.BindTo: %s", config.BindTo)

	config.ElasticSearch, err = newElasticSearch()
	if err != nil {
		return nil, err
	}

	return config, err
}

func IsLocalConfig() bool {
	localFlag := flag.Bool("local", false, "use localhost ports")
	flag.Parse()
	return *localFlag
}

func getLocalConfig(serviceName string) *ServiceConfig {

	var localHosts = map[string]string{
		"pz-logger":   "localhost:12341",
		"pz-uuidgen":  "localhost:12340",
		"pz-alerter":  "localhost:12342",
		"pz-discover": "localhost:3000",
	}

	config := ServiceConfig{
		Local:           true,
		ServiceName:     serviceName,
		ServerAddress:   localHosts[serviceName],
		DiscoverAddress: localHosts["pz-discover"],
		BindTo:          localHosts[serviceName],
	}

	return &config
}

func getPCFConfig(serviceName string) (*ServiceConfig, error) {

	const nonlocalDiscoverHost = "pz-discover.cf.piazzageo.io"

	var config ServiceConfig
	var err error

	config.Local = false

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
		ApplicationID   string `json:"application_id"`
		ApplicationName string `json:"application_name"`
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
