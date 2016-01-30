package piazza

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
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

type Config struct {
	ServiceName     string
	ServerAddress   string
	BindTo          string
	DiscoverAddress string
}

func GetConfig(serviceName string, local bool) (*Config, error) {

	var config *Config
	var err error

	if local {
		config = getLocalConfig(serviceName)
	} else {
		config, err = getCFConfig(serviceName)
		if err != nil {
			return nil, err
		}
	}

	log.Printf("starting: local=%t", true)
	log.Printf("serverAddress: %s", config.ServerAddress)
	log.Printf("serviceName: %s", config.ServiceName)
	log.Printf("discoverAddress: %s", config.DiscoverAddress)
	log.Printf("bindTo: %s", config.BindTo)

	return config, err
}

func IsLocalConfig() bool {
	localFlag := flag.Bool("local", false, "use localhost ports")
	flag.Parse()
	return *localFlag
}

func getLocalConfig(serviceName string) *Config {

	var localHosts = map[string]string{
		"pz-logger":   "localhost:12341",
		"pz-uuidgen":  "localhost:12340",
		"pz-alerter":  "localhost:12342",
		"pz-discover": "localhost:3000",
	}

	config := Config{
		ServiceName:     serviceName,
		ServerAddress:   localHosts[serviceName],
		DiscoverAddress: localHosts["pz-discover"],
		BindTo:          localHosts[serviceName],
	}

	return &config
}

func getCFConfig(serviceName string) (*Config, error) {

	const nonlocalDiscoverHost = "pz-discover.cf.piazzageo.io"

	var config Config
	var err error

	config.ServiceName, config.ServerAddress, err = determineVcapServerAddress()
	if err != nil {
		return nil, err
	}

	config.DiscoverAddress = nonlocalDiscoverHost

	port := os.Getenv("$PORT")
	if port == "" {
		return nil, errors.New("unable to determine bindto address from $PORT")
	}
	config.BindTo = ":" + port
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func determineVcapServerAddress() (serviceName string, serverAddress string, err error) {

	vcapString := os.Getenv("$VCAP_APPLICATION")
	if vcapString == "" {
		return "", "", errors.New("unable to determine server address")
	}
	type VcapData struct {
		ApplicationID   string `json:"application_id"`
		ApplicationName string `json:"application_name"`
		ApplicationURIs string `json:"application_uris"`
	}
	var vcap VcapData
	err = json.Unmarshal([]byte(vcapString), &vcap)
	if err != nil {
		return "", "", err
	}
	serviceName = vcap.ApplicationName
	serverAddress = vcap.ApplicationURIs
	return serviceName, serverAddress, nil
}

type discoverDataDetail struct {
	Type    string `json:"type"`
	Host    string `json:"host,omitempty"`
	Brokers string `json:"brokers,omitempty"`
}
type discoverData struct {
	Name string      `json:"name"`
	Data interface{} `json:"data"`
}

func (config *Config) RegisterServiceWithDiscover() error {
	discDataDetail := discoverDataDetail{Type: "core-service", Host: config.ServerAddress}
	discData := discoverData{Name: config.ServiceName, Data: discDataDetail}
	data, err := json.Marshal(discData)
	if err != nil {
		return err
	}

	discoverUrl := fmt.Sprintf("http://%s/api/v1/resources", config.DiscoverAddress)
	log.Printf("registering to %s: %s", discoverUrl, string(data))
	resp, err := Put(discoverUrl, ContentTypeJSON, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return errors.New("registration failed: " + resp.Status)
	}

	return nil
}
