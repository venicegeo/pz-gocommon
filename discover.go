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
//     if $VCAP_APPLICATION is set, then
//         parse the value as JSON
//         set serverAddress to the "application_uris" string
//     else
//         if "-server server:port" is set, then
//             set serverAddress to "server:port"
//         else
//             set serverAddress to "localhost:12340"   # mpg's dev setting
//         endif
//     endif
//
// (2) determine where pz-discover lives
//
//     if "-discover server:port" is set, then
//         set discoverAddress to "server:port"
//     else
//         set discoverAddress to "localhost:12341"   # mpg's dev setting
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
//     if $PORT set, then
//         start server on ":$PORT"
//     else
//         start server on "localhost:12340"   # mpg's dev setting
//     endif

type discoverService struct {
	defaultDiscoverAddress string
	defaultServerAddress   string
	defaultServiceName     string

	discoverFlag *string
	serverFlag   *string
	DebugFlag    *bool

	serverAddress   string
	serviceName     string
	DiscoverAddress string

	BindTo string
}

func NewDiscoverService(defaultServiceName string, defaultServerAddress string, defaultDiscoverAddress string) (string, string, bool, error) {
	var svc discoverService
	svc.defaultServiceName = defaultServiceName
	svc.defaultServerAddress = defaultServerAddress
	svc.defaultDiscoverAddress = defaultDiscoverAddress

	svc.discoverFlag = flag.String("discover", defaultDiscoverAddress, "server:port of pz-discovery")
	svc.serverFlag = flag.String("server", defaultServerAddress, "server:port of this service")
	svc.DebugFlag = flag.Bool("debug", false, "use debug mode")

	flag.Parse()

	log.Printf("starting: debug=%t", *svc.DebugFlag)

	err := svc.determineServerAddress()
	if err != nil {
		return "", "", false, err
	}

	err = svc.determineDiscoverAddress()
	if err != nil {
		return "", "", false, err
	}

	err = svc.registerServiceWithDiscover()
	if err != nil {
		return "", "", false, err
	}

	err = svc.determineBindAddress()
	if err != nil {
		return "", "", false, err
	}

	return svc.BindTo, svc.DiscoverAddress, *svc.DebugFlag, nil
}

// (1) determine what my own address is
func (svc *discoverService) determineServerAddress() error {
	if vcapString := os.Getenv("$VCAP_APPLICATION"); vcapString != "" {
		type VcapData struct {
			ApplicationID   string `json:"application_id"`
			ApplicationName string `json:"application_name"`
			ApplicationURIs string `json:"application_uris"`
		}
		var vcap VcapData
		err := json.Unmarshal([]byte(vcapString), &vcap)
		if err != nil {
			return err
		}
		svc.serverAddress = vcap.ApplicationURIs
		svc.serviceName = vcap.ApplicationName
	} else {
		svc.serverAddress = *svc.serverFlag
		svc.serviceName = svc.defaultServiceName
	}
	log.Printf("serverAddress: %s", svc.serverAddress)
	log.Printf("serviceName: %s", svc.serviceName)

	return nil
}

// (2) determine where pz-discover lives
func (svc *discoverService) determineDiscoverAddress() error {
	svc.DiscoverAddress = *svc.discoverFlag
	log.Printf("discoverAddress: %s", svc.DiscoverAddress)
	return nil
}

// (3) register myself with pz-discover
type discoverDataDetail struct {
	Type string `json:"type"`
	Host string `json:"host"`
	Brokers string `json:"brokers,omitempty"`
}
type discoverData struct {
	Name string      `json:"name"`
	Data interface{} `json:"data"`
}

func (svc *discoverService) registerServiceWithDiscover() error {
	discoverDataDetail := discoverDataDetail{Type: "core-service", Host: svc.serverAddress}
	discoverData := discoverData{Name: svc.serviceName, Data: discoverDataDetail}
	data, err := json.Marshal(discoverData)
	if err != nil {
		return err
	}

	discoverUrl := fmt.Sprintf("http://%s/api/v1/resources", svc.DiscoverAddress)
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

func (svc *discoverService) determineBindAddress() error {
	// (4) we have to bind our server to something special, not just serverAddress
	port := os.Getenv("$PORT")
	if port != "" {
		svc.BindTo = ":" + port
	} else {
		svc.BindTo = svc.serverAddress
	}
	log.Printf("bindTo: %s", svc.BindTo)

	return nil
}
