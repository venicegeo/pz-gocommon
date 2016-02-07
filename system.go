package piazza

import (
	"fmt"
	"github.com/fvbock/endless"
	"log"
	"net"
	"net/http"
	"time"
)

const (
	PzDiscover      = "pz-discover"
	PzLogger        = "pz-logger"
	PzUuidgen       = "pz-uuidgen"
	PzAlerter       = "pz-alerter"
	PzElasticSearch = "elastic-search"
)

type System struct {
	Config *Config

	ElasticSearchService *ElasticSearchService

	DiscoverService IDiscoverService
	Services        map[string]IService
}

const waitTimeout = 1000
const waitSleep = 100
const hammerTime = 3

func NewSystem(config *Config) (*System, error) {
	var err error

	sys := &System{
		Config:   config,
		Services: make(map[string]IService),
	}

	switch sys.Config.mode {
	case ConfigModeCloud, ConfigModeLocal:
		sys.DiscoverService, err = NewPzDiscoverService(sys)
		if err != nil {
			return nil, err
		}
	case ConfigModeTest:
		sys.DiscoverService, err = NewMockDiscoverService(sys)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("Invalid config mode: %s", sys.Config.mode)
	}
	sys.Services[PzDiscover] = sys.DiscoverService

	sys.ElasticSearchService, err = newElasticSearchService()
	if err != nil {
		return nil, err
	}
	sys.Services[PzElasticSearch] = sys.ElasticSearchService

	return sys, nil
}

func (sys *System) StartServer(routes http.Handler) chan error {
	done := make(chan error)

	ready := make(chan bool)

	endless.DefaultHammerTime = hammerTime * time.Second
	server := endless.NewServer(sys.Config.GetBindToAddress(), routes)
	server.BeforeBegin = func(_ string) {
		sys.Config.bindtoAddress = server.EndlessListener.Addr().(*net.TCPAddr).String()
		ready <- true
	}
	go func() {
		err := server.ListenAndServe()
		done <- err
	}()

	<-ready

	err := sys.WaitForServiceByName(sys.Config.GetName(), sys.Config.GetBindToAddress())
	if err != nil {
		log.Fatal(err)
	}

	err = sys.DiscoverService.RegisterService(sys.Config)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Config.serviceAddress: %s", sys.Config.GetAddress())
	log.Printf("Config.bindtoAddress: %s", sys.Config.GetBindToAddress())

	return done
}

func (sys *System) WaitForServiceByName(name string, address string) error {

	var url string
	switch name {
	case PzDiscover:
		url = fmt.Sprintf("http://%s/health-check", address)
	default:
		url = fmt.Sprintf("http://%s", address)
	}
	return sys.waitForServiceByURL(name, url)
}

func (sys *System) WaitForService(service IService) error {
	return sys.WaitForServiceByName(service.GetName(), service.GetAddress())
}

func (sys *System) waitForServiceByURL(name string, url string) error {
	msTime := 0

	for {
		resp, err := http.Get(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			log.Printf("found service %s", name)
			return nil
		}
		if msTime >= waitTimeout {
			return fmt.Errorf("timed out waiting for service: %s at %s", name, url)
		}
		time.Sleep(waitSleep * time.Millisecond)
		msTime += waitSleep
	}
	/* notreached */
}
