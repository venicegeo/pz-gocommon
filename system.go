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

	svc, err := newElasticSearchService()
	if err != nil {
		return nil, err
	}
	sys.ElasticSearchService = svc
	sys.Services[PzElasticSearch] = svc

	return sys, nil
}

func (sys *System) StartServer(routes http.Handler) chan error {
	done := make(chan error)

	ready := make(chan bool)

	endless.DefaultHammerTime = hammerTime * time.Second
	server := endless.NewServer(sys.Config.GetAddress(), routes)
	server.BeforeBegin = func(_ string) {
		sys.Config.serviceAddress = server.EndlessListener.Addr().(*net.TCPAddr).String()
		ready <- true
	}
	go func() {
		err := server.ListenAndServe()
		done <- err
	}()

	<-ready

	err := sys.WaitForService(sys.Config)
	if err != nil {
		log.Fatal(err)
	}

	err = sys.DiscoverService.RegisterService(sys.Config)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Config.serviceAddress: %s", sys.Config.GetAddress())

	return done
}

func (sys *System) WaitForService(service IService) error {

	var url string
	switch service.GetName() {
	case PzDiscover:
		url = fmt.Sprintf("http://%s/health-check", service.GetAddress())
	default:
		url = fmt.Sprintf("http://%s", service.GetAddress())
	}
	return sys.waitForServiceByURL(service, url)
}

func (sys *System) waitForServiceByURL(service IService, url string) error {
	msTime := 0

	for {
		resp, err := http.Get(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			log.Printf("found service %s", service.GetName())
			return nil
		}
		if msTime >= waitTimeout {
			return fmt.Errorf("timed out waiting for service: %s at %s", service.GetName(), url)
		}
		time.Sleep(waitSleep * time.Millisecond)
		msTime += waitSleep
	}
	/* notreached */
}
