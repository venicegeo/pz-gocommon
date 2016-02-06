package piazza

import (
	"errors"
	"fmt"
	"github.com/fvbock/endless"
	"net"
	"net/http"
	"log"
	"time"
)

type System struct {
	Config *SystemConfig

	DiscoverService IDiscoverService
	ElasticSearch   *ElasticSearch
}

const WaitTimeout = 1000
const WaitSleep = 100

func NewSystem(config *SystemConfig) (*System, error) {
	//var err error

	sys := System{Config: config}

	err := sys.setupDiscover()
	if err != nil {
		return nil, err
	}

	sys.ElasticSearch, err = newElasticSearch()
	if err != nil {
		return nil, err
	}

	return &sys, nil
}

func (sys *System) setupDiscover() error {
	var err error

	switch sys.Config.Mode {
	case ConfigModeCloud, ConfigModeLocal:
		sys.DiscoverService, err = NewPzDiscoverService(sys)
		if err != nil {
			return err
		}
	case ConfigModeTest:
		sys.DiscoverService, err = NewMockDiscoverService(sys)
		if err != nil {
			return err
		}
	}

	return nil
}

func (sys *System) RegisterService(name string, address string) error {
	discoverData := &DiscoverData{Type: "core-service", Host: address}
	sys.DiscoverService.RegisterService(name, discoverData)
	return nil
}

func (sys *System) UnregisterService(name string) error {
	sys.DiscoverService.UnregisterService(name)
	return nil
}

func (sys *System) StartServer(routes http.Handler) chan error {
	done := make(chan error)

	ready := make(chan bool)

	endless.DefaultHammerTime = 3 * time.Second
	server := endless.NewServer(sys.Config.BindTo, routes)
	server.BeforeBegin = func(_ string) {
		sys.Config.BindTo = server.EndlessListener.Addr().(*net.TCPAddr).String()
		ready <- true
	}
	go func() {
		err := server.ListenAndServe()
		done <- err
	}()

	<-ready

	err := sys.WaitForService(sys.Config.ServiceName, sys.Config.BindTo)
	if err != nil {
		log.Fatal(err)
	}

	err = sys.RegisterService(sys.Config.ServiceName, sys.Config.BindTo)
	if err != nil {
		log.Fatal(err)
	}

	return done
}

func (sys *System) WaitForService(name string, address string) error {

	var url string
	switch name {
	case "pz-discover":
		url = fmt.Sprintf("http://%s/health-check", address)
	default:
		url = fmt.Sprintf("http://%s", address)
	}
	return sys.waitForServiceByUrl(name, url)
}

func (sys *System) waitForServiceByUrl(name string, url string) error {
	msTime := 0

	for {
		resp, err := http.Get(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			log.Printf("found service %s", name)
			return nil
		}
		if msTime >= WaitTimeout {
			return errors.New(fmt.Sprintf("timed out waiting for service: %s at %s", name, url))
		}
		time.Sleep(WaitSleep * time.Millisecond)
		msTime += WaitSleep
	}
	/* notreached */
}
