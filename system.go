package piazza

import (
	"errors"
	"net/http"
	"fmt"
	"time"
)

type System struct {
	Config          *SystemConfig

	DiscoverService IDiscoverService
	ElasticSearch   *ElasticSearch
}

func NewSystem(config* SystemConfig) (*System, error) {
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

	discoverData := &DiscoverData{Type: "core-service", Host: sys.Config.ServerAddress}

	switch sys.Config.Mode {

	case ConfigModeCloud, ConfigModeLocal:
		sys.DiscoverService, err = NewPzDiscoverService(sys)
		if err != nil {
			return err
		}
		err = sys.DiscoverService.RegisterService(sys.Config.ServiceName, discoverData)
		if err != nil {
			return err
		}
	case ConfigModeTest:
		sys.DiscoverService, err = NewMockDiscoverService(sys)
		if err != nil {
			return err
		}
		err = sys.DiscoverService.RegisterService(sys.Config.ServiceName, discoverData)
		if err != nil {
			return err
		}
	}
	return nil
}

func (sys *System) WaitForService(name string, msTimeout int) error {
	data, err := sys.DiscoverService.GetData(name)
	if err != nil {
		return err
	}

	var url string
	switch name {
	case "pz-discover":
		url = fmt.Sprintf("http://%s/health-check", data.Host)
	default:
		url = fmt.Sprintf("http://%s", data.Host)
	}
	return sys.WaitForServiceByUrl(url, msTimeout)
}

func (sys *System) WaitForServiceByUrl(url string, msTimeout int) error {
	msTime := 0
	const msSleep = 50

	for {
		resp, err := http.Get(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			return nil
		}
		if msTime == msTimeout {
			return errors.New(fmt.Sprintf("timed out waiting for service: %s", url))
		}
		time.Sleep(msSleep * time.Millisecond)
		msTime += msSleep
	}
	/* notreached */
}
