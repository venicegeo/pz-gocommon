package piazza

import (
	"errors"
	"net/http"
	"fmt"
	"time"
)

type System struct {
	Config      *SystemConfig

	DiscoverSvc *DiscoverService
	ElasticSearch *ElasticSearch
}

func NewSystem(config* SystemConfig) (*System, error) {
	var err error

	sys := System{Config: config}

	err = sys.setupDiscover()
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
	sys.DiscoverSvc, err = NewDiscoverService(sys.Config)
	if err != nil {
		return err
	}

	err = sys.WaitForService("pz-discover", 1000)
	if err != nil {
		return err
	}

	discoverData := &DiscoverData{Type: "core-service", Host: sys.Config.ServerAddress}
	err = sys.DiscoverSvc.RegisterService(sys.Config.ServiceName, discoverData)
	if err != nil {
		return err
	}

	return nil
}

func (sys *System) WaitForService(name string, msTimeout int) error {
	msTime := 0
	const msSleep = 50

	data, ok := (*sys.DiscoverSvc.Data)[name]
	if !ok {
		return errors.New(fmt.Sprintf("service not known: %s", name))
	}

	for {
		resp, err := http.Get(data.HealthUrl)
		if err == nil && resp.StatusCode == http.StatusOK {
			return nil
		}
		if msTime == msTimeout {
			return errors.New(fmt.Sprintf("timed out waiting for service: %s at %s", name, data.HealthUrl))
		}
		time.Sleep(msSleep * time.Millisecond)
		msTime += msSleep
	}
	/* notreached */
}
