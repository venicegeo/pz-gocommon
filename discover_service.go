package piazza

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
)

type IDiscoverService interface {
	GetName() string
	GetAddress() string
	GetData(name string) (*DiscoverData, error)
	RegisterService(name string, data *DiscoverData) error
	UnregisterService(name string) error
}

type DiscoverData struct {
	Type string `json:"type"`

	// TODO: which one of these to use?
	Host    string `json:"host,omitempty"`
	Brokers string `json:"brokers,omitempty"`
	Address string `json:"address,omitempty"`
	DbUri   string `json:"db-uri,omitempty"`

	HealthUrl string `json:"db-uri,omitempty"`
}

type DiscoverDataList map[string]DiscoverData

///////////////////////////////////////////////////////////////////

type MockDiscoverService struct {
	config  *SystemConfig
	data    *DiscoverDataList
	Name    string
	Address string
}

func NewMockDiscoverService(sys *System) (IDiscoverService, error) {
	var _ IService = new(MockDiscoverService)
	var _ IDiscoverService = new(MockDiscoverService)

	discover := MockDiscoverService{config: sys.Config, Name: "pz-discover", Address: sys.Config.DiscoverAddress}

	discover.data = &DiscoverDataList{}
	(*discover.data)[sys.Config.ServiceName] = DiscoverData{Type: "core-service", Host: sys.Config.ServerAddress}

	return &discover, nil
}

func (mock *MockDiscoverService) GetName() string {
	return mock.Name
}

func (mock *MockDiscoverService) GetAddress() string {
	return mock.Address
}

func (mock *MockDiscoverService) GetData(name string) (*DiscoverData, error) {
	data := (*mock.data)[name]
	return &data, nil
}

func (mock *MockDiscoverService) RegisterService(name string, data *DiscoverData) error {
	(*mock.data)[name] = *data
	return nil
}

func (mock *MockDiscoverService) UnregisterService(name string) error {
	delete(*mock.data, name)
	return nil
}

///////////////////////////////////////////////////////////////////

type PzDiscoverService struct {
	config  *SystemConfig
	data    *DiscoverDataList
	Name    string
	Address string
}

func NewPzDiscoverService(sys *System) (IDiscoverService, error) {
	var _ IService = new(PzDiscoverService)
	var _ IDiscoverService = new(PzDiscoverService)

	service := PzDiscoverService{config: sys.Config, Name: "pz-discover", Address: sys.Config.DiscoverAddress}

	err := sys.WaitForService(&service, 1000)
	if err != nil {
		return nil, err
	}

	err = service.update()
	if err != nil {
		return nil, err
	}

	return &service, nil
}

func (service *PzDiscoverService) GetName() string {
	return service.Name
}

func (service *PzDiscoverService) GetAddress() string {
	return service.Address
}

func (discover *PzDiscoverService) GetData(name string) (*DiscoverData, error) {
	v, ok := (*discover.data)[name]
	if !ok {
		return nil, errors.New("service not found: " + name)
	}
	return &v, nil
}

func (discover *PzDiscoverService) update() error {

	url := "http://" + discover.GetAddress() + "/api/v1/resources"

	resp, err := http.Get(url)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return errors.New(resp.Status)
	}

	data, err := ReadFrom(resp.Body)
	if err != nil {
		return err
	}

	var m DiscoverDataList
	err = json.Unmarshal(data, &m)
	if err != nil {
		return err
	}

	m["pz-discover"] = DiscoverData{
		Type:      "core-service",
		Host:      discover.GetAddress(),
		HealthUrl: fmt.Sprintf("http://%s/api/v1/resources", discover.GetAddress()),
	}

	for k, v := range m {
		if v.HealthUrl == "" {
			v.HealthUrl = fmt.Sprintf("http://%s", v.Host)
			m[k] = v
		}
	}

	discover.data = &m

	return nil
}

func (discover *PzDiscoverService) RegisterService(name string, data *DiscoverData) error {

	type discoverEntry struct {
		Name string       `json:"name"`
		Data DiscoverData `json:"data"`
	}
	entry := discoverEntry{Name: name, Data: *data}
	body, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("http://%s/api/v1/resources", discover.GetAddress())

	log.Printf("registering to %s: %s", url, string(body))
	resp, err := Put(url, ContentTypeJSON, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return errors.New("registration failed: " + resp.Status)
	}

	return nil
}

func (discover *PzDiscoverService) UnregisterService(name string) error {

	url := fmt.Sprintf("http://%s/api/v1/resources", discover.GetAddress())

	log.Printf("unregistering %s", name)
	resp, err := Delete(url)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return errors.New("unregistration failed: " + resp.Status)
	}

	return nil
}
