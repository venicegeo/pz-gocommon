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

	GetDataForService(name string) *DiscoverData

	RegisterService(IService) error
	RegisterServiceByName(name string, address string) error
	UnregisterService(name string) error
}

type DiscoverData struct {
	Type string `json:"type"`

	// TODO: which one of these to use?
	Host    string `json:"host,omitempty"`
	Brokers string `json:"brokers,omitempty"`
	Address string `json:"address,omitempty"`
	DbURI   string `json:"db-uri,omitempty"`
}

//type DiscoverDataList map[string]*DiscoverData

///////////////////////////////////////////////////////////////////

type MockDiscoverService struct {
	name    string
	address string

	data map[string]*DiscoverData
}

func NewMockDiscoverService(sys *System) (IDiscoverService, error) {
	var _ IService = new(MockDiscoverService)
	var _ IDiscoverService = new(MockDiscoverService)

	m := make(map[string]*DiscoverData)

	service := MockDiscoverService{
		name: PzDiscover,
		address: sys.Config.discoverAddress,
		data: m,
	}

	return &service, nil
}

func (mock MockDiscoverService) GetName() string {
	return mock.name
}

func (mock MockDiscoverService) GetAddress() string {
	return mock.address
}

func (mock *MockDiscoverService) GetDataForService(name string) *DiscoverData {
	data := (mock.data)[name]
	return data
}
func (mock *MockDiscoverService) RegisterServiceByName(name string, address string) error {
	data := DiscoverData{Type: "core-service", Host: address}
	(mock.data)[name] = &data
	return nil
}

func (mock *MockDiscoverService) RegisterService(service IService) error {
	return mock.RegisterServiceByName(service.GetName(), service.GetAddress())
}

func (mock *MockDiscoverService) UnregisterService(name string) error {
	delete(mock.data, name)
	return nil
}

///////////////////////////////////////////////////////////////////

type PzDiscoverService struct {
	name    string
	address string
	data    map[string]*DiscoverData
	url     string
}

func NewPzDiscoverService(sys *System) (IDiscoverService, error) {
	var _ IService = new(PzDiscoverService)
	var _ IDiscoverService = new(PzDiscoverService)

	service := PzDiscoverService{
		name: PzDiscover,
		address: sys.Config.discoverAddress,
		url: "http://" + sys.Config.discoverAddress + "/api/v1/resources",
	}

	err := sys.WaitForService(service)
	if err != nil {
		return nil, err
	}

	service.data, err = service.fetchData()
	if err != nil {
		return nil, err
	}

	return &service, nil
}

func (service PzDiscoverService) GetName() string {
	return service.name
}

func (service PzDiscoverService) GetAddress() string {
	return service.address
}

func (service *PzDiscoverService) GetDataForService(name string) *DiscoverData {
	return (service.data)[name]
}

func (service *PzDiscoverService) fetchData() (map[string]*DiscoverData, error) {

	resp, err := http.Get(service.url)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusInternalServerError {
		return nil, fmt.Errorf("%s (is the Discover service running?)", resp.Status)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(resp.Status)
	}

	data, err := ReadFrom(resp.Body)
	if err != nil {
		return nil, err
	}

	var m map[string]*DiscoverData
	err = json.Unmarshal(data, &m)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (service *PzDiscoverService) RegisterService(svc IService) error {
	return service.RegisterServiceByName(svc.GetName(), svc.GetAddress())
}

func (service *PzDiscoverService) RegisterServiceByName(name string, address string) error {
	data := &DiscoverData{Type: "core-service", Host: address}

	type discoverEntry struct {
		Name string       `json:"name"`
		Data DiscoverData `json:"data"`
	}
	entry := discoverEntry{Name: name, Data: *data}
	body, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	log.Printf("registering to %s: %s", service.url, string(body))
	resp, err := HTTPPut(service.url, ContentTypeJSON, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return errors.New("registration failed: " + resp.Status)
	}

	return nil
}

func (service *PzDiscoverService) UnregisterService(name string) error {

	log.Printf("unregistering %s", name)
	resp, err := HTTPDelete(service.url)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return errors.New("unregistration failed: " + resp.Status)
	}

	return nil
}
