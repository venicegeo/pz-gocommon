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
	config *SystemConfig
	data   *DiscoverDataList
}

func NewMockDiscoverService(sys *System) (IDiscoverService, error) {
	discover := MockDiscoverService{config: sys.Config}

	discover.data = &DiscoverDataList{}
	(*discover.data)[sys.Config.ServiceName] = DiscoverData{Type: "core-service", Host: sys.Config.ServerAddress}

	return &discover, nil
}

func (mock *MockDiscoverService) GetName() string {
	return "pz-discover"
}

func (mock *MockDiscoverService) GetAddress() string {
	return "0.0.0.0"
}

func (mock *MockDiscoverService) GetData(name string) (*DiscoverData, error) {
	data := (*mock.data)[name]
	return &data, nil
}

func (mock *MockDiscoverService) RegisterService(name string, data *DiscoverData) error {
	log.Print("register")
	log.Print(mock.data)
	log.Print(name)
	log.Print(data)
	(*mock.data)[name] = *data
	return nil
}

func (mock *MockDiscoverService) UnregisterService(name string) error {
	delete(*mock.data, name)
	return nil
}

///////////////////////////////////////////////////////////////////

type PzDiscoverService struct {
	config *SystemConfig
	data   *DiscoverDataList
}

func NewPzDiscoverService(sys *System) (IDiscoverService, error) {
	discover := PzDiscoverService{config: sys.Config}

	err := sys.WaitForServiceByUrl("http://" + sys.Config.DiscoverAddress + "/health-check", 1000)
	if err != nil {
		return nil, err
	}

	err = discover.update()
	if err != nil {
		return nil, err
	}

	return &discover, nil
}

func (*PzDiscoverService) GetName() string {
	return "pz-discover"
}

func (discover *PzDiscoverService) GetAddress() string {
	return discover.config.DiscoverAddress
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
