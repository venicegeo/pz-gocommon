package piazza

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
)

type DiscoverService struct {
	config *SystemConfig
	Data   *DiscoverDataList
}

func NewDiscoverService(config *SystemConfig) (*DiscoverService, error) {
	discover := DiscoverService{config: config, Data: new(DiscoverDataList)}

	discover.Update()

	return &discover, nil
}

func (discover *DiscoverService) Update() error {
	err := discover.fetchData()
	if err != nil {
		return err
	}

	return nil
}

func (*DiscoverService) GetName() string {
	return "pz-discover"
}

func (discover *DiscoverService) GetAddress() string {
	return discover.config.DiscoverAddress
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

func (discover *DiscoverService) fetchData() error {

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

	discover.Data = &m

	return nil
}

func (discover *DiscoverService) RegisterService(name string, data *DiscoverData) error {

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

func (discover *DiscoverService) UnregisterService(name string) error {

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
