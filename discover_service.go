// Copyright 2016, RadiantBlue Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	GetName() ServiceName
	GetAddress() string

	GetDataForService(name ServiceName) *DiscoverData

	RegisterService(IService) error
	RegisterServiceByName(name ServiceName, address string) error
	UnregisterService(name ServiceName) error
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
	name    ServiceName
	address string

	data map[ServiceName]*DiscoverData
}

func NewMockDiscoverService(sys *System) (IDiscoverService, error) {
	var _ IService = new(MockDiscoverService)
	var _ IDiscoverService = new(MockDiscoverService)

	m := make(map[ServiceName]*DiscoverData)

	service := MockDiscoverService{
		name:    PzDiscover,
		address: sys.Config.discoverAddress,
		data:    m,
	}

	return &service, nil
}

func (mock MockDiscoverService) GetName() ServiceName {
	return mock.name
}

func (mock MockDiscoverService) GetAddress() string {
	return mock.address
}

func (mock *MockDiscoverService) GetDataForService(name ServiceName) *DiscoverData {
	data := (mock.data)[name]
	return data
}
func (mock *MockDiscoverService) RegisterServiceByName(name ServiceName, address string) error {
	data := DiscoverData{Type: "core-service", Host: address}
	(mock.data)[name] = &data
	return nil
}

func (mock *MockDiscoverService) RegisterService(service IService) error {
	return mock.RegisterServiceByName(service.GetName(), service.GetAddress())
}

func (mock *MockDiscoverService) UnregisterService(name ServiceName) error {
	delete(mock.data, name)
	return nil
}

///////////////////////////////////////////////////////////////////

type PzDiscoverService struct {
	name    ServiceName
	address string
	data    map[ServiceName]*DiscoverData
	url     string
}

func NewPzDiscoverService(sys *System) (IDiscoverService, error) {
	var _ IService = new(PzDiscoverService)
	var _ IDiscoverService = new(PzDiscoverService)

	service := PzDiscoverService{
		name:    PzDiscover,
		address: sys.Config.discoverAddress,
		url:     "http://" + sys.Config.discoverAddress + "/api/v1/resources",
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

func (service PzDiscoverService) GetName() ServiceName {
	return service.name
}

func (service PzDiscoverService) GetAddress() string {
	return service.address
}

func (service *PzDiscoverService) GetDataForService(name ServiceName) *DiscoverData {
	return (service.data)[name]
}

func (service *PzDiscoverService) fetchData() (map[ServiceName]*DiscoverData, error) {

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

	var m map[ServiceName]*DiscoverData
	err = json.Unmarshal(data, &m)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (service *PzDiscoverService) RegisterService(svc IService) error {
	return service.RegisterServiceByName(svc.GetName(), svc.GetAddress())
}

func (service *PzDiscoverService) RegisterServiceByName(name ServiceName, address string) error {
	data := &DiscoverData{Type: "core-service", Host: address}

	type discoverEntry struct {
		Name ServiceName  `json:"name"`
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

func (service *PzDiscoverService) UnregisterService(name ServiceName) error {

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
