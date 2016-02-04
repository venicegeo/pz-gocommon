package piazza

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"
)

type DiscoverClient struct {
	DiscoverAddress string
	data *discoverDataList
}

func NewDiscoverClient(config *ServiceConfig) (*DiscoverClient, error) {
	c := DiscoverClient{DiscoverAddress: config.DiscoverAddress}

	err := c.setServiceAddresses()
	if err != nil {
		return nil, err
	}

	return &c, nil
}


type discoverData struct {
	Type    string `json:"type"`

	// TODO: which one of these to use?
	Host    string `json:"host,omitempty"`
	Brokers string `json:"brokers,omitempty"`
	Address string `json:"address,omitempty"`
	DbUri   string `json:"db-uri,omitempty"`
}

type discoverDataList map[string]discoverData

func (c *DiscoverClient) setServiceAddresses() error {

	url := "http://" + c.DiscoverAddress + "/api/v1/resources"

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

	var m discoverDataList
	err = json.Unmarshal(data, &m)
	if err != nil {
		return err
	}

	c.data = &m

	/*for k, v := range m {
		// TODO: use correct one
		if v.Host != "" {
			pz.ServiceAddresses[k] = v.Host
		} else if v.Address != "" {
			pz.ServiceAddresses[k] = v.Address
		} else if v.Brokers != "" {
			pz.ServiceAddresses[k] = v.Brokers
		} else if v.DbUri != "" {
			pz.ServiceAddresses[k] = v.DbUri
		} else {
			return errors.New(fmt.Sprintf("unable to parse discover record: %v", v))
		}
	}*/

	return nil
}

func (c *DiscoverClient) RegisterServiceWithDiscover(name string, address string) error {
	data := discoverData{Type: "core-service", Host: address}

	type discoverEntry struct {
		Name string `json:"name"`
		Data discoverData `json:"data"`
	}
	entry := discoverEntry{Name: name, Data: data}
	body, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	discoverUrl := fmt.Sprintf("http://%s/api/v1/resources", c.DiscoverAddress)
	log.Printf("registering to %s: %s", discoverUrl, string(body))
	resp, err := Put(discoverUrl, ContentTypeJSON, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return errors.New("registration failed: " + resp.Status)
	}

	return nil
}

func (c *DiscoverClient) WaitForService(name string, msTimeout int) error {
	msTime := 0
	const msSleep = 50

	address := (*c.data)[name].Host
	if address == "" {
		return errors.New("service address not known: " + name)
	}

	for {
		resp, err := http.Get("http://" + address)
		if err == nil && resp.StatusCode == http.StatusOK {
			return nil
		}
		if msTime == msTimeout {
			return errors.New(fmt.Sprintf("timed out waiting for service: %s at %s", name, address))
		}
		time.Sleep(msSleep * time.Millisecond)
		msTime += msSleep
	}
	/* notreached */
}
