package piazza

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type PzService struct {
	Name             string
	Address          string            // host:port
	ServiceAddresses map[string]string // {"pz-uuidgen":"localhost:1234", ...}
	Debug            bool
	ElasticSearch    *ElasticSearch
}

func NewPzService(config *Config, debug bool) (pz *PzService, err error) {
	pz = &PzService{Name: config.ServiceName, Address: config.ServerAddress, Debug: debug}

	pz.ServiceAddresses = make(map[string]string)
	pz.ServiceAddresses["pz-discover"] = config.DiscoverAddress

	err = pz.setServiceAddresses()
	if err != nil {
		return nil, err
	}

	pz.ElasticSearch, err = newElasticSearch()
	if err != nil {
		return nil, err
	}

	log.Printf("PzService.Name: %s", pz.Name)
	log.Printf("PzService.Address: %s", pz.Address)
	log.Printf("PzService.ServiceAddress: %s", pz.ServiceAddresses)
	log.Printf("PzService.Debug: %t", pz.Debug)
	log.Printf("PzService.ElasticSearch: %v", pz.ElasticSearch)

	return pz, nil
}


func (pz *PzService) setServiceAddresses() error {

	url := "http://" + pz.ServiceAddresses["pz-discover"] + "/api/v1/resources"

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

	var m map[string]discoverDataDetail
	err = json.Unmarshal(data, &m)
	if err != nil {
		return err
	}

	for k, v := range m {
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
	}

	return nil
}

func (pz *PzService) GetUuid() (string, error) {

	url := "http://" + pz.ServiceAddresses["pz-uuidgen"] + "/v1/uuids"

	resp, err := http.Post(url, "text/plain", nil)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", errors.New(resp.Status)
	}

	body, err := ioutil.ReadAll(resp.Body)

	var data map[string][]string
	err = json.Unmarshal(body, &data)
	if err != nil {
		//pz.Error("PzService.GetUuid", err)
		log.Printf("PzService.GetUuid: %v", err)
	}

	uuids, ok := data["data"]
	if !ok {
		//pz.Error("PzService.GetUuid: returned data has invalid data type", nil)
		log.Printf("PzService.GetUuid: returned data has invalid data type")
	}

	if len(uuids) != 1 {
		//pz.Error("PzService.GetUuid: returned array wrong size", nil)
		log.Printf("PzService.GetUuid: returned array wrong size")
	}

	return uuids[0], nil
}

func (pz *PzService) WaitForService(name string, msTimeout int) error {
	msTime := 0
	const msSleep = 50

	address := pz.ServiceAddresses[name]
	if address == "" {
		return errors.New("service not discovered: " + name)
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
