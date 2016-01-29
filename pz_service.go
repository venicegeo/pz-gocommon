package piazza

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
	"io/ioutil"
)

type PzService struct {
	Name            string
	Address         string // host:port
	ServiceAddresses map[string]string // {"pz-uuidgen":"localhost:1234", ...}
	Debug           bool
	ElasticSearch   *ElasticSearch
}

func NewPzService(name string, serviceAddress string, discoverAddress string, debug bool) (pz *PzService, err error) {
	pz = &PzService{Name: name, Address: serviceAddress, Debug: debug}

	pz.ServiceAddresses = make(map[string]string)
	pz.ServiceAddresses["pz-discover"] = discoverAddress

	err = pz.setServiceAddresses()
	if err != nil {
		return nil, err
	}

	pz.ElasticSearch, err = newElasticSearch()

	if err != nil {
		return nil, err
	}

	return pz, nil
}

func (pz *PzService) postLogMessage(mssg *LogMessage) error {

	data, err := json.Marshal(mssg)
	if err != nil {
		log.Printf("pz-logger failed to marshall request: %v", err)
		return err
	}

	resp, err := http.Post("http://"+pz.ServiceAddresses["pz-logger"] +"/v1/messages", ContentTypeJSON, bytes.NewBuffer(data))
	if err != nil {
		log.Printf("pz-logger failed to post request: %v", err)
		return err
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("pz-logger failed to post request: %v", err)
		return errors.New(resp.Status)
	}

	return nil
}

// Log sends a LogMessage to the logger.
// TODO: support fmt
func (pz *PzService) Log(severity string, message string) error {

	mssg := LogMessage{Service: pz.Name, Address: pz.Address, Severity: severity, Message: message, Time: time.Now().String()}

	return pz.postLogMessage(&mssg)
}

func (pz *PzService) Fatal(err error) {
	log.Printf("Fatal: %v", err)

	mssg := LogMessage{Service: pz.Name, Address: pz.Address, Severity: SeverityFatal, Message: fmt.Sprintf("%v", err), Time: time.Now().String()}
	pz.postLogMessage(&mssg)

	os.Exit(1)
}

func (pz *PzService) Error(text string, err error) error {
	log.Printf("Error: %v", err)

	s := fmt.Sprintf("%s: %v", text, err)

	mssg := LogMessage{Service: pz.Name, Address: pz.Address, Severity: SeverityError, Message: s, Time: time.Now().String()}
	return pz.postLogMessage(&mssg)
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

	for k,v := range(m) {
		if k == "kafka" {
			pz.ServiceAddresses[k] = v.Brokers
		} else {
			pz.ServiceAddresses[k] = v.Host
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
		pz.Error("PzService.GetUuid", err)
	}

	uuids, ok := data["data"]
	if !ok {
		pz.Error("PzService.GetUuid: returned data has invalid data type", nil)
	}

	if len(uuids) != 1 {
		pz.Error("PzService.GetUuid: returned array wrong size", nil)
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

