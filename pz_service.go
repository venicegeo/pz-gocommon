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
	DiscoverAddress string // host:port
	loggerAddress   string
	uuidgenAddress  string
	Debug           bool
}

func NewPzService(name string, serviceAddress string, discoverAddress string, debug bool) (pz *PzService, err error) {
	pz = &PzService{Name: name, Address: serviceAddress, DiscoverAddress: discoverAddress, Debug: debug}

	pz.loggerAddress, err = pz.getServiceAddress("pz-logger")
	if err != nil {
		return nil, err
	}

	pz.uuidgenAddress, err = pz.getServiceAddress("pz-uuidgen")
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

	resp, err := http.Post("http://"+pz.loggerAddress+"/log", ContentTypeJSON, bytes.NewBuffer(data))
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

// GetServiceAddress returns the URL of the given service.
// If the service is not found, a non-nil error is returned.
func (pz *PzService) getServiceAddress(name string) (string, error) {

	registryURL := "http://" + pz.DiscoverAddress + "/api/v1/resources"

	target := fmt.Sprintf("%s/%s", registryURL, name)

	resp, err := http.Get(target)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", errors.New(resp.Status)
	}

	data, err := ReadFrom(resp.Body)
	if err != nil {
		return "", err
	}

	var m discoverDataDetail
	err = json.Unmarshal(data, &m)
	if err != nil {
		return "", err
	}

	return m.Host, nil
}

func (pz *PzService) GetUuid() (string, error) {

	url := "http://" + pz.uuidgenAddress + "/uuid"

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
