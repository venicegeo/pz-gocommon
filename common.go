package piazza

import (
	"log"
	"net/http"
	"time"
	"fmt"
	"errors"
	"encoding/json"
	"bytes"
	"io/ioutil"
	"io"
)

//---------------------------------------------------------------------------

type AdminResponse_UuidGen struct {
	NumRequests int `json:"num_requests"`
	NumUUIDs    int `json:"num_uuids"`
}

type AdminResponse_Logger struct {
	NumMessages int `json:"num_messages"`
}

type AdminResponse struct {
	StartTime time.Time              `json:"starttime"`
	UuidGen   *AdminResponse_UuidGen `json:"uuidgen,omitempty"`
	Logger    *AdminResponse_Logger  `json:"logger,omitempty"`
}

func Adder(a int, b int) int {
	return a + b
}

//---------------------------------------------------------------------------

func HttpLogHandler(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s %s", r.RemoteAddr, r.Method, r.URL)
		handler.ServeHTTP(w, r)
	})
}

const HttpContentJson = "application/json"


func HttpPut(url string, contentType string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", contentType)
	client := &http.Client{}
	return client.Do(req)
}

//---------------------------------------------------------------------------

// all fields required
type LogMessage struct {
	Service  string `json:"service"`
	Address  string `json:"address"`
	Time     string `json:"time"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
}

func (m *LogMessage) ToString() string {
	s := fmt.Sprintf("[%s, %s, %s, %s, %s]",
		m.Service, m.Address, m.Time, m.Severity, m.Message)
	return s
}

const SeverityDebug = "Debug"
const SeverityInfo = "Info"
const SeverityWarning = "Warning"
const SeverityError = "Error"
const SeverityFatal = "Fatal"

func (mssg *LogMessage) Validate() error {
	if mssg.Service == "" {
		return errors.New("required field 'service' not set")
	}
	if mssg.Address == "" {
		return errors.New("required field 'address' not set")
	}
	if mssg.Time == "" {
		return errors.New("required field 'time' not set")
	}
	if mssg.Severity == "" {
		return errors.New("required field 'severity' not set")
	}
	if mssg.Message == "" {
		return errors.New("required field 'message' not set")
	}

	ok := false
	for _, code := range([...]string{SeverityDebug, SeverityInfo, SeverityWarning, SeverityError, SeverityFatal}) {
		if mssg.Severity == code {
			ok = true
			break
		}
	}
	if !ok {
		return errors.New("invalid 'severity' setting")
	}

	return nil
}

func SendLogMessage(service string, address string, severity string, message string) error {

	address, err := GetServiceAddress("pz-logger")
	if err != nil {
		return err
	}
	log.Print(address)

	mssg := LogMessage{Service: service, Address: address, Severity: severity, Message: message, Time: time.Now().String()}
	data, err := json.Marshal(mssg)
	if err != nil {
		return err
	}

	resp, err := http.Post(address, HttpContentJson, bytes.NewBuffer(data))
	if err != nil {
		log.Print(77)
		return err
	}
	log.Print(66)

	if resp.StatusCode != http.StatusOK {
		return errors.New(resp.Status)
	}

	return nil
}

//---------------------------------------------------------------------------

func ReadFrom(reader io.ReadCloser) ([]byte, error) {
	defer reader.Close()
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return data, err
}

//---------------------------------------------------------------------------

// singelton
var registryUrl string

type registryItemData struct {
	Type string `json:"type"`
	Address string `json:"address"`
}
type registryItem struct {
	Name string `json:"name"`
	Data registryItemData `json:"data"`
}

func RegistryInit(url string) {
	registryUrl = url + "/api/v1/resources"
}

func RegisterService(name string, itemtype string, url string) error {

	m := registryItem{Name: name, Data: registryItemData{Type: itemtype, Address: url}}

	data, err := json.Marshal(m)
	if err != nil {
		return err
	}

	resp, err := HttpPut(registryUrl, HttpContentJson,  bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return errors.New(resp.Status)
	}

	return nil
}

func GetServiceAddress(name string) (string, error) {

	target := fmt.Sprintf("%s/%s", registryUrl, name)

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



	var m registryItemData
	err = json.Unmarshal(data, &m)
	if err != nil {
		return "", err
	}

	return m.Address, nil
}
