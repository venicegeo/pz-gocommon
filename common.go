package piazza

import (
	"log"
	"net/http"
	"time"
	"fmt"
	"errors"
	"encoding/json"
	"bytes"
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

	mssg := LogMessage{Service: service, Address: address, Severity: severity, Message: message, Time: time.Now().String()}
	data, err := json.Marshal(mssg)
	if err != nil {
		return err
	}

	resp, err := http.Post("http://localhost:12341/log", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New("log post failed")
	}

	return nil
}
