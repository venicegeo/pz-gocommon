package piazza

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

//---------------------------------------------------------------------------

// An AdminResponse represents the data returned from a call to a service's
// /admin API.
type AdminResponse struct {
	StartTime time.Time             `json:"starttime"`
	Uuidgen   *AdminResponseUuidgen `json:"uuidgen,omitempty"`
	Logger    *AdminResponseLogger  `json:"logger,omitempty"`
}

// AdminResponseUuidgen is the response to pz-uuidgen's /admin call
type AdminResponseUuidgen struct {
	NumRequests int `json:"num_requests"`
	NumUUIDs    int `json:"num_uuids"`
}

// AdminResponseLogger is the response to pz-logger's /admin call
type AdminResponseLogger struct {
	NumMessages int `json:"num_messages"`
}

//---------------------------------------------------------------------------

// ServerLogHandler adds traditional logging support to the http server handlers.
func ServerLogHandler(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s %s", r.RemoteAddr, r.Method, r.URL)
		handler.ServeHTTP(w, r)
	})
}

// ContentTypeJSON is the http content-type for JSON.
const ContentTypeJSON = "application/json"

// Put is because there is no http.Put.
func Put(url string, contentType string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", contentType)
	client := &http.Client{}
	return client.Do(req)
}

// Delete is because there is no http.Delete.
func Delete(url string) (*http.Response, error) {
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return nil, err
	}

	client := &http.Client{}
	return client.Do(req)
}

//---------------------------------------------------------------------------

// LogMessage represents the contents of a messge for the logger service.
// All fields are required.
type LogMessage struct {
	Service  string `json:"service"`
	Address  string `json:"address"`
	Time     string `json:"time"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
}

// ToString returns a LogMessage as a formatted string.
func (mssg *LogMessage) ToString() string {
	s := fmt.Sprintf("[%s, %s, %s, %s, %s]",
		mssg.Service, mssg.Address, mssg.Time, mssg.Severity, mssg.Message)
	return s
}

// SeverityDebug is for log messages that are only used in development.
const SeverityDebug = "Debug"

// SeverityInfo is for log messages that are only informative, no action needed.
const SeverityInfo = "Info"

// SeverityWarning is for log messages that indicate possible problems. Execution continues normally.
const SeverityWarning = "Warning"

// SeverityError is for log messages that indicate something went wrong. The problem is usually handled and execution continues.
const SeverityError = "Error"

// SeverityFatal is for log messages that indicate an internal error and the system is likely now unstable. These should never happen.
const SeverityFatal = "Fatal"

// Validate checks to make sure a LogMessage is properly filled out. If not, a non-nil error is returned.
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
	for _, code := range [...]string{SeverityDebug, SeverityInfo, SeverityWarning, SeverityError, SeverityFatal} {
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

// Log sends a LogMessage to the logger.
func Log(service string, address string, severity string, message string) error {

	// TODO: should not fetch this anew every time
	loggerUrl, err := GetServiceAddress("pz-logger")
	if err != nil {
		return err
	}

	loggerUrl += "/log"

	log.Print("111", loggerUrl)

	mssg := LogMessage{Service: service, Address: address, Severity: severity, Message: message, Time: time.Now().String()}
	data, err := json.Marshal(mssg)
	if err != nil {
		return err
	}

	resp, err := http.Post(loggerUrl, ContentTypeJSON, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return errors.New(resp.Status)
	}

	return nil
}

//---------------------------------------------------------------------------

// ReadFrom is a convenience function that returns the bytes taken from a Reader.
// The reader will be closed if necessary.
func ReadFrom(reader io.Reader) ([]byte, error) {
	switch reader.(type) {
	case io.Closer:
		defer reader.(io.Closer).Close()
	}

	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return data, err
}
