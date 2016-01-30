package piazza

import (
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
