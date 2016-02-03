package piazza

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

//---------------------------------------------------------------------------

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

func HandlePostAdminShutdown(pzService *PzService, c *gin.Context) {
	type shutdownRequest struct {
		Reason string `json:"reason"`
	}
	var reason shutdownRequest

	err := c.BindJSON(&reason)
	if err != nil {
		c.String(http.StatusBadRequest, fmt.Sprintf("%v", err))
		return
	}
	if reason.Reason == "" {
		c.String(http.StatusBadRequest, "no reason supplied")
		return
	}
	pzService.Log(SeverityFatal, "Shutdown requested: "+reason.Reason)

	// TODO: need a graceful shutdown method
	// need to ACK to the HTTP caller, then call exit
	os.Exit(0)
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
