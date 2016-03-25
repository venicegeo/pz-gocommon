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

package elasticsearch

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/venicegeo/pz-gocommon"

	"gopkg.in/olivere/elastic.v2"
)

const elasticsearchURL = "https://search-venice-es-pjebjkdaueu2gukocyccj4r5m4.us-east-1.es.amazonaws.com"

// Client is the object that provides access to Elasticsearch. It implements
// the IService interface.
type Client struct {
	name        piazza.ServiceName
	address     string
	indexSuffix string
	lib         *elastic.Client
}

func init() {
	rand.Seed(int64(time.Now().Nanosecond()))
}

// NewClient returns an initialized Client object.
func NewClient(sys *piazza.System, testMode bool) (*Client, error) {

	lib, err := elastic.NewClient(
		elastic.SetURL(elasticsearchURL),
		elastic.SetSniff(false),
		elastic.SetMaxRetries(5),
		//elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)), // TODO
		//elastic.SetInfoLog(log.New(os.Stdout, "", log.LstdFlags)),
	)
	if err != nil {
		return nil, err
	}

	suffix := ""
	if testMode {
		n := rand.Intn(0xffffffff)
		suffix = fmt.Sprintf(".%x", n)
	}

	es := Client{lib: lib, name: piazza.PzElasticSearch, address: elasticsearchURL, indexSuffix: suffix}

	if sys != nil {
		sys.Services[piazza.PzElasticSearch] = es
	}

	return &es, nil
}

// GetName returns the name of the service.
func (es Client) GetName() piazza.ServiceName {
	return es.name
}

// GetAddress returns the IP address (and port) of this service.
func (es Client) GetAddress() string {
	return es.address
}

// Version returns the version of Elasticsearch as a string, e.g. "1.5.2".
func (es Client) Version() (string, error) {
	return es.lib.ElasticsearchVersion(elasticsearchURL)
}
