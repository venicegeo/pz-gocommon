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

	"gopkg.in/olivere/elastic.v3"

	"github.com/venicegeo/pz-gocommon"
)

// Client is the object that provides access to Elasticsearch. It implements
// the IService interface.
type Client struct {
	indexSuffix          string
	lib                  *elastic.Client
	elasticsearchVersion string
}

func init() {
	rand.Seed(int64(time.Now().Nanosecond()))
}

// NewClient returns an initialized Client object.
func NewClient(sys *piazza.SystemConfig) (*Client, error) {

	url, err := sys.GetURL(piazza.PzElasticSearch)
	if err != nil {
		return nil, err
	}

	//log.Printf("NewClient: %s", url)

	lib, err := elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetSniff(false),
		elastic.SetMaxRetries(5),
		//elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)), // TODO
		//elastic.SetInfoLog(log.New(os.Stdout, "", log.LstdFlags)),
	)
	if err != nil {
		return nil, err
	}

	version, err := lib.ElasticsearchVersion(url)
	if err != nil {
		return nil, err
	}

	suffix := ""
	if sys.Testing() {
		n := rand.Intn(0xffffffff)
		suffix = fmt.Sprintf(".%x", n)
	}

	client := Client{lib: lib, indexSuffix: suffix, elasticsearchVersion: version}

	return &client, nil
}

// Version returns the version of Elasticsearch as a string, e.g. "1.5.2".
func (client *Client) GetVersion() string {
	return client.elasticsearchVersion
}
