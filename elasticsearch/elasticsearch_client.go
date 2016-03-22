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

const elasticsearchUrl = "https://search-venice-es-pjebjkdaueu2gukocyccj4r5m4.us-east-1.es.amazonaws.com"

type ElasticsearchClient struct {
	name        piazza.ServiceName
	address     string
	indexPrefix string
	lib         *elastic.Client
}

func NewElasticsearchClient(sys *piazza.System, testMode bool) (*ElasticsearchClient, error) {

	lib, err := elastic.NewClient(
		elastic.SetURL(elasticsearchUrl),
		elastic.SetSniff(false),
		elastic.SetMaxRetries(5),
		//elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)), // TODO
		//elastic.SetInfoLog(log.New(os.Stdout, "", log.LstdFlags)),
	)
	if err != nil {
		return nil, err
	}

	rand.Seed(int64(time.Now().Nanosecond()))
	prefix := ""
	if testMode {
		n := rand.Intn(0xffff)
		prefix = fmt.Sprintf("%x", n)
	}

	es := ElasticsearchClient{lib: lib, name: piazza.PzElasticSearch, address: elasticsearchUrl, indexPrefix: prefix}

	if sys != nil {
		sys.Services[piazza.PzElasticSearch] = es
	}

	return &es, nil
}

func (es ElasticsearchClient) GetName() piazza.ServiceName {
	return es.name
}

func (es ElasticsearchClient) GetAddress() string {
	return es.address
}

func (es ElasticsearchClient) Version() (string, error) {
	return es.lib.ElasticsearchVersion(elasticsearchUrl)
}
