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

package piazza

import (
	"fmt"
	"gopkg.in/olivere/elastic.v2"
	"log"
	"math/rand"
	"time"
)

// TODO (default is "http://127.0.0.1:9200")
const elasticsearchUrl = "https://search-venice-es-pjebjkdaueu2gukocyccj4r5m4.us-east-1.es.amazonaws.com"

type ElasticSearchService struct {
	name    ServiceName
	address string

	indexPrefix string
	Client      *elastic.Client
}

func newElasticSearchService(testMode bool) (*ElasticSearchService, error) {
	client, err := elastic.NewClient(
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
		log.Printf("Elsasticsearch index prefix: %s", prefix)
	}

	es := ElasticSearchService{Client: client, name: PzElasticSearch, address: elasticsearchUrl, indexPrefix: prefix}
	return &es, nil
}

func (es *ElasticSearchService) GetName() ServiceName {
	return es.name
}

func (es *ElasticSearchService) GetAddress() string {
	return es.address
}

func (es *ElasticSearchService) Version() (string, error) {
	return es.Client.ElasticsearchVersion(elasticsearchUrl)
}

func (es *ElasticSearchService) prefixed(index string) string {
	return fmt.Sprintf("%s.%s", es.indexPrefix, index)
}

func (es *ElasticSearchService) IndexExists(index string) (bool, error) {
	return es.Client.IndexExists(es.prefixed(index)).Do()
}

// if index already exists, does nothing
func (es *ElasticSearchService) CreateIndex(index string) error {

	ok, err := es.IndexExists(es.prefixed(index))
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	createIndex, err := es.Client.CreateIndex(es.prefixed(index)).Do()
	if err != nil {
		return err
	}

	if !createIndex.Acknowledged {
		return fmt.Errorf("Elasticsearch: create index not acknowledged!")
	}

	return nil
}

// if index doesn't already exist, does nothing
func (es *ElasticSearchService) DeleteIndex(index string) error {

	exists, err := es.Client.IndexExists(es.prefixed(index)).Do()
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	deleteIndex, err := es.Client.DeleteIndex(es.prefixed(index)).Do()
	if err != nil {
		return err
	}
	if !deleteIndex.Acknowledged {
		return fmt.Errorf("Elasticsearch: delete index not acknowledged!")
	}
	return nil
}

// TODO: how often should we do this?
func (es *ElasticSearchService) FlushIndex(index string) error {
	_, err := es.Client.Flush().Index(es.prefixed(index)).Do()
	if err != nil {
		return err
	}
	return nil
}

func (es *ElasticSearchService) PostData(index string, mapping string, id string, json interface{}) (*elastic.IndexResult, error) {
	indexResult, err := es.Client.Index().
		Index(es.prefixed(index)).
		Type(mapping).
		Id(id).
		BodyJson(json).
		Do()
	return indexResult, err
}

func (es *ElasticSearchService) GetById(index string, id string) (*elastic.GetResult, error) {
	getResult, err := es.Client.Get().Index(es.prefixed(index)).Id(id).Do()
	return getResult, err
}

func (es *ElasticSearchService) DeleteById(index string, mapping string, id string) (*elastic.DeleteResult, error) {
	deleteResult, err := es.Client.Delete().
		Index(es.prefixed(index)).
		Type(mapping).
		Id(id).
		Do()
	return deleteResult, err
}

func (es *ElasticSearchService) SearchByMatchAll(index string) (*elastic.SearchResult, error) {
	searchResult, err := es.Client.Search().
		Index(es.prefixed(index)).
		Query(elastic.NewMatchAllQuery()).
		//Sort("id", true).
		Do()
	return searchResult, err
}

func (es *ElasticSearchService) SearchByTermQuery(index string, name string, value interface{}) (*elastic.SearchResult, error) {
	termQuery := elastic.NewTermQuery(name, value)
	searchResult, err := es.Client.Search().
		Index(es.prefixed(index)).
		Query(&termQuery).
		//Sort("id", true).
		Do()
	return searchResult, err
}
