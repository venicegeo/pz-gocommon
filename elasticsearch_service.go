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
	"encoding/json"
	"fmt"
	"gopkg.in/olivere/elastic.v2"
	"math/rand"
	"time"
)

// TODO (default is "http://127.0.0.1:9200")
const elasticsearchUrl = "https://search-venice-es-pjebjkdaueu2gukocyccj4r5m4.us-east-1.es.amazonaws.com"

type EsClient struct {
	name        ServiceName
	address     string
	indexPrefix string
	lib         *elastic.Client
}

func newEsClient(testMode bool) (*EsClient, error) {
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

	es := EsClient{lib: lib, name: PzElasticSearch, address: elasticsearchUrl, indexPrefix: prefix}
	return &es, nil
}

func (es *EsClient) GetName() ServiceName {
	return es.name
}

func (es *EsClient) GetAddress() string {
	return es.address
}

func (es *EsClient) Version() (string, error) {
	return es.lib.ElasticsearchVersion(elasticsearchUrl)
}

///////////////////////////////////////////////////

type EsIndexClient struct {
	esClient *EsClient
	lib      *elastic.Client
	index    string
}

func NewEsIndexClient(es *EsClient, index string) *EsIndexClient {
	esi := &EsIndexClient{
		esClient: es,
		lib:      es.lib,
		index:    es.indexPrefix + index,
	}
	return esi
}

func (esi *EsIndexClient) IndexName() string {
	return esi.index
}

func (esi *EsIndexClient) Exists() (bool, error) {
	return esi.lib.IndexExists(esi.index).Do()
}

// if index already exists, does nothing
func (esi *EsIndexClient) Create() error {

	ok, err := esi.Exists()
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	createIndex, err := esi.lib.CreateIndex(esi.index).Do()
	if err != nil {
		return err
	}

	if !createIndex.Acknowledged {
		return fmt.Errorf("Elasticsearch: create index not acknowledged!")
	}

	return nil
}

// if index doesn't already exist, does nothing
func (esi *EsIndexClient) Close() error {

	closeIndexResponse, err := esi.lib.CloseIndex(esi.index).Do()
	if err != nil {
		return err
	}
	if !closeIndexResponse.Acknowledged {
		return fmt.Errorf("Elasticsearch: close index not acknowledged!")
	}
	return nil
}

// if index doesn't already exist, does nothing
func (esi *EsIndexClient) Delete() error {

	exists, err := esi.lib.IndexExists(esi.index).Do()
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	deleteIndex, err := esi.lib.DeleteIndex(esi.index).Do()
	if err != nil {
		return err
	}
	if !deleteIndex.Acknowledged {
		return fmt.Errorf("Elasticsearch: delete index not acknowledged!")
	}
	return nil
}

// TODO: how often should we do this?
func (esi *EsIndexClient) Flush() error {
	_, err := esi.lib.Flush().Index(esi.index).Do()
	if err != nil {
		return err
	}
	return nil
}

func (esi *EsIndexClient) PostData(mapping string, id string, obj interface{}) (*elastic.IndexResult, error) {
	indexResult, err := esi.lib.Index().
		Index(esi.index).
		Type(mapping).
		Id(id).
		BodyJson(obj).
		Do()
	return indexResult, err
}

func (esi *EsIndexClient) GetById(mapping string, id string) (*elastic.GetResult, error) {
	getResult, err := esi.lib.Get().Index(esi.index).Type(mapping).Id(id).Do()
	return getResult, err
}

func (esi *EsIndexClient) DeleteById(mapping string, id string) (*elastic.DeleteResult, error) {
	deleteResult, err := esi.lib.Delete().
		Index(esi.index).
		Type(mapping).
		Id(id).
		Do()
	return deleteResult, err
}

func (esi *EsIndexClient) SearchByMatchAll() (*elastic.SearchResult, error) {
	searchResult, err := esi.lib.Search().
		Index(esi.index).
		Query(elastic.NewMatchAllQuery()).
		//Sort("id", true).
		Do()
	return searchResult, err
}

func (esi *EsIndexClient) SearchByTermQuery(name string, value interface{}) (*elastic.SearchResult, error) {
	termQuery := elastic.NewTermQuery(name, value)
	searchResult, err := esi.lib.Search().
		Index(esi.index).
		Query(&termQuery).
		//Sort("id", true).
		Do()
	return searchResult, err
}

func (esi *EsIndexClient) SearchByJson(jsn string) (*elastic.SearchResult, error) {

	var obj interface{}
	err := json.Unmarshal([]byte(jsn), &obj)
	if err != nil {
		return nil, err
	}

	searchResult, err := esi.lib.Search().Index(esi.index).Source(obj).Do()

	return searchResult, err
}

func (esi *EsIndexClient) SetMapping(typename string, jsn JsonString) error {

	putresp, err := esi.lib.PutMapping().Index(esi.index).Type(typename).BodyString(string(jsn)).Do()
	if err != nil {
		return fmt.Errorf("expected put mapping to succeed; got: %v", err)
	}
	if putresp == nil {
		return fmt.Errorf("expected put mapping response; got: %v", putresp)
	}
	if !putresp.Acknowledged {
		return fmt.Errorf("expected put mapping ack; got: %v", putresp.Acknowledged)
	}

	esi.GetMapping(typename)

	return nil
}

func (esi *EsIndexClient) GetMapping(typename string) (interface{}, error) {

	getresp, err := esi.lib.GetMapping().Index(esi.index).Type(typename).Do()
	if err != nil {
		return nil, fmt.Errorf("expected get mapping to succeed; got: %v", err)
	}
	if getresp == nil {
		return nil, fmt.Errorf("expected get mapping response; got: %v", getresp)
	}
	props, ok := getresp[esi.index]
	if !ok {
		return nil, fmt.Errorf("expected JSON root to be of type map[string]interface{}; got: %s -- %#v", esi.index, getresp)
	}

	props2 := props.(map[string]interface{})

	return props2["mappings"], nil
}

func (esi *EsIndexClient) AddPercolationQuery(id string, query JsonString) (*elastic.IndexResult, error) {

	indexResponse, err := esi.lib.
		Index().
		Index(esi.index).
		Type(".percolator").
		Id(id).
		BodyString(string(query)).
		Do()
	if err != nil {
		return nil, err
	}

	return indexResponse, nil
}

func (esi *EsIndexClient) DeletePercolationQuery(id string) (*elastic.DeleteResult, error) {

	deleteResult, err := esi.lib.Delete().
	Index(esi.index).
	Type(".percolator").
	Id(id).
	Do()
	if err != nil {
		return nil, err
	}

	return deleteResult, nil
}

func (esi *EsIndexClient) AddPercolationDocument(typename string, doc interface{}) (*elastic.PercolateResponse, error) {
	percolateResponse, err := esi.lib.
		Percolate().
		Index(esi.index).Type(typename).
		Doc(doc).
		Pretty(true).
		Do()
	if err != nil {
		return nil, err
	}

	return percolateResponse, nil
}
