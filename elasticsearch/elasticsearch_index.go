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
	"encoding/json"
	"fmt"

	"github.com/venicegeo/pz-gocommon"

	"gopkg.in/olivere/elastic.v2"
)

type ElasticsearchIndex struct {
	esClient *ElasticsearchClient
	lib      *elastic.Client
	index    string
}

func NewElasticsearchIndex(es *ElasticsearchClient, index string) *ElasticsearchIndex {
	esi := &ElasticsearchIndex{
		esClient: es,
		lib:      es.lib,
		index:    es.indexPrefix + index,
	}
	return esi
}

func (esi *ElasticsearchIndex) IndexName() string {
	return esi.index
}

func (esi *ElasticsearchIndex) Exists() (bool, error) {
	return esi.lib.IndexExists(esi.index).Do()
}

// if index already exists, does nothing
func (esi *ElasticsearchIndex) Create() error {

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
func (esi *ElasticsearchIndex) Close() error {

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
func (esi *ElasticsearchIndex) Delete() error {

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
func (esi *ElasticsearchIndex) Flush() error {
	_, err := esi.lib.Flush().Index(esi.index).Do()
	if err != nil {
		return err
	}
	return nil
}

func (esi *ElasticsearchIndex) PostData(mapping string, id string, obj interface{}) (*elastic.IndexResult, error) {
	indexResult, err := esi.lib.Index().
		Index(esi.index).
		Type(mapping).
		Id(id).
		BodyJson(obj).
		Do()
	return indexResult, err
}

func (esi *ElasticsearchIndex) GetById(mapping string, id string) (*elastic.GetResult, error) {
	getResult, err := esi.lib.Get().Index(esi.index).Type(mapping).Id(id).Do()
	return getResult, err
}

func (esi *ElasticsearchIndex) DeleteById(mapping string, id string) (*elastic.DeleteResult, error) {
	deleteResult, err := esi.lib.Delete().
		Index(esi.index).
		Type(mapping).
		Id(id).
		Do()
	return deleteResult, err
}

func (esi *ElasticsearchIndex) SearchByMatchAll() (*elastic.SearchResult, error) {
	searchResult, err := esi.lib.Search().
		Index(esi.index).
		Query(elastic.NewMatchAllQuery()).
		//Sort("id", true).
		Do()
	return searchResult, err
}

func (esi *ElasticsearchIndex) SearchByMatchAllWithMapping(mapping string) (*elastic.SearchResult, error) {
	searchResult, err := esi.lib.Search().
		Index(esi.index).
		Type(mapping).
		Query(elastic.NewMatchAllQuery()).
		//Sort("id", true).
		Do()
	return searchResult, err
}

func (esi *ElasticsearchIndex) SearchByTermQuery(name string, value interface{}) (*elastic.SearchResult, error) {
	termQuery := elastic.NewTermQuery(name, value)
	searchResult, err := esi.lib.Search().
		Index(esi.index).
		Query(&termQuery).
		//Sort("id", true).
		Do()
	return searchResult, err
}

func (esi *ElasticsearchIndex) SearchByJson(jsn string) (*elastic.SearchResult, error) {

	var obj interface{}
	err := json.Unmarshal([]byte(jsn), &obj)
	if err != nil {
		return nil, err
	}

	searchResult, err := esi.lib.Search().Index(esi.index).Source(obj).Do()

	return searchResult, err
}

func (esi *ElasticsearchIndex) SetMapping(typename string, jsn piazza.JsonString) error {

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

func (esi *ElasticsearchIndex) GetMapping(typename string) (interface{}, error) {

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

func (esi *ElasticsearchIndex) AddPercolationQuery(id string, query piazza.JsonString) (*elastic.IndexResult, error) {

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

func (esi *ElasticsearchIndex) DeletePercolationQuery(id string) (*elastic.DeleteResult, error) {

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

func (esi *ElasticsearchIndex) AddPercolationDocument(typename string, doc interface{}) (*elastic.PercolateResponse, error) {
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
