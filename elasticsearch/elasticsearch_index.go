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

type Index struct {
	esClient *Client
	lib      *elastic.Client
	index    string
}

func NewIndex(es *Client, index string) *Index {
	esi := &Index{
		esClient: es,
		lib:      es.lib,
		index:    index + es.indexSuffix,
	}
	return esi
}

func (esi *Index) IndexName() string {
	return esi.index
}

func (esi *Index) Exists() (bool, error) {
	return esi.lib.IndexExists(esi.index).Do()
}

// if index already exists, does nothing
func (esi *Index) Create() error {

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
func (esi *Index) Close() error {

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
func (esi *Index) Delete() error {

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
func (esi *Index) Flush() error {
	_, err := esi.lib.Flush().Index(esi.index).Do()
	if err != nil {
		return err
	}
	return nil
}

func (esi *Index) PostData(mapping string, id string, obj interface{}) (*elastic.IndexResult, error) {
	indexResult, err := esi.lib.Index().
		Index(esi.index).
		Type(mapping).
		Id(id).
		BodyJson(obj).
		Do()
	return indexResult, err
}

func (esi *Index) GetByID(mapping string, id string) (*elastic.GetResult, error) {
	getResult, err := esi.lib.Get().Index(esi.index).Type(mapping).Id(id).Do()
	return getResult, err
}

func (esi *Index) DeleteByID(mapping string, id string) (*elastic.DeleteResult, error) {
	deleteResult, err := esi.lib.Delete().
		Index(esi.index).
		Type(mapping).
		Id(id).
		Do()
	return deleteResult, err
}

func (esi *Index) FilterByMatchAll(mapping string) (*elastic.SearchResult, error) {
	//q := elastic.NewBoolFilter()
	//q.Must(elastic.NewTermFilter("a", 1))
	q := elastic.NewMatchAllFilter()
	searchResult, err := esi.lib.Search().
		Index(esi.index).
		Type(mapping).
		Query(q).
		//Sort("id", true).
		Do()
	return searchResult, err
}

func (esi *Index) FilterByTermQuery(mapping string, name string, value interface{}) (*elastic.SearchResult, error) {
	termQuery := elastic.NewTermFilter(name, value)
	searchResult, err := esi.lib.Search().
		Index(esi.index).
		Type(mapping).
		Query(&termQuery).
		//Sort("id", true).
		Do()
	return searchResult, err
}

func (esi *Index) SearchByJSON(mapping string, jsn string) (*elastic.SearchResult, error) {

	var obj interface{}
	err := json.Unmarshal([]byte(jsn), &obj)
	if err != nil {
		return nil, err
	}

	searchResult, err := esi.lib.Search().
		Index(esi.index).
		Type(mapping).
		Source(obj).Do()

	return searchResult, err
}

func (esi *Index) SetMapping(typename string, jsn piazza.JsonString) error {

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

func (esi *Index) GetIndexTypes() ([]string, error) {

	getresp, err := esi.lib.IndexGet().Feature("_mappings").Index(esi.index).Do()
	if err != nil {
		return nil, err
	}

	mappings := (*getresp[esi.index]).Mappings
	result := make([]string, len(mappings))

	i := 0
	for k := range mappings {
		result[i] = k
		i++
	}

	return result, nil
}

func (esi *Index) GetMapping(typename string) (interface{}, error) {

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

func (esi *Index) AddPercolationQuery(id string, query piazza.JsonString) (*elastic.IndexResult, error) {

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

func (esi *Index) DeletePercolationQuery(id string) (*elastic.DeleteResult, error) {

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

func (esi *Index) AddPercolationDocument(typename string, doc interface{}) (*elastic.PercolateResponse, error) {
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
