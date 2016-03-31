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
	"log"

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

func (esi *Index) IndexExists() bool {
	ok, err := esi.lib.IndexExists(esi.index).Do()
	if err != nil {
		return false
	}
	return ok
}

func (esi *Index) TypeExists(typ string) bool {
	ok := esi.IndexExists()
	if !ok {
		return false
	}

	ok, err := esi.lib.TypeExists().Index(esi.index).Type(typ).Do()
	if err != nil {
		return false
	}
	return ok
}

func (esi *Index) ItemExists(typ string, id string) bool {
	ok := esi.TypeExists(typ)
	if !ok {
		return false
	}

	ok, err := esi.lib.Exists().Index(esi.index).Type(typ).Id(id).Do()
	if err != nil {
		return false
	}
	return ok
}

// if index already exists, does nothing
func (esi *Index) Create() error {

	ok := esi.IndexExists()
	if ok {
		return fmt.Errorf("Index %s already exists", esi.index)
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

	// TODO: the caller should enforce this instead
	ok := esi.IndexExists()
	if !ok {
		return fmt.Errorf("Index %s does not already exist", esi.index)
	}

	_, err := esi.lib.CloseIndex(esi.index).Do()
	if err != nil {
		return err
	}

	return nil
}

// if index doesn't already exist, does nothing
func (esi *Index) Delete() error {

	ok := esi.IndexExists()
	if !ok {
		return fmt.Errorf("Index %s does not exist", esi.index)
	}

	deleteIndex, err := esi.lib.DeleteIndex(esi.index).Do()
	if err != nil {
		return err
	}

	// TODO: is this check needed? should it also be on Create(), etc?
	if !deleteIndex.Acknowledged {
		return fmt.Errorf("Elasticsearch: delete index not acknowledged!")
	}
	return nil
}

// TODO: how often should we do this?
func (esi *Index) Flush() error {
	// TODO: the caller should enforce this instead
	ok := esi.IndexExists()
	if !ok {
		return fmt.Errorf("Index %s does not exist", esi.index)
	}

	_, err := esi.lib.Flush().Index(esi.index).Do()
	if err != nil {
		return err
	}

	return nil
}

func (esi *Index) PostData(typ string, id string, obj interface{}) (*elastic.IndexResult, error) {
	/*ok := esi.IndexExists()
	if !ok {
		log.Printf("Index %s does not exist", esi.index)
		return nil, errors.New(fmt.Sprintf("Index %s does not exist", esi.index))
	}
	ok = esi.TypeExists(typ)
	if !ok {
		log.Printf("Index %s or type %s does not exist", esi.index, typ)
		return nil, errors.New(fmt.Sprintf("Index %s or type %s does not exist", esi.index, typ))
	}*/

	indexResult, err := esi.lib.Index().
		Index(esi.index).
		Type(typ).
		Id(id).
		BodyJson(obj).
		Do()

	return indexResult, err
}

func (esi *Index) GetByID(typ string, id string) (*elastic.GetResult, error) {
	// TODO: the caller should enforce this instead (here and elsewhere)
	ok := esi.ItemExists(typ, id)
	if !ok {
		return nil, fmt.Errorf("Item %s in index %s and type %s does not exist", id, esi.index, typ)
	}

	svc := esi.lib.Get().Index(esi.index).Type(typ).Id(id)
	log.Printf("Index.GetByID: %s", svc.String())
	getResult, err := svc.Do()
	if err != nil {
		log.Printf("Index.GetByID failed: %s", err)
		return nil, err
	}

	return getResult, nil
}

func (esi *Index) DeleteByID(typ string, id string) (*elastic.DeleteResult, error) {
	ok := esi.ItemExists(typ, id)
	if !ok {
		return nil, fmt.Errorf("Item %s in index %s and type %s does not exist", id, esi.index, typ)
	}

	deleteResult, err := esi.lib.Delete().
		Index(esi.index).
		Type(typ).
		Id(id).
		Do()
	return deleteResult, err
}

func (esi *Index) FilterByMatchAll(typ string) (*elastic.SearchResult, error) {
	//q := elastic.NewBoolFilter()
	//q.Must(elastic.NewTermFilter("a", 1))

	// TODO: the caller should enforce this instead
	ok := esi.TypeExists(typ)
	if !ok {
		return nil, fmt.Errorf("Type %s in index %s does not exist", typ, esi.index)
	}

	q := elastic.NewMatchAllFilter()
	searchResult, err := esi.lib.Search().
		Index(esi.index).
		Type(typ).
		Query(q).
		//Sort("id", true).
		Do()
	return searchResult, err
}

func (esi *Index) FilterByTermQuery(typ string, name string, value interface{}) (*elastic.SearchResult, error) {

	// TODO: the caller should enforce this instead
	ok := esi.TypeExists(typ)
	if !ok {
		return nil, fmt.Errorf("Type %s in index %s does not exist", typ, esi.index)
	}

	termQuery := elastic.NewTermFilter(name, value)
	searchResult, err := esi.lib.Search().
		Index(esi.index).
		Type(typ).
		Query(&termQuery).
		//Sort("id", true).
		Do()
	return searchResult, err
}

func (esi *Index) SearchByJSON(typ string, jsn string) (*elastic.SearchResult, error) {

	// TODO: the caller should enforce this instead
	ok := esi.TypeExists(typ)
	if !ok {
		return nil, fmt.Errorf("Type %s in index %s does not exist", typ, esi.index)
	}

	var obj interface{}
	err := json.Unmarshal([]byte(jsn), &obj)
	if err != nil {
		return nil, err
	}

	searchResult, err := esi.lib.Search().
		Index(esi.index).
		Type(typ).
		Source(obj).Do()

	return searchResult, err
}

func (esi *Index) SetMapping(typename string, jsn piazza.JsonString) error {

	ok := esi.IndexExists()
	if !ok {
		return fmt.Errorf("Index %s does not exist", esi.index)
	}

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

	return nil
}

func (esi *Index) GetTypes() ([]string, error) {

	ok := esi.IndexExists()
	if !ok {
		return nil, fmt.Errorf("Index %s does not exist", esi.index)
	}

	getresp, err := esi.lib.IndexGet().Feature("_mappings").Index(esi.index).Do()
	if err != nil {
		return nil, err
	}

	typs := (*getresp[esi.index]).Mappings
	result := make([]string, len(typs))

	i := 0
	for k := range typs {
		result[i] = k
		i++
	}

	return result, nil
}

func (esi *Index) GetMapping(typ string) (interface{}, error) {

	ok := esi.TypeExists(typ)
	if !ok {
		return nil, fmt.Errorf("Type %s in index %s does not exist", typ, esi.index)
	}

	getresp, err := esi.lib.GetMapping().Index(esi.index).Type(typ).Do()
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

	ok := esi.IndexExists()
	if !ok {
		return nil, fmt.Errorf("Index %s does not exist", esi.index)
	}

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
	typ := ".percolator"
	ok := esi.ItemExists(typ, id)
	if !ok {
		return nil, fmt.Errorf("Item %s in index %s and type %s does not exist", id, esi.index, typ)
	}

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

func (esi *Index) AddPercolationDocument(typ string, doc interface{}) (*elastic.PercolateResponse, error) {
	ok := esi.TypeExists(typ)
	if !ok {
		return nil, fmt.Errorf("Type %s in index %s does not exist", typ, esi.index)
	}

	percolateResponse, err := esi.lib.
		Percolate().
		Index(esi.index).
		Type(typ).
		Doc(doc).
		//Pretty(true).
		Do()
	if err != nil {
		return nil, err
	}

	return percolateResponse, nil
}
