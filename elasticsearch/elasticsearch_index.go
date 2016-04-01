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
	"errors"
	"fmt"
	"log"
	"strconv"

	"github.com/venicegeo/pz-gocommon"

	"gopkg.in/olivere/elastic.v3"
)

type Index struct {
	esClient *Client
	lib      *elastic.Client
	index    string
}

type IIndex interface {
	IndexName() string
	IndexExists() bool
	TypeExists(typ string) bool
	ItemExists(typ string, id string) bool
	Create() error
	Close() error
	Delete() error
	Flush() error
	PostData(typ string, id string, obj interface{}) (*elastic.IndexResponse, error)
	GetByID(typ string, id string) (*elastic.GetResult, error)
	DeleteByID(typ string, id string) (*elastic.DeleteResponse, error)
	FilterByMatchAll(typ string) (*elastic.SearchResult, error)
	FilterByTermQuery(typ string, name string, value interface{}) (*elastic.SearchResult, error)
	SearchByJSON(typ string, jsn string) (*elastic.SearchResult, error)
	SetMapping(typename string, jsn piazza.JsonString) error
	GetTypes() ([]string, error)
	GetMapping(typ string) (interface{}, error)
	AddPercolationQuery(id string, query piazza.JsonString) (*elastic.IndexResponse, error)
	DeletePercolationQuery(id string) (*elastic.DeleteResponse, error)
	AddPercolationDocument(typ string, doc interface{}) (*elastic.PercolateResponse, error)
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

func (esi *Index) PostData(typ string, id string, obj interface{}) (*elastic.IndexResponse, error) {
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

	indexResponse, err := esi.lib.Index().
		Index(esi.index).
		Type(typ).
		Id(id).
		BodyJson(obj).
		Do()

	return indexResponse, err
}

func (esi *Index) GetByID(typ string, id string) (*elastic.GetResult, error) {
	// TODO: the caller should enforce this instead (here and elsewhere)
	ok := esi.ItemExists(typ, id)
	if !ok {
		return nil, fmt.Errorf("Item %s in index %s and type %s does not exist", id, esi.index, typ)
	}

	getResult, err := esi.lib.Get().Index(esi.index).Type(typ).Id(id).Do()
	if err != nil {
		log.Printf("Index.GetByID failed: %s", err)
		return nil, err
	}

	return getResult, nil
}

func (esi *Index) DeleteByID(typ string, id string) (*elastic.DeleteResponse, error) {
	ok := esi.ItemExists(typ, id)
	if !ok {
		return nil, fmt.Errorf("Item %s in index %s and type %s does not exist", id, esi.index, typ)
	}

	deleteResponse, err := esi.lib.Delete().
		Index(esi.index).
		Type(typ).
		Id(id).
		Do()
	return deleteResponse, err
}

func (esi *Index) FilterByMatchAll(typ string) (*elastic.SearchResult, error) {
	//q := elastic.NewBoolFilter()
	//q.Must(elastic.NewTermFilter("a", 1))

	// TODO: the caller should enforce this instead
	ok := esi.TypeExists(typ)
	if !ok {
		return nil, fmt.Errorf("Type %s in index %s does not exist", typ, esi.index)
	}

	q := elastic.NewMatchAllQuery()
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

	termQuery := elastic.NewTermQuery(name, value)
	searchResult, err := esi.lib.Search().
		Index(esi.index).
		Type(typ).
		Query(termQuery).
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

func (esi *Index) AddPercolationQuery(id string, query piazza.JsonString) (*elastic.IndexResponse, error) {

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

func (esi *Index) DeletePercolationQuery(id string) (*elastic.DeleteResponse, error) {
	typ := ".percolator"
	ok := esi.ItemExists(typ, id)
	if !ok {
		return nil, fmt.Errorf("Item %s in index %s and type %s does not exist", id, esi.index, typ)
	}

	deleteResponse, err := esi.lib.Delete().
		Index(esi.index).
		Type(".percolator").
		Id(id).
		Do()
	if err != nil {
		return nil, err
	}

	return deleteResponse, nil
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

//=========================================================================================
//=========================================================================================

type MockIndex struct {
	esClient  *Client
	index     string
	items     map[string](map[string]*json.RawMessage)
	exists    bool
	open      bool
	ids       int
	percolate string
}

func NewMockIndex(es *Client, index string) *MockIndex {
	esi := MockIndex{
		esClient:  es,
		index:     index,
		items:     make(map[string](map[string]*json.RawMessage)),
		exists:    false,
		open:      false,
		percolate: ".percolate",
	}
	return &esi
}

func (esi *MockIndex) newid() string {
	id := strconv.Itoa(esi.ids)
	esi.ids++
	return id
}

func (esi *MockIndex) IndexName() string {
	return esi.index
}

func (esi *MockIndex) IndexExists() bool {
	return esi.exists
}

func (esi *MockIndex) TypeExists(typ string) bool {
	if !esi.IndexExists() {
		return false
	}
	_, ok := esi.items[typ]
	return ok
}

func (esi *MockIndex) ItemExists(typ string, id string) bool {
	if !esi.IndexExists() {
		return false
	}
	if !esi.TypeExists(typ) {
		return false
	}
	_, ok := esi.items[typ][id]
	return ok
}

// if index already exists, does nothing
func (esi *MockIndex) Create() error {
	esi.exists = true
	return nil
}

// if index doesn't already exist, does nothing
func (esi *MockIndex) Close() error {
	esi.open = false
	return nil
}

// if index doesn't already exist, does nothing
func (esi *MockIndex) Delete() error {
	esi.exists = false
	esi.open = false

	for k := range esi.items {
		for j := range esi.items[k] {
			delete(esi.items[k], j)
		}
		delete(esi.items, k)
	}

	return nil
}

func (esi *MockIndex) Flush() error {
	return nil
}

func (esi *MockIndex) PostData(typ string, id string, obj interface{}) (*elastic.IndexResponse, error) {
	if !esi.TypeExists(typ) {
		esi.items[typ] = make(map[string]*json.RawMessage)
	}

	byts, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}

	var raw json.RawMessage
	err = raw.UnmarshalJSON(byts)
	if err != nil {
		return nil, err
	}
	esi.items[typ][id] = &raw

	r := &elastic.IndexResponse{Created: true, Id: id, Index: esi.index, Type: typ}
	return r, nil
}

func (esi *MockIndex) GetByID(typ string, id string) (*elastic.GetResult, error) {
	for k, _ := range esi.items[typ] {
		if k == id {
			r := &elastic.GetResult{Id: id, Source: esi.items[typ][k]}
			return r, nil
		}
	}
	return nil, errors.New("GetById: not found: " + id)
}

func (esi *MockIndex) DeleteByID(typ string, id string) (*elastic.DeleteResponse, error) {
	for k, _ := range esi.items[typ] {
		if k == id {
			delete(esi.items[typ], k)
			r := &elastic.DeleteResponse{Found: true}
			return r, nil
		}
	}
	r := &elastic.DeleteResponse{Found: false}
	return r, nil
}

func (esi *MockIndex) FilterByMatchAll(typ string) (*elastic.SearchResult, error) {
	return nil, nil

	/*q := elastic.NewMatchAllQuery()
	searchResult, err := esi.lib.Search().
		Index(esi.index).
		Type(typ).
		Query(q).
		Do()
	return searchResult, err*/
}

func (esi *MockIndex) FilterByTermQuery(typ string, name string, value interface{}) (*elastic.SearchResult, error) {
	return nil, nil

	/*termQuery := elastic.NewTermQuery(name, value)
	searchResult, err := esi.lib.Search().
		Index(esi.index).
		Type(typ).
		Query(termQuery).
		Do()
	return searchResult, err*/
}

func (esi *MockIndex) SearchByJSON(typ string, jsn string) (*elastic.SearchResult, error) {
	return nil, nil

	/*var obj interface{}
	err := json.Unmarshal([]byte(jsn), &obj)
	if err != nil {
		return nil, err
	}

	searchResult, err := esi.lib.Search().
		Index(esi.index).
		Type(typ).
		Source(obj).Do()

	return searchResult, err*/
}

func (esi *MockIndex) SetMapping(typename string, jsn piazza.JsonString) error {
	return nil
}

func (esi *MockIndex) GetTypes() ([]string, error) {
	var s []string

	for k, _ := range esi.items {
		s = append(s, k)
	}

	return s, nil
}

func (esi *MockIndex) GetMapping(typ string) (interface{}, error) {
	return nil, nil
}

func (esi *MockIndex) AddPercolationQuery(id string, query piazza.JsonString) (*elastic.IndexResponse, error) {
	return esi.PostData(esi.percolate, id, query)
}

func (esi *MockIndex) DeletePercolationQuery(id string) (*elastic.DeleteResponse, error) {
	return esi.DeleteByID(esi.percolate, id)
}

func (esi *MockIndex) AddPercolationDocument(typ string, doc interface{}) (*elastic.PercolateResponse, error) {
	for _, _ = range esi.items[esi.percolate] {
		r := &elastic.PercolateResponse{}
		return r, nil
	}
	return nil, nil
}
