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
	"sort"

	"github.com/venicegeo/pz-gocommon/gocommon"
)

type MockIndex struct {
	index     string
	items     map[string](map[string]*json.RawMessage)
	exists    bool
	open      bool
	percolate string
}

func NewMockIndex(index string) *MockIndex {
	var _ IIndex = new(MockIndex)

	esi := MockIndex{
		index:     index,
		items:     make(map[string](map[string]*json.RawMessage)),
		exists:    false,
		open:      false,
		percolate: ".percolate",
	}
	return &esi
}

func (client *MockIndex) GetVersion() string {
	return "2.2.0"
}

func (esi *MockIndex) IndexName() string {
	return esi.index
}

func (esi *MockIndex) IndexExists() (bool, error) {
	return esi.exists, nil
}

func (esi *MockIndex) TypeExists(typ string) (bool, error) {

	ok, err := esi.IndexExists()
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	_, ok = esi.items[typ]
	return ok, nil
}

func (esi *MockIndex) ItemExists(typ string, id string) (bool, error) {
	ok, err := esi.TypeExists(typ)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	_, ok = esi.items[typ][id]
	return ok, nil
}

// if index already exists, does nothing
func (esi *MockIndex) Create(settings string) error {
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

func (esi *MockIndex) PostData(typ string, id string, obj interface{}) (*IndexResponse, error) {
	ok, err := esi.IndexExists()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("Index does not exist")
	}
	ok, err = esi.TypeExists(typ)
	if err != nil {
		return nil, err
	}
	if !ok {
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

	r := &IndexResponse{Created: true, Id: id, Index: esi.index, Type: typ}
	return r, nil
}

func (esi *MockIndex) GetByID(typ string, id string) (*GetResult, error) {
	for k, _ := range esi.items[typ] {
		if k == id {
			r := &GetResult{Id: id, Source: esi.items[typ][k], Found: true}
			return r, nil
		}
	}
	return nil, errors.New("GetById: not found: " + id)
}

func (esi *MockIndex) DeleteByID(typ string, id string) (*DeleteResponse, error) {
	for k, _ := range esi.items[typ] {
		if k == id {
			delete(esi.items[typ], k)
			r := &DeleteResponse{Found: true}
			return r, nil
		}
	}
	r := &DeleteResponse{Found: false}
	return r, nil
}

type srhByID []*SearchResultHit

func (a srhByID) Len() int {
	return len(a)
}
func (a srhByID) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a srhByID) Less(i, j int) bool {
	return (*a[i]).Id < (*a[j]).Id
}
func srhSortMatches(matches []*SearchResultHit) []*SearchResultHit {
	sort.Sort(srhByID(matches))
	return matches
}

func (esi *MockIndex) FilterByMatchAll(typ string, realFormat *piazza.JsonPagination) (*SearchResult, error) {

	format := NewQueryFormat(realFormat)

	objs := make(map[string]*json.RawMessage)

	if typ == "" {
		for t := range esi.items {
			for id, i := range esi.items[t] {
				objs[id] = i
			}
		}
	} else {
		for id, i := range esi.items[typ] {
			objs[id] = i
		}
	}

	resp := &SearchResult{
		totalHits: int64(len(objs)),
		hits:      make([]*SearchResultHit, len(objs)),
	}

	i := 0
	for id, obj := range objs {
		tmp := &SearchResultHit{
			Id:     id,
			Source: obj,
		}
		resp.hits[i] = tmp
		i++
	}

	// TODO; sort key not supported
	// TODO: sort order not supported

	from := format.From
	size := format.Size

	resp.hits = srhSortMatches(resp.hits)

	if from >= len(resp.hits) {
		resp.hits = make([]*SearchResultHit, 0)
	}
	if from+size >= len(resp.hits) {
		size = len(resp.hits)
	}
	resp.hits = resp.hits[from : from+size]

	return resp, nil
}

func (esi *MockIndex) GetAllElements(typ string) (*SearchResult, error) {
	return nil, errors.New("GetAllElements not supported under mocking")
}

func (esi *MockIndex) FilterByMatchQuery(typ string, name string, value interface{}) (*SearchResult, error) {

	/*termQuery := NewTermQuery(name, value)
	searchResult, err := esi.lib.Search().
		Index(esi.index).
		Type(typ).
		Query(termQuery).
		Do()
	return searchResult, err*/

	////resp := &SearchResult{}
	////return resp, nil
	return nil, errors.New("FilterByMatchQuery not supported under mocking")
}

func (esi *MockIndex) FilterByTermQuery(typ string, name string, value interface{}) (*SearchResult, error) {

	/*termQuery := NewTermQuery(name, value)
	searchResult, err := esi.lib.Search().
		Index(esi.index).
		Type(typ).
		Query(termQuery).
		Do()
	return searchResult, err*/

	////resp := &SearchResult{}
	////return resp, nil
	return nil, errors.New("FilterByTermQuery not supported under mocking")
}

func (esi *MockIndex) SearchByJSON(typ string, jsn string) (*SearchResult, error) {

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

	////resp := &SearchResult{}
	////return resp, nil

	return nil, errors.New("SearchByJSON not supported under mocking")
}

func (esi *MockIndex) SetMapping(typename string, jsn piazza.JsonString) error {
	return nil
	//return errors.New("SetMapping not supported under mocking")
}

func (esi *MockIndex) GetTypes() ([]string, error) {
	var s []string

	for k, _ := range esi.items {
		s = append(s, k)
	}

	return s, nil
}

func (esi *MockIndex) GetMapping(typ string) (interface{}, error) {
	return nil, errors.New("GetMapping not supported under mocking")
}

func (esi *MockIndex) AddPercolationQuery(id string, query piazza.JsonString) (*IndexResponse, error) {
	return esi.PostData(esi.percolate, id, query)
}

func (esi *MockIndex) DeletePercolationQuery(id string) (*DeleteResponse, error) {
	return esi.DeleteByID(esi.percolate, id)
}

func (esi *MockIndex) AddPercolationDocument(typ string, doc interface{}) (*PercolateResponse, error) {
	for _, _ = range esi.items[esi.percolate] {
		r := &PercolateResponse{}
		return r, nil
	}

	resp := &PercolateResponse{}
	return resp, nil
}
