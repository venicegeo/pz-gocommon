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

	"github.com/venicegeo/pz-gocommon"
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

func (esi *MockIndex) PostData(typ string, id string, obj interface{}) (*IndexResponse, error) {
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

func (esi *MockIndex) FilterByMatchAll(typ string) (*SearchResult, error) {
	objs := esi.items[typ]
	resp := &SearchResult{
		totalHits: int64(len(objs)),
		hits:      make([]*SeachResultHit, len(objs)),
	}

	i := 0
	for id, obj := range objs {
		tmp := &SeachResultHit{
			Id:     id,
			Source: obj,
		}
		resp.hits[i] = tmp
		i++
	}

	return resp, nil
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
