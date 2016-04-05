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

	"gopkg.in/olivere/elastic.v3"
)

type SeachResultHit struct {
	Id     string
	Source *json.RawMessage
}

type SearchResult struct {
	totalHits int64
	hits      []*SeachResultHit
}

func NewSearchResult(searchResult *elastic.SearchResult) *SearchResult {
	resp := &SearchResult{
		totalHits: searchResult.TotalHits(),
		hits:      make([]*SeachResultHit, searchResult.TotalHits()),
	}

	for i, hit := range searchResult.Hits.Hits {
		tmp := &SeachResultHit{
			Id:     hit.Id,
			Source: hit.Source,
		}
		resp.hits[i] = tmp
	}

	return resp
}

func (r *SearchResult) TotalHits() int64 {
	return r.totalHits
}

func (r *SearchResult) GetHits() *[]*SeachResultHit {
	return &r.hits
}

func (r *SearchResult) GetHit(i int) *SeachResultHit {
	arr := r.GetHits()
	return (*arr)[i]
}

type IndexResponse struct {
	Created bool
	Id      string
	Index   string
	Type    string
	Version int
}

func NewIndexResponse(indexResponse *elastic.IndexResponse) *IndexResponse {
	resp := &IndexResponse{
		Created: indexResponse.Created,
		Id:      indexResponse.Id,
		Index:   indexResponse.Index,
		Type:    indexResponse.Type,
		Version: indexResponse.Version,
	}
	return resp
}

type PercolateResponseMatch struct {
	Id    string
	Index string
}

type PercolateResponse struct {
	Total   int64
	Matches []*PercolateResponseMatch
}

func NewPercolateResponse(percolateResponse *elastic.PercolateResponse) *PercolateResponse {
	resp := &PercolateResponse{
		Total:   percolateResponse.Total,
		Matches: make([]*PercolateResponseMatch, len(percolateResponse.Matches)),
	}

	for i, v := range percolateResponse.Matches {
		m := &PercolateResponseMatch{
			Id:    v.Id,
			Index: v.Index,
		}
		resp.Matches[i] = m
	}

	return resp
}

type DeleteResponse struct {
	Found bool
	Id    string
}

func NewDeleteResponse(deleteResponse *elastic.DeleteResponse) *DeleteResponse {
	resp := &DeleteResponse{
		Found: deleteResponse.Found,
		Id:    deleteResponse.Id,
	}
	return resp
}

type GetResult struct {
	Id     string
	Source *json.RawMessage
	Found  bool
}

func NewGetResult(getResult *elastic.GetResult) *GetResult {
	resp := &GetResult{
		Id:     getResult.Id,
		Source: getResult.Source,
		Found:  getResult.Found,
	}
	return resp
}
