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
	"strings"
	"time"

	"github.com/venicegeo/pz-gocommon/gocommon"

	"gopkg.in/olivere/elastic.v3"
)

// Index is the representation of the Elasticsearch index used by the
type Index struct {
	lib     *elastic.Client
	version string
	index   string
}

// NewIndex is the initializing constructor for the type Index
func NewIndex(sys *piazza.SystemConfig, index string, settings string) (*Index, error) {
	if strings.HasSuffix(index, "$") {
		index = fmt.Sprintf("%s.%x", index[0:len(index)-1], time.Now().Nanosecond())
	}

	esi := &Index{index: index}

	url, err := sys.GetURL(piazza.PzElasticSearch)
	if err != nil {
		return nil, err
	}

	esi.lib, err = elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetSniff(false),
		elastic.SetMaxRetries(5),
		//elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)), // TODO
		//elastic.SetInfoLog(log.New(os.Stdout, "", log.LstdFlags)),
	)
	if err != nil {
		return nil, err
	}

	esi.version, err = esi.lib.ElasticsearchVersion(url)
	if err != nil {
		return nil, err
	}

	// This does nothing if the index is already created, but creates it if not
	esi.Create(settings)

	return esi, nil
}

// GetVersion returns the Elasticsearch version
func (esi *Index) GetVersion() string {
	return esi.version
}

// IndexName returns the name of the index
func (esi *Index) IndexName() string {
	return esi.index
}

// IndexExists checks to see if the index exists
func (esi *Index) IndexExists() bool {
	ok, err := esi.lib.IndexExists(esi.index).Do()
	if err != nil {
		return false
	}
	return ok
}

// TypeExists checks to see if the specified type exists within the index
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

// ItemExists checks to see if the specified item exists within the type and index specified
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

// Create the index; if index already exists, does nothing
func (esi *Index) Create(settings string) error {

	ok := esi.IndexExists()
	if ok {
		//return fmt.Errorf("Index %s already exists", esi.index)
		return nil
	}

	createIndex, err := esi.lib.CreateIndex(esi.index).Body(settings).Do()

	if err != nil {
		return err
	}

	if !createIndex.Acknowledged {
		return fmt.Errorf("Elasticsearch: create index not acknowledged!")
	}

	return nil
}

// Close the index; if index doesn't already exist, does nothing
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

// Delete the index; if index doesn't already exist, does nothing
func (esi *Index) Delete() error {

	ok := esi.IndexExists()
	if !ok {
		return fmt.Errorf("Index %s does not exist", esi.index)
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

// PostData send JSON data to the index.
func (esi *Index) PostData(typ string, id string, obj interface{}) (*IndexResponse, error) {
	ok := esi.IndexExists()
	if !ok {
		log.Printf("Index %s does not exist", esi.index)
		return nil, fmt.Errorf("Index %s does not exist", esi.index)
	}
	ok = esi.TypeExists(typ)
	if !ok {
		log.Printf("Index %s or type %s does not exist", esi.index, typ)
		return nil, fmt.Errorf("Index %s or type %s does not exist", esi.index, typ)
	}

	log.Printf("typ: %#v", typ)
	log.Printf("id: %#v", id)
	log.Printf("obj: %#v", obj)

	indexResponse, err := esi.lib.Index().
		Index(esi.index).
		Type(typ).
		Id(id).
		BodyJson(obj).
		Do()

	log.Printf("IndexResponse: %#v", indexResponse)
	log.Printf("err: %#v", err)

	if err != nil {
		return nil, err
	}
	return NewIndexResponse(indexResponse), nil
}

// GetByID returns a document by ID within the specified index and type
func (esi *Index) GetByID(typ string, id string) (*GetResult, error) {
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

	return NewGetResult(getResult), nil
}

// DeleteByID deletes a document by ID within a specified index and type
func (esi *Index) DeleteByID(typ string, id string) (*DeleteResponse, error) {
	ok := esi.ItemExists(typ, id)
	if !ok {
		return nil, fmt.Errorf("Item %s in index %s and type %s does not exist", id, esi.index, typ)
	}

	deleteResponse, err := esi.lib.Delete().
		Index(esi.index).
		Type(typ).
		Id(id).
		Do()
	return NewDeleteResponse(deleteResponse), err
}

// FilterByMatchAll returns all documents of a specified type
func (esi *Index) FilterByMatchAll(typ string, realFormat *piazza.JsonPagination) (*SearchResult, error) {
	// ok := typ != "" && esi.TypeExists(typ)
	// if !ok {
	// 	return nil, fmt.Errorf("Type %s in index %s does not exist", typ, esi.index)
	// }

	format := NewQueryFormat(realFormat)
	q := elastic.NewMatchAllQuery()
	f := esi.lib.Search().Index(esi.index).Type(typ).Query(q)

	f = f.From(format.From)
	f = f.Size(format.Size)
	f = f.Sort(format.Key, !bool(format.Order))

	log.Printf("FilterByMatchAll: %#v", format)

	searchResult, err := f.Do()
	if err != nil {
		// if the mapping (or the index?) doesn't exist yet, squash the error
		// (this is the case in some of the unit tests which ((try to)) assure the DB is empty)
		resp := &SearchResult{totalHits: 0, hits: make([]*SearchResultHit, 0)}
		return resp, nil
	}

	resp := NewSearchResult(searchResult)
	return resp, nil
}

// FilterByTermQuery creates an Elasticsearch term query and performs the query over the specified type.
// For more information on term queries, see
// https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-term-query.html
func (esi *Index) FilterByTermQuery(typ string, name string, value interface{}) (*SearchResult, error) {
	ok := typ != "" && esi.TypeExists(typ)
	if !ok {
		return nil, fmt.Errorf("Type %s in index %s does not exist", typ, esi.index)
	}

	// Returns a query of the form {"term":{"name":"value"}}
	// The value parameter is typically sent in as a string rather than an interface,
	// but technically value can be an interface.
	termQuery := elastic.NewTermQuery(name, value)
	searchResult, err := esi.lib.Search().
		Index(esi.index).
		Type(typ).
		Query(termQuery).
		//Sort("id", true).
		Do()

	return NewSearchResult(searchResult), err
}

// FilterByMatchQuery creates an Elasticsearch match query and performs the query over the specified type.
// For more information on match queries, see
// https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html
func (esi *Index) FilterByMatchQuery(typ string, name string, value interface{}) (*SearchResult, error) {
	ok := typ != "" && esi.TypeExists(typ)
	if !ok {
		return nil, fmt.Errorf("Type %s in index %s does not exist", typ, esi.index)
	}

	matchQuery := elastic.NewMatchQuery(name, value)
	searchResult, err := esi.lib.Search().
		Index(esi.index).
		Type(typ).
		Query(matchQuery).
		Do()

	return NewSearchResult(searchResult), err
}

// SearchByJSON performs a search over the index via raw JSON
func (esi *Index) SearchByJSON(typ string, jsn string) (*SearchResult, error) {

	ok := typ != "" && esi.TypeExists(typ)
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
		Source(obj).
		Do()
	if err != nil {
		return nil, err
	}

	return NewSearchResult(searchResult), nil
}

// SetMapping sets the _mapping field for a new type
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

// GetTypes returns the list of types within the index
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

// GetMapping returns the _mapping of a type
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

// AddPercolationQuery adds a percolation query to the index
// For more detail on percolation, see
// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-percolate.html
func (esi *Index) AddPercolationQuery(id string, query piazza.JsonString) (*IndexResponse, error) {

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

	return NewIndexResponse(indexResponse), nil
}

// DeletePercolationQuery removes a percolation query from the index
func (esi *Index) DeletePercolationQuery(id string) (*DeleteResponse, error) {
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

	return NewDeleteResponse(deleteResponse), nil
}

// AddPercolationDocument adds a document to the index that is to be percolated
// For more detail on percolation, see
// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-percolate.html
func (esi *Index) AddPercolationDocument(typ string, doc interface{}) (*PercolateResponse, error) {
	ok := esi.TypeExists(typ)
	if !ok {
		return nil, fmt.Errorf("Type %s in index %s does not exist", typ, esi.index)
	}

	percolateResponse, err := esi.lib.
		Percolate().
		Index(esi.index).
		Type(typ).
		Doc(doc).
		Do()
	if err != nil {
		return nil, err
	}

	return NewPercolateResponse(percolateResponse), nil
}
