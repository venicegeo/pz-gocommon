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
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/venicegeo/pz-gocommon"
	"gopkg.in/olivere/elastic.v2"
)

type EsTester struct {
	suite.Suite
}

func (suite *EsTester) SetupSuite() {
	//t := suite.T()
}

func (suite *EsTester) TearDownSuite() {
}

func TestRunSuite(t *testing.T) {
	s1 := new(EsTester)
	suite.Run(t, s1)
}

type Obj struct {
	ID   string `json:"id" binding:"required"`
	Data string `json:"data" binding:"required"`
	Tags string `json:"tags" binding:"required"`
}

const objMapping = `{
	 "Obj":{
		"properties":{
			"id": {
				"type":"string"
			},
			"data": {
				"type":"string"
			},
			"tags": {
				"type":"string"
			}
		}
	}
}`

var objs = []Obj{
	{ID: "id0", Data: "data0", Tags: "foo bar"},
	{ID: "id1", Data: "data1", Tags: "bar baz"},
	{ID: "id2", Data: "data2", Tags: "foo"},
}

const mapping = "Obj"

func (suite *EsTester) SetUpIndex() *Index {
	t := suite.T()
	assert := assert.New(t)

	esBase, err := NewClient(nil, true)
	assert.NoError(err)
	assert.NotNil(esBase)

	esi := NewIndex(esBase, "estest")
	assert.NotNil(esi)

	err = esi.Delete()
	//assert.NoError(err)

	ok := esi.IndexExists()
	assert.False(ok)

	// make the index
	err = esi.Create()
	assert.NoError(err)
	ok = esi.IndexExists()
	assert.True(ok)

	if mapping != "" {
		err = esi.SetMapping(mapping, objMapping)
		assert.NoError(err)
	}

	// populate the index
	for _, o := range objs {
		indexResult, err := esi.PostData(mapping, o.ID, o)
		assert.NoError(err)
		assert.NotNil(indexResult)
	}

	// Flush
	// TODO: needed? how often?
	err = esi.Flush()
	assert.NoError(err)

	return esi
}

//---------------------------------------------------------------------------

func deleteOldIndexes(es *elastic.Client) {
	s, err := es.IndexNames()
	if err != nil {
		panic(err)
	}
	log.Printf("%d indexes", len(s))

	del := func(nam string) {
		ret, err := es.DeleteIndex(nam).Do()
		if err != nil {
			log.Printf("%s: %s", nam, err.Error())
		} else {
			log.Printf("%s: %t", nam, ret.Acknowledged)
		}
	}

	for _, v := range s {
		if strings.HasPrefix(v, "alerts.") ||
			strings.HasPrefix(v, "triggers") ||
			strings.HasPrefix(v, "events") ||
			strings.HasPrefix(v, "eventtypes") ||
			strings.HasPrefix(v, "estest.") {
			del(v)
		} else {
			log.Printf("Skipping %s", v)
		}
	}

	panic(999)
}

func (suite *EsTester) Test01Client() {
	t := suite.T()
	assert := assert.New(t)

	es, err := NewClient(nil, true)
	assert.NoError(err)
	assert.NotNil(es)

	version, err := es.Version()
	assert.NoError(err)
	assert.Contains("1.5.2", version)

	//deleteOldIndexes(es.lib)
}

func (suite *EsTester) Test02SimplePost() {
	t := suite.T()
	assert := assert.New(t)

	var err error

	esi := suite.SetUpIndex()
	assert.NotNil(esi)
	defer func() {
		esi.Close()
		esi.Delete()
	}()

	type NotObj struct {
		ID   int    `json:"id" binding:"required"`
		Data string `json:"data" binding:"required"`
		Foo  bool   `json:"foo" binding:"required"`
	}
	o := NotObj{ID: 99, Data: "quick fox", Foo: true}

	indexResult, err := esi.PostData(mapping, "88", o)
	assert.NoError(err)
	assert.NotNil(indexResult)
}

func (suite *EsTester) Test03Operations() {
	t := suite.T()
	assert := assert.New(t)

	var tmp1, tmp2 Obj
	var err error
	var src *json.RawMessage
	var searchResult *elastic.SearchResult

	esi := suite.SetUpIndex()
	assert.NotNil(esi)
	defer func() {
		esi.Close()
		esi.Delete()
	}()

	{
		// GET a specific one
		getResult, err := esi.GetByID(mapping, "id1")
		assert.NoError(err)
		assert.NotNil(getResult)
		src = getResult.Source
		err = json.Unmarshal(*src, &tmp1)
		assert.NoError(err)
		assert.EqualValues("data1", tmp1.Data)
	}

	{
		// SEARCH for everything
		searchResult, err := esi.FilterByMatchAll(mapping)
		assert.NoError(err)
		assert.NotNil(searchResult)

		assert.Equal(int64(3), searchResult.TotalHits())
		assert.EqualValues(3, searchResult.Hits.TotalHits)

		m := make(map[string]Obj)

		for _, hit := range searchResult.Hits.Hits {
			err = json.Unmarshal(*hit.Source, &tmp1)
			assert.NoError(err)
			m[tmp1.ID] = tmp1
		}

		assert.Contains(m, "id0")
		assert.Contains(m, "id1")
		assert.Contains(m, "id2")
	}

	{
		// SEARCH for a specific one
		searchResult, err = esi.FilterByTermQuery(mapping, "id", "id1")
		assert.NoError(err)
		assert.NotNil(searchResult)
		assert.EqualValues(1, searchResult.Hits.TotalHits)
		assert.NotNil(searchResult.Hits.Hits[0])
		src = searchResult.Hits.Hits[0].Source
		assert.NotNil(src)
		err = json.Unmarshal(*src, &tmp1)
		assert.NoError(err)
		assert.EqualValues("data1", tmp1.Data)
	}

	{
		// SEARCH fuzzily
		searchResult, err = esi.FilterByTermQuery(mapping, "tags", "foo")
		assert.NoError(err)
		assert.NotNil(searchResult)
		assert.EqualValues(2, searchResult.Hits.TotalHits)
		assert.NotNil(searchResult.Hits.Hits[0])

		src = searchResult.Hits.Hits[0].Source
		assert.NotNil(src)
		err = json.Unmarshal(*src, &tmp1)
		assert.NoError(err)

		src = searchResult.Hits.Hits[1].Source
		assert.NotNil(src)
		err = json.Unmarshal(*src, &tmp2)
		assert.NoError(err)

		ok1 := ("id0" == tmp1.ID && "id2" == tmp2.ID)
		ok2 := ("id0" == tmp2.ID && "id2" == tmp1.ID)
		assert.True((ok1 || ok2) && !(ok1 && ok2))
	}

	{
		// DELETE by id
		_, err = esi.DeleteByID(mapping, "id2")
		assert.NoError(err)
		_, err := esi.GetByID(mapping, "id2")
		assert.Error(err)
	}
}

func (suite *EsTester) Test04JsonOperations() {
	t := suite.T()
	assert := assert.New(t)

	var tmp1, tmp2 Obj
	var err error
	var src *json.RawMessage

	var searchResult *elastic.SearchResult

	esi := suite.SetUpIndex()
	assert.NotNil(esi)
	defer func() {
		esi.Close()
		esi.Delete()
	}()

	// SEARCH for everything
	{
		str :=
			`{
		      "query": {
			        "match_all": {}
    	        }
            }`

		searchResult, err = esi.SearchByJSON(mapping, str)
		assert.NoError(err)
		assert.NotNil(searchResult)

		for _, hit := range searchResult.Hits.Hits {
			err = json.Unmarshal(*hit.Source, &tmp1)
			assert.NoError(err)
		}
		assert.EqualValues(3, searchResult.Hits.TotalHits)
	}

	// SEARCH for a specific one
	{
		str :=
			`{
    	        "query": {
	    	        "term": {"id":"id1"}
	            }
            }`

		searchResult, err = esi.SearchByJSON(mapping, str)
		assert.NoError(err)
		assert.NotNil(searchResult)

		assert.EqualValues(1, searchResult.Hits.TotalHits)
		src = searchResult.Hits.Hits[0].Source
		assert.NotNil(src)
		err = json.Unmarshal(*src, &tmp1)
		assert.NoError(err)
		assert.EqualValues("data1", tmp1.Data)
	}

	// SEARCH fuzzily
	{
		str :=
			`{
	            "query": {
		            "term": {"tags":"foo"}
	            }
            }`

		searchResult, err = esi.SearchByJSON(mapping, str)
		assert.NoError(err)
		assert.NotNil(searchResult)

		assert.EqualValues(2, searchResult.Hits.TotalHits)
		assert.NotNil(searchResult.Hits.Hits[0])

		src = searchResult.Hits.Hits[0].Source
		assert.NotNil(src)
		err = json.Unmarshal(*src, &tmp1)
		assert.NoError(err)

		src = searchResult.Hits.Hits[1].Source
		assert.NotNil(src)
		err = json.Unmarshal(*src, &tmp2)
		assert.NoError(err)

		ok1 := ("id0" == tmp1.ID && "id2" == tmp2.ID)
		ok2 := ("id0" == tmp2.ID && "id2" == tmp1.ID)
		assert.True((ok1 || ok2) && !(ok1 && ok2))
	}
}

func (suite *EsTester) Test05Mapping() {
	t := suite.T()
	assert := assert.New(t)

	var err error

	esi := suite.SetUpIndex()
	assert.NotNil(esi)
	defer func() {
		esi.Close()
		esi.Delete()
	}()

	mapping :=
		`{
		    "tweetdoc":{
			    "properties":{
				    "message":{
					    "type":"string",
					    "store":true
    			    }
	    	    }
	        }
        }`

	err = esi.SetMapping("tweetdoc", piazza.JsonString(mapping))
	assert.NoError(err)

	mappings, err := esi.GetMapping("tweetdoc")
	assert.NoError(err)

	tweetdoc := mappings.(map[string]interface{})["tweetdoc"]
	assert.NotNil(tweetdoc)
	properties := tweetdoc.(map[string]interface{})["properties"]
	assert.NotNil(properties)
	message := properties.(map[string]interface{})["message"]
	assert.NotNil(message)
	typ := message.(map[string]interface{})["type"].(string)
	assert.NotNil(typ)
	store := message.(map[string]interface{})["store"].(bool)
	assert.NotNil(store)

	assert.EqualValues("string", typ)
	assert.True(store)
}

func (suite *EsTester) Test06SetMapping() {
	t := suite.T()
	assert := assert.New(t)

	esi := suite.SetUpIndex()
	assert.NotNil(esi)
	defer func() {
		esi.Close()
		esi.Delete()
	}()

	var err error

	data :=
		`{
			"MyTestObj": {
				"properties":{
					"bool1": {"type": "boolean"},
					"date1": {"format": "dateOptionalTime", "type": "date"},
					"double1": {"type": "double"},
					"integer1": {"type": "integer"},
					"integer2": {"type": "integer"}
				}
			}
		}`
	jsn := piazza.JsonString(data)
	jsn, err = jsn.ToCompactJson()
	assert.NoError(err)

	expected := jsn

	err = esi.SetMapping("MyTestObj", jsn)
	assert.NoError(err)

	mapobj, err := esi.GetMapping("MyTestObj")
	assert.NoError(err)

	actual, err := piazza.ConvertObjectToJsonString(mapobj, true)
	assert.NoError(err)

	assert.Equal(expected, actual)

	mappings, err := esi.GetTypes()
	assert.NoError(err)
	assert.Len(mappings, 2)
	assert.True((mappings[0] == "Obj" && mappings[1] == "MyTestObj") ||
		(mappings[1] == "Obj" && mappings[0] == "MyTestObj"))
}

func (suite *EsTester) Test07ConstructMapping() {
	t := suite.T()
	assert := assert.New(t)

	es := suite.SetUpIndex()
	assert.NotNil(es)
	defer func() {
		es.Close()
		es.Delete()
	}()

	items := make(map[string]MappingElementTypeName)

	items["integer1"] = MappingElementTypeInteger
	items["integer2"] = MappingElementTypeInteger
	items["double1"] = MappingElementTypeDouble
	items["bool1"] = MappingElementTypeBool
	items["date1"] = MappingElementTypeDate

	jsonstr, err := ConstructMappingSchema("MyTestObj", items)
	assert.NoError(err)
	assert.NotNil(jsonstr)
	assert.NotEmpty(jsonstr)

	var iface interface{}
	err = json.Unmarshal([]byte(jsonstr), &iface)
	assert.NoError(err)

	byts, err := json.Marshal(iface)
	assert.NoError(err)
	assert.NotNil(byts)

	actual := string(byts)

	expected :=
		`{"MyTestObj":{"properties":{"bool1":{"type":"boolean"},"date1":{"type":"date"},"double1":{"type":"double"},"integer1":{"type":"integer"},"integer2":{"type":"integer"}}}}`

	assert.Equal(expected, actual)

	err = es.SetMapping("MyTestObj", piazza.JsonString(actual))
	assert.NoError(err)
}

func (suite *EsTester) Test08Percolation() {
	t := suite.T()
	assert := assert.New(t)

	esi := suite.SetUpIndex()
	assert.NotNil(esi)
	defer func() {
		esi.Close()
		esi.Delete()
	}()

	items := make(map[string]MappingElementTypeName)
	items["tag"] = MappingElementTypeString
	jsonstr, err := ConstructMappingSchema("Event", items)
	assert.NoError(err)
	assert.NotEmpty(jsonstr)

	err = esi.SetMapping("Event", jsonstr)
	assert.NoError(err)

	query1 :=
		`{
 	 	"query": {
    		"match": {
      			"tag": {
        			"query": "kitten"
      			}
    		}
  		}
	}`
	query2 :=
		`{
		"query" : {
			"match" : {
				"tag" : "lemur"
			}
		}
	}`

	_, err = esi.AddPercolationQuery("p1", piazza.JsonString(query1))
	assert.NoError(err)

	type Event struct {
		ID  string `json:"id" binding:"required"`
		Tag string `json:"tag" binding:"required"`
	}
	event1 := Event{ID: "id1", Tag: "kitten"}
	event2 := Event{ID: "id2", Tag: "cat"}
	event3 := Event{ID: "id3", Tag: "lemur"}

	percolateResponse, err := esi.AddPercolationDocument("Event", event1)
	assert.NoError(err)
	assert.EqualValues(1, percolateResponse.Total)
	assert.Equal("p1", percolateResponse.Matches[0].Id)

	percolateResponse, err = esi.AddPercolationDocument("Event", event2)
	assert.NoError(err)
	assert.EqualValues(0, percolateResponse.Total)

	_, err = esi.AddPercolationQuery("p2", piazza.JsonString(query2))
	assert.NoError(err)

	percolateResponse, err = esi.AddPercolationDocument("Event", event3)
	assert.NoError(err)
	assert.EqualValues(1, percolateResponse.Total)
	assert.Equal("p2", percolateResponse.Matches[0].Id)
}

type ByID []*elastic.PercolateMatch

func (a ByID) Len() int {
	return len(a)
}
func (a ByID) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a ByID) Less(i, j int) bool {
	return a[i].Id < a[j].Id
}
func sortMatches(matches []*elastic.PercolateMatch) []*elastic.PercolateMatch {
	sort.Sort(ByID(matches))
	return matches
}

func (suite *EsTester) Test09FullPercolation() {
	t := suite.T()
	assert := assert.New(t)

	var esi *Index
	var index = "fullperctest"
	var err error

	defer func() {
		esi.Close()
		esi.Delete()
	}()

	// create index
	{
		esBase, err := NewClient(nil, true)
		assert.NoError(err)
		assert.NotNil(esBase)

		esi = NewIndex(esBase, index)
		assert.NotNil(esi)

		// make the index
		err = esi.Create()
		assert.NoError(err)

		ok := esi.IndexExists()
		assert.True(ok)
	}

	// flush
	{
		err = esi.Flush()
		assert.NoError(err)
	}

	//-----------------------------------------------------------------------

	addMappings := func(maps map[string](map[string]MappingElementTypeName)) {
		for k, v := range maps {
			jsn, err := ConstructMappingSchema(k, v)
			assert.NoError(err)
			assert.NotEmpty(jsn)
			err = esi.SetMapping(k, jsn)
			assert.NoError(err)
		}

		err = esi.Flush()
		assert.NoError(err)
	}

	addQueries := func(queries map[string]piazza.JsonString) {
		for k, v := range queries {
			_, err = esi.AddPercolationQuery(k, v)
			assert.NoError(err)
		}

		err = esi.Flush()
		assert.NoError(err)
	}

	type TestCase struct {
		typeName string
		event    interface{}
		expected []string
	}

	addEvents := func(tests []TestCase) {
		for i, t := range tests {

			percolateResponse, err := esi.AddPercolationDocument(t.typeName, t.event)
			assert.NoError(err)

			assert.EqualValues(len(t.expected), percolateResponse.Total, fmt.Sprintf("for test #%d", i))

			matches := sortMatches(percolateResponse.Matches)

			for i, expected := range t.expected {
				assert.Equal(esi.index, matches[i].Index)
				assert.Equal(expected, matches[i].Id)
			}
		}
	}

	//-----------------------------------------------------------------------

	type EventType1 struct {
		ID  string `json:"id" binding:"required"`
		Str string `json:"str" binding:"required"`
		Num int    `json:"num" binding:"required"`
	}

	type EventType2 struct {
		ID  string `json:"id" binding:"required"`
		Boo bool   `json:"boo" binding:"required"`
		Num int    `json:"num" binding:"required"`
	}

	maps := map[string](map[string]MappingElementTypeName){
		"EventType1": map[string]MappingElementTypeName{
			"id":  MappingElementTypeString,
			"str": MappingElementTypeString,
			"num": MappingElementTypeInteger,
		},
		"EventType2": map[string]MappingElementTypeName{
			"id":  MappingElementTypeString,
			"boo": MappingElementTypeBool,
			"num": MappingElementTypeInteger,
		},
	}
	addMappings(maps)

	//-----------------------------------------------------------------------

	q1 :=
		`{
 	        "query": {
		    	"match": {
			    	"str": {
				    	"query": "kitten"
    				}
	    		}
		    }
	    }`
	q2 :=
		`{
			"query" : {
				"match" : {
					"boo" : true
				}
			}
		}`
	q3 :=
		`{
			"query" : {
				"match" : {
					"num" : 17
				}
			}
		}`
	q4 :=
		`{
			"query" : {
				"range" : {
					"num" : {
						"lt": 10.0
					}
				}
			}
		}`
	q5 :=
		`{
			"query" : {
				"filtered": {
					"query": {
						"match": {
							"num": 17
						}
					},
					"filter": {
						"term": {
							"_type": "EventType2"
						}
					}
				}
			}
		}`
	q6 :=
		`{
		"query" : {
			"bool": {
				"must": [
					{
						"match" : {
							"num" : 17
						}
					},
					{
						"match" : {
							"_type" : "EventType1"
						}
					}
				]
			}
		}
	}`
	queries := map[string]piazza.JsonString{
		"Q1": piazza.JsonString(q1),
		"Q2": piazza.JsonString(q2),
		"Q3": piazza.JsonString(q3),
		"Q4": piazza.JsonString(q4),
		"Q5": piazza.JsonString(q5),
		"Q6": piazza.JsonString(q6),
	}
	addQueries(queries)

	//-----------------------------------------------------------------------

	tests := []TestCase{
		TestCase{
			typeName: "EventType1",
			event:    EventType1{ID: "E1", Str: "kitten", Num: 17},
			expected: []string{"Q1", "Q3", "Q6"},
		},
		TestCase{
			typeName: "EventType2",
			event:    EventType2{ID: "E2", Boo: true, Num: 17},
			expected: []string{"Q2", "Q3", "Q5"},
		},
		TestCase{
			typeName: "EventType1",
			event:    EventType1{ID: "E3", Str: "lemur", Num: -31},
			expected: []string{"Q4"},
		},
	}

	addEvents(tests)
}
