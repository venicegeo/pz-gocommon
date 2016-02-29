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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"gopkg.in/olivere/elastic.v2"
	"sort"
)

type EsTester struct {
	suite.Suite
}

func (suite *EsTester) SetupSuite() {
	//t := suite.T()
}

func (suite *EsTester) TearDownSuite() {
}

type Obj struct {
	Id   string `json:"id" binding:"required"`
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
	{Id: "id0", Data: "data0", Tags: "foo bar"},
	{Id: "id1", Data: "data1", Tags: "bar baz"},
	{Id: "id2", Data: "data2", Tags: "foo"},
}

func (suite *EsTester) SetUpIndex(withMapping bool) *EsIndexClient {
	t := suite.T()
	assert := assert.New(t)

	index := "testing-index"

	esBase, err := newEsClient(true)
	assert.NoError(err)
	assert.NotNil(esBase)

	esi := NewEsIndexClient(esBase, index)
	assert.NotNil(esi)

	ok, err := esi.Exists()
	assert.NoError(err)
	if ok {
		err = esi.Delete()
		assert.NoError(err)
	}

	// make the index
	err = esi.Create()
	assert.NoError(err)
	exists, err := esi.Exists()
	assert.NoError(err)
	assert.True(exists)

	if withMapping {
		err := esi.SetMapping("Obj", objMapping)
		assert.NoError(err)
	}

	// populate the index
	for _, o := range objs {
		indexResult, err := esi.PostData("Obj", o.Id, o)
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

func (suite *EsTester) TestEsBasics() {
	t := suite.T()
	assert := assert.New(t)

	es, err := newEsClient(true)
	assert.NoError(err)
	assert.NotNil(es)

	version, err := es.Version()
	assert.NoError(err)
	assert.Contains("1.5.2", version)
}

func (suite *EsTester) TestEsOps() {
	t := suite.T()
	assert := assert.New(t)

	var tmp1, tmp2 Obj
	var err error
	var src *json.RawMessage
	var searchResult *elastic.SearchResult

	esi := suite.SetUpIndex(false)
	assert.NotNil(esi)
	defer func() {
		esi.Close()
		esi.Delete()
	}()

	{
		// GET a specific one
		getResult, err := esi.GetById("Obj", "id1")
		assert.NoError(err)
		assert.NotNil(getResult)
		src = getResult.Source
		err = json.Unmarshal(*src, &tmp1)
		assert.NoError(err)
		assert.EqualValues("data1", tmp1.Data)
	}

	{
		// SEARCH for everything
		searchResult, err := esi.SearchByMatchAll()
		assert.NoError(err)
		assert.NotNil(searchResult)

		assert.Equal(int64(3), searchResult.TotalHits())
		assert.EqualValues(3, searchResult.Hits.TotalHits)

		m := make(map[string]Obj)

		for _, hit := range searchResult.Hits.Hits {
			err = json.Unmarshal(*hit.Source, &tmp1)
			assert.NoError(err)
			m[tmp1.Id] = tmp1
		}

		assert.Contains(m, "id0")
		assert.Contains(m, "id1")
		assert.Contains(m, "id2")
	}

	{
		// SEARCH for a specific one
		searchResult, err = esi.SearchByTermQuery("id", "id1")
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
		searchResult, err = esi.SearchByTermQuery("tags", "foo")
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

		ok1 := ("id0" == tmp1.Id && "id2" == tmp2.Id)
		ok2 := ("id0" == tmp2.Id && "id2" == tmp1.Id)
		assert.True((ok1 || ok2) && !(ok1 && ok2))
	}

	{
		// DELETE by id
		_, err = esi.DeleteById("Obj", "id2")
		assert.NoError(err)
		getResult, err := esi.GetById("Obj", "id2")
		assert.NoError(err)
		assert.False(getResult.Found)
	}
}

func (suite *EsTester) TestEsOpsJson() {
	t := suite.T()
	assert := assert.New(t)

	var tmp1, tmp2 Obj
	var err error
	var src *json.RawMessage

	var searchResult *elastic.SearchResult

	esi := suite.SetUpIndex(false)
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

		searchResult, err = esi.SearchByJson(str)
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

		searchResult, err = esi.SearchByJson(str)
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

		searchResult, err = esi.SearchByJson(str)
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

		ok1 := ("id0" == tmp1.Id && "id2" == tmp2.Id)
		ok2 := ("id0" == tmp2.Id && "id2" == tmp1.Id)
		assert.True((ok1 || ok2) && !(ok1 && ok2))
	}
}

func (suite *EsTester) TestEsMapping() {
	t := suite.T()
	assert := assert.New(t)

	var err error

	esi := suite.SetUpIndex(false)
	assert.NotNil(esi)
	defer func() {
		esi.Close()
		esi.Delete()
	}()

	var mapping JsonString = `{
		"tweetdoc":{
			"properties":{
				"message":{
					"type":"string",
					"store":true
			    }
		    }
	    }
    }`

	err = esi.SetMapping("tweetdoc", mapping)
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

func (suite *EsTester) TestEsFull() {
	t := suite.T()
	assert := assert.New(t)

	var err error

	esi := suite.SetUpIndex(true)
	assert.NotNil(esi)
	defer func() {
		esi.Close()
		esi.Delete()
	}()

	type NotObj struct {
		Id   int    `json:"id" binding:"required"`
		Data string `json:"data" binding:"required"`
		Foo  bool   `json:"foo" binding:"required"`
	}
	o := NotObj{Id: 99, Data: "quick fox", Foo: true}

	indexResult, err := esi.PostData("Obj", "88", o)
	assert.NoError(err)
	assert.NotNil(indexResult)
}

func (suite *EsTester) TestMapping() {
	t := suite.T()
	assert := assert.New(t)

	esi := suite.SetUpIndex(false)
	assert.NotNil(esi)
	defer func() {
		esi.Close()
		esi.Delete()
	}()

	var err error

	var jsn JsonString = `{
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

	jsn, err = ConvertJsonToCompactJson(jsn)
	assert.NoError(err)

	expected := jsn

	err = esi.SetMapping("MyTestObj", jsn)
	assert.NoError(err)

	mapobj, err := esi.GetMapping("MyTestObj")
	assert.NoError(err)

	actual, err := ConvertObjectToJsonString(mapobj, true)
	assert.NoError(err)

	assert.Equal(expected, actual)
}

func (suite *EsTester) TestConstructMappingSchema() {
	t := suite.T()
	assert := assert.New(t)

	es := suite.SetUpIndex(false)
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

	err = es.SetMapping("MyTestObj", JsonString(actual))
	assert.NoError(err)
}

func (suite *EsTester) TestPercolation() {
	t := suite.T()
	assert := assert.New(t)

	esi := suite.SetUpIndex(false)
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

	var query1 JsonString = `{
 	 	"query": {
    		"match": {
      			"tag": {
        			"query": "kitten"
      			}
    		}
  		}
	}`
	var query2 JsonString = `{
		"query" : {
			"match" : {
				"tag" : "lemur"
			}
		}
	}`

	_, err = esi.AddPercolationQuery("p1", query1)
	assert.NoError(err)

	type Event struct {
		Id  string `json:"id" binding:"required"`
		Tag string `json:"tag" binding:"required"`
	}
	event1 := Event{Id: "id1", Tag: "kitten"}
	event2 := Event{Id: "id2", Tag: "cat"}
	event3 := Event{Id: "id3", Tag: "lemur"}

	percolateResponse, err := esi.AddPercolationDocument("Event", event1)
	assert.NoError(err)
	assert.EqualValues(1, percolateResponse.Total)
	assert.Equal("p1", percolateResponse.Matches[0].Id)

	percolateResponse, err = esi.AddPercolationDocument("Event", event2)
	assert.NoError(err)
	assert.EqualValues(0, percolateResponse.Total)

	_, err = esi.AddPercolationQuery("p2", query2)
	assert.NoError(err)

	percolateResponse, err = esi.AddPercolationDocument("Event", event3)
	assert.NoError(err)
	assert.EqualValues(1, percolateResponse.Total)
	assert.Equal("p2", percolateResponse.Matches[0].Id)
}

type ById []*elastic.PercolateMatch

func (a ById) Len() int {
	return len(a)
}
func (a ById) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a ById) Less(i, j int) bool {
	return a[i].Id < a[j].Id
}
func sortMatches(matches []*elastic.PercolateMatch) []*elastic.PercolateMatch {
	sort.Sort(ById(matches))
	return matches
}

func (suite *EsTester) TestFullPercolation() {
	t := suite.T()
	assert := assert.New(t)

	var esi *EsIndexClient
	var index = "fullperctest"
	var err error

	defer func() {
		esi.Close()
		esi.Delete()
	}()

	// create index
	{
		esBase, err := newEsClient(true)
		assert.NoError(err)
		assert.NotNil(esBase)

		esi = NewEsIndexClient(esBase, index)
		assert.NotNil(esi)

		exists, err := esi.Exists()
		assert.NoError(err)
		assert.False(exists)

		// make the index
		err = esi.Create()
		assert.NoError(err)

		exists, err = esi.Exists()
		assert.NoError(err)
		assert.True(exists)
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

	addQueries := func(queries map[string]JsonString) {
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
		Id  string `json:"id" binding:"required"`
		Str string `json:"str" binding:"required"`
		Num int    `json:"num" binding:"required"`
	}

	type EventType2 struct {
		Id  string `json:"id" binding:"required"`
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

	queries := map[string]JsonString{
		"Q1": `{
 	 		"query": {
				"match": {
					"str": {
						"query": "kitten"
					}
				}
			}
		}`,
		"Q2": `{
			"query" : {
				"match" : {
					"boo" : true
				}
			}
		}`,
		"Q3": `{
			"query" : {
				"match" : {
					"num" : 17
				}
			}
		}`,
		"Q4": `{
			"query" : {
				"range" : {
					"num" : {
						"lt": 10.0
					}
				}
			}
		}`,
		"Q5": `{
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
		}`,
		"Q6": `{
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
		}`,
	}
	addQueries(queries)

	//-----------------------------------------------------------------------

	tests := []TestCase{
		TestCase{
			typeName: "EventType1",
			event:    EventType1{Id: "E1", Str: "kitten", Num: 17},
			expected: []string{"Q1", "Q3", "Q6"},
		},
		TestCase{
			typeName: "EventType2",
			event:    EventType2{Id: "E2", Boo: true, Num: 17},
			expected: []string{"Q2", "Q3", "Q5"},
		},
		TestCase{
			typeName: "EventType1",
			event:    EventType1{Id: "E3", Str: "lemur", Num: -31},
			expected: []string{"Q4"},
		},
	}

	addEvents(tests)
}