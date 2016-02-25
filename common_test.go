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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"gopkg.in/olivere/elastic.v2"
	"testing"
)

type CommonTester struct {
	suite.Suite
}

func (suite *CommonTester) SetupSuite() {
	//t := suite.T()
}

func (suite *CommonTester) TearDownSuite() {
}

func TestRunSuite(t *testing.T) {
	s := new(CommonTester)
	suite.Run(t, s)
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

func (suite *CommonTester) SetUpIndex(withMapping bool) *EsIndexClient {
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

func (suite *CommonTester) TestEsBasics() {
	t := suite.T()
	assert := assert.New(t)

	es, err := newEsClient(true)
	assert.NoError(err)
	assert.NotNil(es)

	version, err := es.Version()
	assert.NoError(err)
	assert.Contains("1.5.2", version)
}

func (suite *CommonTester) TestEsOps() {
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
		getResult, err := esi.GetById("id1")
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
		getResult, err := esi.GetById("id2")
		assert.NoError(err)
		assert.False(getResult.Found)
	}
}

func (suite *CommonTester) TestEsOpsJson() {
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

		searchResult, err = esi.SearchRaw(str)
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

		searchResult, err = esi.SearchRaw(str)
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

		searchResult, err = esi.SearchRaw(str)
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

func (suite *CommonTester) TestEsMapping() {
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

func (suite *CommonTester) TestEsFull() {
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

func (suite *CommonTester) TestMapping() {
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

func (suite *CommonTester) TestConstructMappingSchema() {
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

func (suite *CommonTester) TestAAAPercolation() {
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

func (suite *CommonTester) TestAAAFullPercolation() {
	t := suite.T()
	assert := assert.New(t)

	var esi *EsIndexClient
	var index = "fullperctest"
	var err error
	var jsn JsonString

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

	type EventType1 struct {
		Id  string `json:"id" binding:"required"`
		Str string `json:"str" binding:"required"`
		Num int `json:"num" binding:"required"`
	}

	type EventType2 struct {
		Id  string `json:"id" binding:"required"`
		Boo bool   `json:"boo" binding:"required"`
		Num int `json:"num" binding:"required"`
	}

	// add mappings
	{
		items1 := make(map[string]MappingElementTypeName)
		items1["id"] = MappingElementTypeString
		items1["str"] = MappingElementTypeString
		items1["num"] = MappingElementTypeInteger
		jsn, err = ConstructMappingSchema("EventType1", items1)
		assert.NoError(err)
		assert.NotEmpty(jsn)
		err = esi.SetMapping("EventType1", jsn)
		assert.NoError(err)

		items2 := make(map[string]MappingElementTypeName)
		items2["id"] = MappingElementTypeString
		items2["boo"] = MappingElementTypeBool
		items2["num"] = MappingElementTypeInteger
		jsn, err = ConstructMappingSchema("EventType2", items2)
		assert.NoError(err)
		assert.NotEmpty(jsn)
		err = esi.SetMapping("EventType2", jsn)
		assert.NoError(err)
	}

	var condition1 JsonString = `{
 	 	"query": {
    		"match": {
      			"str": {
        			"query": "kitten"
      			}
    		}
  		}
	}`

	var condition2 JsonString = `{
		"query" : {
			"match" : {
				"boo" : true
			}
		}
	}`

	var condition3 JsonString = `{
		"query" : {
			"match" : {
				"num" : 17
			}
		}
	}`

	var condition4 JsonString = `{
		"query" : {
			"range" : {
				"num" : {
					"lt": 10.0
				}
			}
		}
	}`

	// add perc queries (conditions)
	{
		_, err = esi.AddPercolationQuery("PQ1", condition1)
		assert.NoError(err)

		_, err = esi.AddPercolationQuery("PQ2", condition2)
		assert.NoError(err)

		_, err = esi.AddPercolationQuery("PQ3", condition3)
		assert.NoError(err)

		_, err = esi.AddPercolationQuery("PQ4", condition4)
		assert.NoError(err)
	}

	orderedMatches := func(matches []*elastic.PercolateMatch) (*elastic.PercolateMatch, *elastic.PercolateMatch) {
		if matches[0].Id > matches[1].Id {
			matches[0], matches[1] = matches[1], matches[0]
		}
		return matches[0], matches[1]
	}

	// add perc documents (events)
	{
		event1 := EventType1{Id: "E1", Str: "kitten", Num: 17}
		event2 := EventType2{Id: "E2", Boo: true, Num: 17}
		event3 := EventType1{Id: "E3", Str: "lemur", Num: -31}

		percolateResponse, err := esi.AddPercolationDocument("EventType1", event1)
		assert.NoError(err)
		assert.EqualValues(2, percolateResponse.Total)
		{
			match1, match2 := orderedMatches(percolateResponse.Matches)
			assert.Equal("PQ1", match1.Id)
			assert.Equal(esi.index, match1.Index)
			assert.Equal("PQ3", match2.Id)
			assert.Equal(esi.index, match2.Index)
		}

		percolateResponse, err = esi.AddPercolationDocument("EventType2", event2)
		assert.NoError(err)
		assert.EqualValues(2, percolateResponse.Total)
		{
			match1, match2 := orderedMatches(percolateResponse.Matches)
			assert.Equal("PQ2", match1.Id)
			assert.Equal(esi.index, match1.Index)
			assert.Equal("PQ3", match2.Id)
			assert.Equal(esi.index, match2.Index)
		}

		percolateResponse, err = esi.AddPercolationDocument("EventType1", event3)
		assert.NoError(err)
		assert.EqualValues(1, percolateResponse.Total)
		assert.Equal("PQ4", percolateResponse.Matches[0].Id)
	}
}
