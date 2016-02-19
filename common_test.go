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
	"testing"
	"gopkg.in/olivere/elastic.v2"
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

const objMapping =
`{
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

	es := newEsIndexClient(esBase, index)
	assert.NotNil(esBase)

	ok, err := es.Exists()
	assert.NoError(err)
	if ok {
		err = es.Delete()
		assert.NoError(err)
	}

	// make the index
	err = es.Create()
	assert.NoError(err)
	exists, err := es.Exists()
	assert.NoError(err)
	assert.True(exists)

	if withMapping {
		err := es.SetMapping("Obj", objMapping)
		assert.NoError(err)
	}

	// populate the index
	for _, o := range objs {
		indexResult, err := es.PostData("Obj", o.Id, o)
		assert.NoError(err)
		assert.NotNil(indexResult)
	}

	// Flush
	// TODO: needed? how often?
	err = es.Flush()
	assert.NoError(err)

	return es
}

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

	es := suite.SetUpIndex(false)
	assert.NotNil(es)
	defer func() {
		es.Close()
		es.Delete()
	}()

	{
		// GET a specific one
		getResult, err := es.GetById("id1")
		assert.NoError(err)
		assert.NotNil(getResult)
		src = getResult.Source
		err = json.Unmarshal(*src, &tmp1)
		assert.NoError(err)
		assert.EqualValues("data1", tmp1.Data)
	}

	{
		// SEARCH for everything
		searchResult, err := es.SearchByMatchAll()
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
		searchResult, err = es.SearchByTermQuery("id", "id1")
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
		searchResult, err = es.SearchByTermQuery("tags", "foo")
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
		_, err = es.DeleteById("Obj", "id2")
		assert.NoError(err)
		getResult, err := es.GetById("id2")
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

	es := suite.SetUpIndex(false)
	assert.NotNil(es)
	defer func() {
		es.Close()
		es.Delete()
	}()

	// SEARCH for everything
	{
		str :=
		`{
		    "query": {
			    "match_all": {}
		    }
	    }`

		searchResult, err = es.SearchRaw(str)
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

		searchResult, err = es.SearchRaw(str)
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

		searchResult, err = es.SearchRaw(str)
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

	es := suite.SetUpIndex(false)
	assert.NotNil(es)
	defer func() {
		es.Close()
		es.Delete()
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

	err = es.SetMapping("tweetdoc", mapping)
	assert.NoError(err)

	props, err := es.GetMapping("tweetdoc")
	assert.NoError(err)

	mappings := props.(map[string]interface{})["mappings"]
	assert.NotNil(mappings)
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

	es := suite.SetUpIndex(true)
	assert.NotNil(es)
	defer func() {
		es.Close()
		es.Delete()
	}()

	type NotObj struct {
		Id   int `json:"id" binding:"required"`
		Data string `json:"data" binding:"required"`
		Foo  bool `json:"foo" binding:"required"`
	}
	o := NotObj{Id:99, Data:"quick fox", Foo:true}

	indexResult, err := es.PostData("Obj", "88", o)
	assert.NoError(err)
	assert.NotNil(indexResult)
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

	jsonstr, err := es.ConstructMappingSchema("MyTestObj", items)
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

	err = es.SetMapping("MyTestObj", actual)
	assert.NoError(err)
}
