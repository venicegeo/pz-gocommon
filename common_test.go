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

var objs = []Obj{
	{Id: "id0", Data: "data0", Tags: "foo bar"},
	{Id: "id1", Data: "data1", Tags: "bar baz"},
	{Id: "id2", Data: "data2", Tags: "foo"},
}

func (suite *CommonTester) TestElasticSearch() {
	return
	t := suite.T()
	assert := assert.New(t)

	index := "testing-index"
	var tmp1, tmp2 Obj
	var err error
	var src *json.RawMessage
	var ok bool

	// make our client
	es, err := newElasticSearchService(true)
	assert.NoError(err)
	assert.NotNil(es)

	version, err := es.Version()
	assert.NoError(err)
	assert.Contains("1.5.2", version)

	ok, err = es.IndexExists(index)
	assert.NoError(err)
	if ok {
		err = es.DeleteIndex(index)
		assert.NoError(err)
	}

	// make the index
	err = es.CreateIndex(index)
	assert.NoError(err)
	exists, err := es.Client.IndexExists(index).Do()
	assert.NoError(err)
	assert.True(exists)

	ok, err = es.IndexExists(index)
	assert.NoError(err)
	assert.True(ok)

	// populate the index
	for _, o := range objs {
		indexResult, err := es.PostData(index, "Obj", o.Id, o)
		assert.NoError(err)
		assert.NotNil(indexResult)
	}

	// Flush
	// TODO: needed? how often?
	err = es.FlushIndex(index)
	assert.NoError(err)

	// GET for all
	// TODO: simple API?

	// GET a specific one
	getResult, err := es.GetById(index, "id1")
	assert.NoError(err)
	assert.NotNil(getResult)
	src = getResult.Source
	err = json.Unmarshal(*src, &tmp1)
	assert.NoError(err)
	assert.EqualValues("data1", tmp1.Data)

	// SEARCH for everything
	searchResult, err := es.SearchByMatchAll(index)
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

	// SEARCH for a specific one
	searchResult, err = es.SearchByTermQuery(index, "id", "id1")
	assert.NoError(err)
	assert.NotNil(searchResult)
	assert.EqualValues(1, searchResult.Hits.TotalHits)
	assert.NotNil(searchResult.Hits.Hits[0])
	src = searchResult.Hits.Hits[0].Source
	assert.NotNil(src)
	err = json.Unmarshal(*src, &tmp1)
	assert.NoError(err)
	assert.EqualValues("data1", tmp1.Data)

	// SEARCH fuzzily
	searchResult, err = es.SearchByTermQuery(index, "tags", "foo")
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

	// DELETE by id
	_, err = es.DeleteById(index, "Obj", "id2")
	assert.NoError(err)
	getResult, err = es.GetById(index, "id2")
	assert.NoError(err)
	assert.False(getResult.Found)
}

func (suite *CommonTester) TestEsViaSource() {
	t := suite.T()
	assert := assert.New(t)

	index0 := "testing-index"

	var tmp1, tmp2 Obj
	var err error
	var src *json.RawMessage
	var ok bool

	var searchResult *elastic.SearchResult

	// make our client
	es, err := newElasticSearchService(true)
	assert.NoError(err)
	assert.NotNil(es)

	index1 := es.prefixed(index0)

	ok, err = es.IndexExists(index0)
	assert.NoError(err)
	if ok {
		err = es.DeleteIndex(index0)
		assert.NoError(err)
	}

	// make the index
	err = es.CreateIndex(index0)
	assert.NoError(err)

	// populate the index
	for _, o := range objs {
		indexResult, err := es.PostData(index0, "Obj", o.Id, o)
		assert.NoError(err)
		assert.NotNil(indexResult)
	}

	// Flush
	// TODO: needed? how often?
	err = es.FlushIndex(index0)
	assert.NoError(err)

	// SEARCH for everything
	{
		searchResult, err = es.SearchByMatchAll(index0)
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
		str :=
		`{
		    "query": {
			    "match_all": {}
		    }
	    }`

		var strobj interface{}
		err = json.Unmarshal([]byte(str), &strobj)
		assert.NoError(err)

		//bs, err := json.Marshal(i)
		//assert.NoError(err)

		searchResult, err = es.Client.Search().Index(index1).Source(strobj).Do()
		assert.NoError(err)
		assert.NotNil(searchResult)

		//t.Logf("%#v", searchResult.Hits)

		for _, hit := range searchResult.Hits.Hits {
			err = json.Unmarshal(*hit.Source, &tmp1)
			assert.NoError(err)
			//t.Logf("HIT: %s", tmp1.Id)
		}
		assert.EqualValues(3, searchResult.Hits.TotalHits)
	}

	// SEARCH for a specific one
	{
		searchResult, err = es.SearchByTermQuery(index0, "id", "id1")
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
		str :=
		`{
		    "query": {
			    "term": {"id":"id1"}
		    }
	    }`

		var strobj interface{}
		err = json.Unmarshal([]byte(str), &strobj)
		assert.NoError(err)

		searchResult, err = es.Client.Search().Index(index1).Source(strobj).Do()
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
		searchResult, err = es.SearchByTermQuery(index0, "tags", "foo")
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
		str :=
		`{
		    "query": {
			    "term": {"tags":"foo"}
		    }
	    }`

		var strobj interface{}
		err = json.Unmarshal([]byte(str), &strobj)
		assert.NoError(err)

		searchResult, err = es.Client.Search().Index(index1).Source(strobj).Do()
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
