package piazza

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
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

var objs = []Obj{
	{Id: "id0", Data: "data0", Tags: "foo bar"},
	{Id: "id1", Data: "data1", Tags: "bar baz"},
	{Id: "id2", Data: "data2", Tags: "foo"},
}

func (suite *CommonTester) TestElasticSearch() {
	t := suite.T()
	assert := assert.New(t)

	index := "testing-index"
	var tmp Obj
	var err error
	var src *json.RawMessage
	var ok bool

	// make our client
	es, err := newElasticSearchService()
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
	err = json.Unmarshal(*src, &tmp)
	assert.NoError(err)
	assert.EqualValues("data1", tmp.Data)

	// SEARCH for everything
	searchResult, err := es.SearchByMatchAll(index)
	assert.NoError(err)
	assert.NotNil(searchResult)

	assert.Equal(int64(3), searchResult.TotalHits())
	assert.EqualValues(3, searchResult.Hits.TotalHits)

	m := make(map[string]Obj)

	for _, hit := range searchResult.Hits.Hits {
		err = json.Unmarshal(*hit.Source, &tmp)
		assert.NoError(err)
		m[tmp.Id] = tmp
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
	err = json.Unmarshal(*src, &tmp)
	assert.NoError(err)
	assert.EqualValues("data1", tmp.Data)

	// SEARCH fuzzily
	searchResult, err = es.SearchByTermQuery(index, "tags", "foo")
	assert.NoError(err)
	assert.NotNil(searchResult)
	assert.EqualValues(2, searchResult.Hits.TotalHits)
	assert.NotNil(searchResult.Hits.Hits[0])

	src = searchResult.Hits.Hits[0].Source
	assert.NotNil(src)
	err = json.Unmarshal(*src, &tmp)
	assert.NoError(err)
	assert.EqualValues("id0", tmp.Id)

	src = searchResult.Hits.Hits[1].Source
	assert.NotNil(src)
	err = json.Unmarshal(*src, &tmp)
	assert.NoError(err)
	assert.EqualValues("id2", tmp.Id)

	// DELETE by id
	_, err = es.DeleteById(index, "Obj", "id2")
	assert.NoError(err)
	getResult, err = es.GetById(index, "id2")
	assert.NoError(err)
	assert.False(getResult.Found)
}
