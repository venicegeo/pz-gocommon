package piazza

import (
	"fmt"
	"gopkg.in/olivere/elastic.v2"
)

// TODO (default is "http://127.0.0.1:9200")
const elasticsearchUrl = "https://search-venice-es-pjebjkdaueu2gukocyccj4r5m4.us-east-1.es.amazonaws.com"

type ElasticSearchService struct {
	name    ServiceName
	address string

	Client *elastic.Client
}

func newElasticSearchService() (*ElasticSearchService, error) {
	client, err := elastic.NewClient(
		elastic.SetURL(elasticsearchUrl),
		elastic.SetSniff(false),
		elastic.SetMaxRetries(5),
		//elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)), // TODO
		//elastic.SetInfoLog(log.New(os.Stdout, "", log.LstdFlags)),
	)
	if err != nil {
		return nil, err
	}

	es := ElasticSearchService{Client: client, name: PzElasticSearch, address: elasticsearchUrl}
	return &es, nil
}

func (es *ElasticSearchService) GetName() ServiceName {
	return es.name
}

func (es *ElasticSearchService) GetAddress() string {
	return es.address
}

func (es *ElasticSearchService) Version() (string, error) {
	return es.Client.ElasticsearchVersion(elasticsearchUrl)
}

func (es *ElasticSearchService) IndexExists(index string) (bool, error) {
	return es.Client.IndexExists(index).Do()
}

// if index already exists, does nothing
func (es *ElasticSearchService) CreateIndex(index string) error {

	ok, err := es.IndexExists(index)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	createIndex, err := es.Client.CreateIndex(index).Do()
	if err != nil {
		return err
	}

	if !createIndex.Acknowledged {
		return fmt.Errorf("Elasticsearch: create index not acknowledged!")
	}

	return nil
}

// if index doesn't already exist, does nothing
func (es *ElasticSearchService) DeleteIndex(index string) error {

	exists, err := es.Client.IndexExists(index).Do()
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	deleteIndex, err := es.Client.DeleteIndex(index).Do()
	if err != nil {
		return err
	}
	if !deleteIndex.Acknowledged {
		return fmt.Errorf("Elasticsearch: delete index not acknowledged!")
	}
	return nil
}

// TODO: how often should we do this?
func (es *ElasticSearchService) FlushIndex(index string) error {
	_, err := es.Client.Flush().Index(index).Do()
	if err != nil {
		return err
	}
	return nil
}

func (es *ElasticSearchService) PostData(index string, mapping string, id string, json interface{}) (*elastic.IndexResult, error) {
	indexResult, err := es.Client.Index().
		Index(index).
		Type(mapping).
		Id(id).
		BodyJson(json).
		Do()
	return indexResult, err
}

func (es *ElasticSearchService) GetById(index string, id string) (*elastic.GetResult, error) {
	getResult, err := es.Client.Get().Index(index).Id(id).Do()
	return getResult, err
}

func (es *ElasticSearchService) DeleteById(index string, mapping string, id string) (*elastic.DeleteResult, error) {
	deleteResult, err := es.Client.Delete().
		Index(index).
		Type(mapping).
		Id(id).
		Do()
	return deleteResult, err
}

// always sorts by id
func (es *ElasticSearchService) SearchByMatchAll(index string) (*elastic.SearchResult, error) {
	searchResult, err := es.Client.Search().
		Index(index).
		Query(elastic.NewMatchAllQuery()).
		Sort("id", true).
		Do()
	return searchResult, err
}

// always sorts by id
func (es *ElasticSearchService) SearchByTermQuery(index string, name string, value interface{}) (*elastic.SearchResult, error) {
	termQuery := elastic.NewTermQuery(name, value)
	searchResult, err := es.Client.Search().
		Index(index).
		Query(&termQuery).
		Sort("id", true).
		Do()
	return searchResult, err
}
