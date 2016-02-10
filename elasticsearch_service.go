package piazza

import (
	"gopkg.in/olivere/elastic.v2"
	"log"
	"errors"
	"os"
)

type ElasticSearchService struct {
	name    ServiceName
	address string

	Client *elastic.Client
}

func newElasticSearchService() (es *ElasticSearchService, err error) {
	client, err := elastic.NewClient(
		elastic.SetURL("https://search-venice-es-pjebjkdaueu2gukocyccj4r5m4.us-east-1.es.amazonaws.com"),
		elastic.SetSniff(false),
		//elastic.SetMaxRetries(5),
		elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)),
		elastic.SetInfoLog(log.New(os.Stdout, "", log.LstdFlags)))
	if err != nil {
		return nil, err
	}

	esversion, err := client.ElasticsearchVersion("https://search-venice-es-pjebjkdaueu2gukocyccj4r5m4.us-east-1.es.amazonaws.com") //("http://127.0.0.1:9200")
	if err != nil {
		// Handle error
		panic(err)
	}
	log.Printf("Elasticsearch version %s", esversion)

/*	info, code, err := client.Ping().Do()
	if err != nil {
		// Handle error
		log.Printf("panic")
		panic(err)
	}
	log.Printf("Elasticsearch returned with code %d and version %s", code, info.Version.Number)
*/

	return &ElasticSearchService{Client: client, name: PzElasticSearch, address: "...es.amazonaws.com"}, nil
}

func (es *ElasticSearchService) GetName() ServiceName {
	return es.name
}

func (es *ElasticSearchService) GetAddress() string {
	return es.address
}

func (es *ElasticSearchService) MakeIndex(index string) error {

	exists, err := es.Client.IndexExists(index).Do()
	if err != nil {
		return err
	}

	if exists {
		deleteIndex, err := es.Client.DeleteIndex(index).Do()
		if err != nil {
			return err
		}
		if !deleteIndex.Acknowledged {
			panic(errors.New("ES Delete Not acknowledged!"))
		}
	}

	createIndex, err := es.Client.CreateIndex(index).Do()
	if err != nil {
		return err
	}

	if !createIndex.Acknowledged {
		panic(errors.New("ES Create Not acknowledged!"))
	}

	return nil
}

// TODO: how often should we do this?
func (es *ElasticSearchService) Flush(index string) error {
	_, err := es.Client.Flush().Index(index).Do()
	if err != nil {
		return err
	}
	return nil
}
