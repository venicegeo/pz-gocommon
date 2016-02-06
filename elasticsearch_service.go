package piazza

import (
	"gopkg.in/olivere/elastic.v3"
)

type ElasticSearchService struct {
	name    string
	address string

	Client *elastic.Client
}

func newElasticSearchService() (es *ElasticSearchService, err error) {
	client, err := elastic.NewClient(
		elastic.SetURL("https://search-venice-es-pjebjkdaueu2gukocyccj4r5m4.us-east-1.es.amazonaws.com"),
		elastic.SetSniff(false),
		elastic.SetMaxRetries(5),
		/*elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)),*/
		/*elastic.SetInfoLog(log.New(os.Stdout, "", log.LstdFlags))*/)
	if err != nil {
		return nil, err
	}

	return &ElasticSearchService{Client: client, name: PzElasticSearch, address: "...es.amazonaws.com"}, nil
}

func (es *ElasticSearchService) GetName() string {
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
		_, err = es.Client.DeleteIndex(index).Do()
		if err != nil {
			return err
		}
	}

	_, err = es.Client.CreateIndex(index).Do()
	if err != nil {
		return err
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
