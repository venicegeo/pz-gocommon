package piazza

import (
	"gopkg.in/olivere/elastic.v3"
)

type ElasticSearch struct {
	Client *elastic.Client
}

func NewElasticSearch() (es *ElasticSearch, err error) {
	client, err := elastic.NewClient(
		elastic.SetURL("https://search-venice-es-pjebjkdaueu2gukocyccj4r5m4.us-east-1.es.amazonaws.com"),
		elastic.SetSniff(false),
		elastic.SetMaxRetries(5),
		/*elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)),*/
		/*elastic.SetInfoLog(log.New(os.Stdout, "", log.LstdFlags))*/)
	if err != nil {
		return nil, err
	}

	return &ElasticSearch{Client: client}, nil
}

func (es *ElasticSearch) MakeIndex(index string) error {

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
func (es *ElasticSearch) Flush(index string) error {
	_, err := es.Client.Flush().Index(index).Do()
	if err != nil {
		return err
	}
	return nil
}
