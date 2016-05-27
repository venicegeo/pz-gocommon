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
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

const kafkaHost = "localhost:9092"

type Closer interface {
	Close() error
}

func close(t *testing.T, c Closer) {
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func makeTopicName() string {
	rand.Seed(int64(time.Now().Nanosecond()))
	topicName := fmt.Sprintf("test.%x", rand.Uint32())
	log.Printf("topic: %s", topicName)
	return topicName
}

func Test01(t *testing.T) {
	const M1 = "message one"
	const M2 = "message two"

	var producer sarama.AsyncProducer
	var consumer sarama.Consumer
	var partitionConsumer sarama.PartitionConsumer

	var err error

	topicName := makeTopicName()

	{
		config := sarama.NewConfig()
		config.Producer.Return.Successes = false
		config.Producer.Return.Errors = false

		producer, err = sarama.NewAsyncProducer([]string{kafkaHost}, config)
		if err != nil {
			t.Fatal(err)
		}
		defer close(t, producer)

		producer.Input() <- &sarama.ProducerMessage{
			Topic: topicName,
			Key:   nil,
			Value: sarama.StringEncoder(M1)}

		producer.Input() <- &sarama.ProducerMessage{
			Topic: topicName,
			Key:   nil,
			Value: sarama.StringEncoder(M2)}
	}

	{
		consumer, err = sarama.NewConsumer([]string{kafkaHost}, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer close(t, consumer)

		var offsetNewest int64 = 0
		var partition int32 = 0
		partitionConsumer, err = consumer.ConsumePartition(topicName, partition, offsetNewest)
		if err != nil {
			t.Fatal(err)
		}
		defer close(t, partitionConsumer)
	}

	{
		mssg1 := <-partitionConsumer.Messages()
		t.Logf("Consumed: offset:%d  value:%v", mssg1.Offset, string(mssg1.Value))
		mssg2 := <-partitionConsumer.Messages()
		t.Logf("Consumed: offset:%d  value:%v", mssg2.Offset, string(mssg2.Value))

		if M1 != string(mssg1.Value) {
			t.Errorf("expected %s, got %s", string(M1), mssg1.Value)
		}
		if M2 != string(mssg2.Value) {
			t.Errorf("expected %s, got %s", string(M2), mssg2.Value)
		}
	}
}

func doReads(t *testing.T, topicName string, numReads *int) {
	consumer, err := sarama.NewConsumer([]string{kafkaHost}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer close(t, consumer)

	var offsetNewest int64 = 0
	var partition int32 = 0
	partitionConsumer, err := consumer.ConsumePartition(topicName, partition, offsetNewest)
	if err != nil {
		t.Fatal(err)
	}
	defer close(t, partitionConsumer)

	for {
		msg := <-partitionConsumer.Messages()
		t.Logf("Consumed: offset:%d  value:%v", msg.Offset, string(msg.Value))
		*numReads++
	}

	//t.Logf("Reader done: %d", *numReads)
}

func doWrites(t *testing.T, topicName string, id int, count int) {

	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = false

	producer, err := sarama.NewAsyncProducer([]string{kafkaHost}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer close(t, producer)

	// TODO: handle "err := <-w.Errors():"

	for n := 0; n < count; n++ {
		producer.Input() <- &sarama.ProducerMessage{
			Topic: topicName,
			Key:   nil,
			Value: sarama.StringEncoder(fmt.Sprintf("mssg %d from %d", n, id)),
		}
	}

	t.Logf("Writer done: %d", count)
}

func Test02(t *testing.T) {
	topicName := makeTopicName()

	var numReads1, numReads2 int

	go doReads(t, topicName, &numReads1)
	go doReads(t, topicName, &numReads2)

	n := 3
	go doWrites(t, topicName, 1, n)
	go doWrites(t, topicName, 2, n)

	time.Sleep(1 * time.Second)

	t.Log(numReads1, "---")
	t.Log(numReads2, "---")

	if numReads1 != n*2 {
		t.Fatalf("read1 count was %d, expected %d", numReads1, n*2)
	}
	if numReads2 != n*2 {
		t.Fatalf("read2 count was %d, expected %d", numReads2, n*2)
	}
}
