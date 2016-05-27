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
)

const kafkaHost = "localhost:9092"

var topicName = "test.topic."

var OffsetNewest int64

var kafka *Kafka

func doReads(t *testing.T, numReads *int) {
	c, err := kafka.NewConsumer()
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	partitionConsumer, err := c.ConsumePartition(topicName, 0, OffsetNewest)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	for {
		msg := <-partitionConsumer.Messages()
		t.Logf("Consumed: offset:%d  value:%v", msg.Offset, string(msg.Value))
		*numReads++
	}

	//t.Logf("Reader done: %d", *numReads)
}

func doWrites(t *testing.T, id int, count int) {

	p, err := kafka.NewProducer()
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// TODO: handle "err := <-w.Errors():"

	for n := 0; n < count; n++ {
		p.Input() <- NewMessage(topicName, fmt.Sprintf("mssg %d from %d", n, id))
	}

	t.Logf("Writer done: %d", count)
}

func TestKafka(t *testing.T) {
	kafka = &Kafka{host: kafkaHost}

	rand.Seed(int64(time.Now().Nanosecond()))
	topicName += fmt.Sprintf("%x", rand.Uint32())
	log.Printf("topic: %s", topicName)

	var numReads1, numReads2 int

	go doReads(t, &numReads1)
	go doReads(t, &numReads2)

	n := 3
	go doWrites(t, 1, n)
	go doWrites(t, 2, n)

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
