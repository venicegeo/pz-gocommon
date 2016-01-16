package piazza

import (
	"github.com/Shopify/sarama"
)

//===========================================================================

const offsetNewest int64 = sarama.OffsetNewest

//===========================================================================

// Kafka represents a running kafka server. It is used to create
// Consumer and Producer objects.
type Kafka struct {
	host string
}

// NewProducer returns a new Producer that can have messages sent to it.
func (k *Kafka) NewProducer() (*Producer, error) {

	w := new(Producer)

	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = false

	producer, err := sarama.NewAsyncProducer([]string{k.host}, config)
	if err != nil {
		return nil, err
	}

	w.producer = producer
	return w, nil
}

// NewConsumer returns a new Consumer that can be used to create a ParitionConsumer
// that can receive messages.
func (k *Kafka) NewConsumer() (*Consumer, error) {
	r := new(Consumer)

	consumer, err := sarama.NewConsumer([]string{k.host}, nil)
	if err != nil {
		return nil, err
	}

	r.consumer = consumer

	return r, nil
}

//===========================================================================

// A Producer is an object that can send messages.
type Producer struct {
	producer sarama.AsyncProducer
}

// Close shuts down the Producer.
func (w *Producer) Close() error {
	return w.producer.Close()
}

// NewMessage creates a message that can be sent to a queue.
// TODO: don't want to expose sarama message type, either on send or recv
func NewMessage(topic string, data string) *sarama.ProducerMessage {
	m := &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(data)}
	return m
}

// Input returns a channel that can have messages sent to it.
func (w *Producer) Input() chan<- *sarama.ProducerMessage {
	return w.producer.Input()
}

/*
// Success returns a channel that can have messages sent to it, for passing success codes to the listener
func (w *Producer) Successes() <-chan *sarama.ProducerMessage {
	return w.producer.Successes()
}

// Errors returns a channel that can have messages sent to it, for passing failure codes to the listener
func (w *Producer) Errors() <-chan *sarama.ProducerError {
	return w.producer.Errors()
}
*/

//===========================================================================

// Consumer is an object that can receive messages (indirectly, via PartitionConsumer).
type Consumer struct {
	consumer sarama.Consumer
}

// ConsumePartition returns a PartitionConsumer for the given topic.
func (r *Consumer) ConsumePartition(topic string, partition int32, offset int64) (*PartitionConsumer, error) {
	spc, err := r.consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		return nil, err
	}
	pc := new(PartitionConsumer)
	pc.partitionConsumer = spc
	return pc, nil
}

// Close shuts down the Consumer.
func (r *Consumer) Close() error {
	return r.consumer.Close()
}

//===========================================================================

// PartitionConsumer is an object that can receive messages.
type PartitionConsumer struct {
	partitionConsumer sarama.PartitionConsumer
}

// Close shuts down the PartitionConsumer.
func (pc *PartitionConsumer) Close() error {
	return pc.partitionConsumer.Close()
}

// Messages returns a channel that messages can be read from.
func (pc *PartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return pc.partitionConsumer.Messages()
}
