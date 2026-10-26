package queue

import (
	"bytes"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestKafkaNativeMockRoundTrip(t *testing.T) {
	cluster, err := kafka.NewMockCluster(1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  cluster.BootstrapServers(),
		"security.protocol":  "PLAINTEXT",
		"message.timeout.ms": 5000,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	topic := "cli-native-fixture"
	payload := []byte("native-librdkafka-round-trip")
	delivery := make(chan kafka.Event, 1)
	if err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          payload,
	}, delivery); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-delivery:
		message, ok := event.(*kafka.Message)
		if !ok {
			t.Fatalf("unexpected delivery event %T", event)
		}
		if message.TopicPartition.Error != nil {
			t.Fatal(message.TopicPartition.Error)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("mock Kafka delivery timed out")
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  cluster.BootstrapServers(),
		"security.protocol":  "PLAINTEXT",
		"group.id":           "cli-native-fixture",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	if err := consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		t.Fatal(err)
	}
	message, err := consumer.ReadMessage(15 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(message.Value, payload) {
		t.Fatalf("received %q, want %q", message.Value, payload)
	}
}
