//go:build dynamic

package queue

import (
	"bytes"
	"crypto/x509"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestKafkaNativeMockRoundTrip(t *testing.T) {
	if os.Getenv("CLI_RUNTIME_FIXTURE") == "1" && (os.Getuid() != 65532 || os.Getgid() != 65532) {
		t.Fatal("runtime fixture must execute as UID/GID 65532")
	}
	caBundle, err := os.ReadFile("/etc/ssl/certs/ca-certificates.crt")
	if err != nil {
		t.Fatal(err)
	}
	if !x509.NewCertPool().AppendCertsFromPEM(caBundle) {
		t.Fatal("runtime CA bundle contains no parseable certificates")
	}
	if kafka.LibrdkafkaLinkInfo != "dynamically linked to librdkafka" {
		t.Fatal(kafka.LibrdkafkaLinkInfo)
	}
	_, version := kafka.LibraryVersion()
	if !strings.HasPrefix(version, "2.16.0") {
		t.Fatalf("unexpected packaged librdkafka version %q", version)
	}
	t.Logf("%s: %s", kafka.LibrdkafkaLinkInfo, version)

	cluster, err := kafka.NewMockCluster(1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  cluster.BootstrapServers(),
		"security.protocol":  "PLAINTEXT",
		"builtin.features":   "gzip,snappy,ssl,sasl,lz4,sasl_plain,sasl_scram,sasl_oauthbearer,zstd,http,oidc",
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
