package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	models "github.com/uug-ai/cli/models"
)

type KafkaQueue struct {
	Name         string
	Consumer     *kafka.Consumer
	Producer     *kafka.Producer
	AdminClient  *kafka.AdminClient
	DeliveryChan chan kafka.Event
}

func CreateKafkaQueue(connection models.Queue) (*KafkaQueue, error) {
	kafka_broker := connection.Broker
	kafka_username := connection.Username
	kafka_password := connection.Password
	kafka_mechanism := connection.Mechanism
	kafka_security := connection.Security
	configMap := kafka.ConfigMap{
		"bootstrap.servers":  kafka_broker,
		"group.id":           connection.Group,
		"session.timeout.ms": 10000,
		"auto.offset.reset":  "earliest",
		"sasl.mechanisms":    kafka_mechanism, //"PLAIN",
		"security.protocol":  kafka_security,  //"SASL_PLAINTEXT",
		"sasl.username":      kafka_username,
		"sasl.password":      kafka_password,
	}
	c, err := kafka.NewConsumer(&configMap)
	p, err := kafka.NewProducer(&configMap)
	deliveryChan := make(chan kafka.Event)

	return &KafkaQueue{
		Name:         connection.Topic,
		Consumer:     c,
		Producer:     p,
		DeliveryChan: deliveryChan,
	}, err
}

func CreateKafkaAdminQueue(connection models.Queue) (*KafkaQueue, error) {
	kafka_broker := connection.Broker
	kafka_username := connection.Username
	kafka_password := connection.Password
	kafka_mechanism := connection.Mechanism
	kafka_security := connection.Security
	configMap := kafka.ConfigMap{
		"bootstrap.servers":  kafka_broker,
		"group.id":           connection.Group,
		"session.timeout.ms": 10000,
		"auto.offset.reset":  "earliest",
		"sasl.mechanisms":    kafka_mechanism, //"PLAIN",
		"security.protocol":  kafka_security,  //"SASL_PLAINTEXT",
		"sasl.username":      kafka_username,
		"sasl.password":      kafka_password,
	}
	a, err := kafka.NewAdminClient(&configMap)

	return &KafkaQueue{
		Name:        connection.Topic,
		AdminClient: a,
	}, err
}

func (q KafkaQueue) ReadMessagesForever(handleMessage func(Payload) (string, int), sync ...bool) {
	q.Consumer.SubscribeTopics([]string{q.Name}, nil)

	isSync := true
	if len(sync) > 0 {
		isSync = sync[0]
	}

	for {
		msg, err := q.Consumer.ReadMessage(-1)
		if err == nil {
			var payload Payload
			json.Unmarshal(msg.Value, &payload)
			fmt.Printf("Message on %s: %+v\n", msg.TopicPartition, payload)
			result, _ := handleMessage(payload)

			if isSync {
				if result == "forward" {
					// Shift event
					payload.Events = payload.Events[1:]
					bytes, _ := json.Marshal(payload)
					topic := "kcloud-event-queue"
					q.Producer.Produce(&kafka.Message{
						TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
						Value:          bytes,
					}, q.DeliveryChan)
					_ = <-q.DeliveryChan //wait
				} else if result == "cancel" {
					// todo, if needed ;)
				} else if result == "retry" {
					// todo, if needed ;)
				}
			}
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}

func (q KafkaQueue) ReadEventsForever() {
	q.Consumer.SubscribeTopics([]string{q.Name}, nil)
	for {
		msg, err := q.Consumer.ReadMessage(-1)
		if err == nil {
			var payload Payload
			json.Unmarshal(msg.Value, &payload)
			fmt.Printf("Message on %s: %+v\n", msg.TopicPartition, payload)

			if len(payload.Events) > 0 {
				theQueue := payload.Events[0]
				topic := "kcloud-" + theQueue + "-queue"
				q.Producer.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
					Value:          msg.Value,
				}, q.DeliveryChan)
				_ = <-q.DeliveryChan //wait
			}
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}

func (q KafkaQueue) Test(queueName string) error {
	ctx, cancel := context.WithCancel(context.Background())
	ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Create topics on cluster.
	results, err := q.AdminClient.CreateTopics(
		ctxTimeout,
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		[]kafka.TopicSpecification{{
			Topic:             "test",
			NumPartitions:     1,
			ReplicationFactor: 1}})

	for _, result := range results {
		fmt.Printf("%s\n", result)
	}

	q.AdminClient.Close()

	return err
}

func (q KafkaQueue) SendMessage(queueName string, payload string, delay int) {
	q.Producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &queueName, Partition: kafka.PartitionAny},
		Value:          []byte(payload),
	}, q.DeliveryChan)
	_ = <-q.DeliveryChan //wait
	fmt.Printf("[Send message] \n%v \n\n", queueName)
}

func (q KafkaQueue) SendMessageFifo(queueName string, bytes []byte, deduplicationId string) {
	q.Producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &queueName, Partition: kafka.PartitionAny},
		Value:          bytes,
	}, q.DeliveryChan)
	_ = <-q.DeliveryChan //wait
	fmt.Printf("[Send message] \n%v \n\n", queueName)
}

func (q KafkaQueue) Close() {
	q.Producer.Close()
	q.Consumer.Close()
	close(q.DeliveryChan)
}
