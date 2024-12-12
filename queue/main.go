package queue

import (
	"fmt"
	"os"

	models "github.com/uug-ai/cli/models"
)

type Payload struct {
	Events             []string `json:"events,omitempty"`
	Provider           string   `json:"provider,omitempty"`
	SecondaryProviders []string `json:"secondary_providers,omitempty"`
	Source             string   `json:"source,omitempty"`
	Request            string   `json:"request,omitempty"`
	FileName           string   `json:"fileName,omitempty"`
	Payload            Media    `json:"payload,omitempty"`
	ReceiveCount       int64    `json:"receivecount,omitempty"`
	Date               int64    `json:"date,omitempty"`

	// If source is AWS, we got some other info.
	Bucket string `json:"bucket,omitempty"`
	Region string `json:"region,omitempty"`

	// For async queue's (e.g. analysis)
	Operation string                 `json:"operation,omitempty"`
	Data      map[string]interface{} `json:"data,omitempty"`
}

type Media struct {
	FileName         string                    `json:"key,omitempty"`
	FileSize         int64                     `json:"fileSize,omitempty"`
	MetaData         MetaData                  `json:"metadata,omitempty"`
	BytesRanges      string                    `json:"bytes_ranges" bson:"bytes_ranges"`
	BytesRangeOnTime []models.BytesRangeOnTime `json:"bytes_range_on_time" bson:"bytes_range_on_time"`
	IsFragmented     bool                      `json:"is_fragmented" bson:"is_fragmented"`
}

type MetaData struct {
	Capture           string `json:"capture,omitempty"`
	Token             string `json:"event-token,omitempty"`
	NumberOfChanges   string `json:"event-numberofchanges,omitempty"`
	UploadTime        string `json:"uploadtime,omitempty"`
	MicroSeconds      string `json:"event-microseconds,omitempty"`
	ProductId         string `json:"productid,omitempty"`
	InstanceName      string `json:"event-instancename,omitempty"`
	RegionCoordinates string `json:"event-regioncoordinates,omitempty"`
	Timestamp         string `json:"event-timestamp,omitempty"`
	PublicKey         string `json:"publickey,omitempty"`
}

type Queue interface {
	ReadEventsForever()
	ReadMessagesForever(handleMessage func(Payload) (string, int), sync ...bool)
	SendMessageFifo(queueName string, bytes []byte, deduplicationId string)
	SendMessage(queueName string, payload string, delay int)
	Close()
	Test(queueName string) error
}

func CreateQueue(queue_system string, connection models.Queue) (Queue, error) {
	var q Queue
	var err error
	if queue_system == "sqs" {
		q, err = CreateSQSQueue(connection, 20, 10, 5) // Timeout, NumberOfMessages, Visibility
	}
	if queue_system == "kafka" {
		q, err = CreateKafkaQueue(connection)
	}
	if queue_system == "rabbitmq" {
		q, err = CreateRabbitMQ(connection)
	}
	if queue_system == "kerberoscloud" {
		q, err = CreateKerberosCloudQueue(connection)
	}
	if queue_system == "kerberosvault" {
		q, err = CreateKerberosVault(connection)
	}
	return q, err
}

func CreateTestQueue(queue_system string, connection models.Queue) (Queue, error) {
	var q Queue
	var err error
	if queue_system == "sqs" {
		q, err = CreateSQSQueue(connection, 20, 10, 5) // Timeout, NumberOfMessages, Visibility
	}
	if queue_system == "kafka" {
		q, err = CreateKafkaAdminQueue(connection)
	}
	if queue_system == "rabbitmq" {
		q, err = CreateRabbitMQ(connection)
	}
	if queue_system == "kerberoscloud" {
		q, err = CreateKerberosCloudQueue(connection)
	}
	if queue_system == "kerberosvault" {
		q, err = CreateKerberosVault(connection)
	}
	return q, err
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
