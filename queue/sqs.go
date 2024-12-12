package queue

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	models "github.com/uug-ai/cli/models"
)

type SQSQueue struct {
	Name             string
	Url              *string
	EventName        string
	Connection       *sqs.SQS
	Timeout          int64
	NumberOfMessages int64
	Visibility       int64
}

func CreateSQSQueue(connection models.Queue, timeout int64, numberofmessages int64, visibility int64) (*SQSQueue, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(connection.Region),
		Credentials: credentials.NewStaticCredentials(connection.AccessKey, connection.Secret, ""),
	})

	sqsQueue := sqs.New(sess)

	url, err := sqsQueue.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(connection.Topic),
	})

	eventName := connection.Topic

	return &SQSQueue{
		Name:             connection.Topic,
		Connection:       sqsQueue,
		Url:              url.QueueUrl,
		EventName:        eventName,
		Timeout:          timeout,
		NumberOfMessages: numberofmessages,
		Visibility:       visibility,
	}, err
}

func (q SQSQueue) ReadMessagesForever(handleMessage func(Payload) (string, int), sync ...bool) {

	isSync := true
	if len(sync) > 0 {
		isSync = sync[0]
	}

	if q.Url != nil {
		// Keep reading from queue... until we die :o
		for {
			result, err := q.Connection.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueUrl:              q.Url,
				AttributeNames:        aws.StringSlice([]string{"SentTimestamp", "ApproximateReceiveCount"}),
				MaxNumberOfMessages:   aws.Int64(q.NumberOfMessages),
				MessageAttributeNames: aws.StringSlice([]string{"All"}),
				WaitTimeSeconds:       aws.Int64(q.Timeout),
				VisibilityTimeout:     aws.Int64(q.Visibility),
			})

			if err != nil {
				exitErrorf("Unable to receive message from queue %q, %v.", q.Name, err)
			}

			fmt.Printf("Received %d messages.\n", len(result.Messages))
			if len(result.Messages) > 0 {

				if len(result.Messages) > 0 {

					if !isSync {
						// Remove all messages
						for _, message := range result.Messages {
							q.DeleteMessage(q.Url, message.ReceiptHandle)
						}
					}

					for _, message := range result.Messages {
						var payload Payload
						if err := json.Unmarshal([]byte(*message.Body), &payload); err != nil {
							panic(err)
						}

						payload.ReceiveCount, _ = strconv.ParseInt((*message.Attributes["ApproximateReceiveCount"]), 10, 64)

						result, delay := handleMessage(payload)

						if isSync {
							if result == "forward" {
								// Shift event
								payload.Events = payload.Events[1:]
								bytes, _ := json.Marshal(payload)
								q.SendMessage(q.EventName, string(bytes), delay)
								q.DeleteMessage(q.Url, message.ReceiptHandle)
							} else if result == "cancel" {
								// Do nothing, simply remove the message from the queue
								q.DeleteMessage(q.Url, message.ReceiptHandle)

							} else if result == "retry" {
								bytes, _ := json.Marshal(payload)
								q.SendMessage(q.EventName, string(bytes), delay)
								q.DeleteMessage(q.Url, message.ReceiptHandle)
							}
						}
					}
				}
			}
		}
	}
}

func (q SQSQueue) ReadEventsForever() {
	if q.Url != nil {
		// Keep reading from queue... until we die :o
		for {
			result, err := q.Connection.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueUrl:              q.Url,
				AttributeNames:        aws.StringSlice([]string{"SentTimestamp"}),
				MaxNumberOfMessages:   aws.Int64(q.NumberOfMessages),
				MessageAttributeNames: aws.StringSlice([]string{"All"}),
				WaitTimeSeconds:       aws.Int64(q.Timeout),
				VisibilityTimeout:     aws.Int64(q.Visibility),
			})

			if err != nil {
				exitErrorf("Unable to receive message from queue %q, %v.", q.Name, err)
			}

			fmt.Printf("Received %d messages.\n", len(result.Messages))
			if len(result.Messages) > 0 {

				for _, message := range result.Messages {

					var payload Payload
					if err := json.Unmarshal([]byte(*message.Body), &payload); err != nil {
						panic(err)
					}

					if len(payload.Events) > 0 {
						theQueue := payload.Events[0]
						bytes, _ := json.Marshal(payload)
						q.SendMessage("kcloud-"+theQueue+"-queue", string(bytes), 0)
					}

					q.DeleteMessage(q.Url, message.ReceiptHandle)
				}
			}
		}

	}
}

func (q SQSQueue) Test(queueName string) error {
	_, err := q.Connection.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	return err
}

func (q SQSQueue) SendMessage(queueName string, payload string, delay int) {
	url, _ := q.Connection.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	send_params := &sqs.SendMessageInput{
		MessageBody:  aws.String(payload),
		QueueUrl:     url.QueueUrl,
		DelaySeconds: aws.Int64(int64(delay)),
	}
	send_resp, err := q.Connection.SendMessage(send_params)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("[Send message] \n%v \n\n", send_resp)
}

func (q SQSQueue) DeleteMessage(queueUrl *string, receiptHandle *string) {
	resultDelete, err := q.Connection.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      queueUrl,
		ReceiptHandle: receiptHandle,
	})
	if err != nil {
		fmt.Println("Delete Error", err)
		return
	}
	fmt.Printf("[Message Deleted] \n%v \n\n", resultDelete)
}

func (q SQSQueue) SendMessageFifo(queueName string, bytes []byte, deduplicationId string) {
	url, _ := q.Connection.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	send_params := &sqs.SendMessageInput{
		MessageBody:            aws.String(string(bytes)),
		QueueUrl:               url.QueueUrl,
		MessageGroupId:         aws.String("fifo"),
		MessageDeduplicationId: aws.String(deduplicationId),
	}
	q.Connection.SendMessage(send_params)

	fmt.Printf("[Send message] \n%v \n\n", queueName)
}

func (q SQSQueue) Close() {

}
