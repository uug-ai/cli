package queue

import (
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	models "github.com/uug-ai/cli/models"
)

// RabbitMQ struct
type RabbitMQ struct {
	// RabbitMQ connection
	conn *amqp.Connection
	// RabbitMQ channel for producing
	chP *amqp.Channel
	// RabbitMQ channel for consuming
	chC *amqp.Channel
	// RabbitMQ queue
	q amqp.Queue
	// RabbitMQ exchange
	exchange string
	// RabbitMQ routing key
	routingKey string
	// RabbitMQ queue name
	queueName string
	// RabbitMQ connection string
	connString string
}

// NewRabbitMQ creates a new RabbitMQ instance
func CreateRabbitMQ(connection models.Queue) (*RabbitMQ, error) {
	exchange := connection.Exchange
	queueName := connection.Topic
	username := connection.Username
	password := connection.Password
	host := connection.Broker
	connectionString := ""

	// Check if host containers aqmps:// or amqp://
	hasProtocol := false
	protocol := ""
	if len(host) > 7 {
		if host[:8] == "amqps://" {
			hasProtocol = true
			protocol = "amqps://"
			host = host[8:]
		} else if host[:7] == "amqp://" {
			hasProtocol = true
			protocol = "amqp://"
			host = host[7:]
		}
	}

	if host != "" && username != "" && password != "" {
		if hasProtocol {
			connectionString = protocol + username + ":" + password + "@" + host + "/"
		} else {
			connectionString = "amqp://" + username + ":" + password + "@" + host + "/"
		}
	}

	r := &RabbitMQ{
		exchange:   exchange,  //"amd.direct",
		routingKey: queueName, //"kcloud-monitor-queue",
		queueName:  queueName, //"kcloud-monitor-queue",
		connString: connectionString,
	}
	//fmt.Println("RabbitMQ connection string: " + connectionString)
	err := r.Connect()
	if err != nil {
		//fmt.Println("RabbitMQ connection error: " + err.Error())
		return r, errors.New("RabbitMQ connection error: " + err.Error())
	} else {
		//fmt.Println("RabbitMQ connection success")
	}
	return r, err
}

// Connect to RabbitMQ
func (r *RabbitMQ) Connect() error {
	var err error
	r.conn, err = amqp.Dial(r.connString)
	if err != nil {
		return err
	}

	// Create channel for producing
	r.chP, err = r.conn.Channel()
	if err != nil {
		return err
	}

	// Create channel for consuming
	r.chC, err = r.conn.Channel()
	if err != nil {
		return err
	}

	// Check if queue exists
	_, err = r.chC.QueueInspect(r.queueName)
	if err != nil {
		fmt.Println("RabbitMQ: Queue does not exist")
		// Declare queue, if it doesn't exist already.
		r.q, err = r.chP.QueueDeclare(
			r.queueName, // name
			true,        // durable
			false,       // delete when unused
			false,       // exclusive
			false,       // no-wait
			amqp.Table{
				"x-queue-type": "quorum",
			}, // arguments
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r RabbitMQ) ReadMessagesForever(handleMessage func(Payload) (string, int), sync ...bool) {

}

func (r RabbitMQ) ReadEventsForever() {

}

func (r RabbitMQ) SendMessage(queueName string, payload string, delay int) {

	// Publish message to the queue
	err := r.chP.Publish(
		r.exchange, // using the nameless exchange, which will route the message to the queue with the name specified by routingKey.
		queueName,  // routing key == queue name
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(payload),
		})

	if err != nil {
		//	fmt.Printf("[Error - Send message] \n%v \n\n", err.Error())
	}

	//fmt.Printf("[Send message - RabbitMQ] \n%v \n\n", queueName)
}

func (r RabbitMQ) SendMessageFifo(queueName string, bytes []byte, deduplicationId string) {
	r.SendMessage(queueName, string(bytes), 0)
}

func (r RabbitMQ) Test(queueName string) error {
	var err error

	// Check if connection is established
	if r.conn == nil {
		err = r.Connect()
	}

	if r.conn.IsClosed() {
		err = errors.New("Connection is closed")
	}

	return err
}

func (r RabbitMQ) Close() {
	r.chP.Close()
	r.chC.Close()
	r.conn.Close()
}
