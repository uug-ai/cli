module github.com/uug-ai/cli

go 1.24.4

toolchain go1.24.9

replace github.com/uug-ai/models => ../models

require (
	github.com/aws/aws-sdk-go v1.55.5
	github.com/confluentinc/confluent-kafka-go/v2 v2.6.1
	github.com/gosuri/uiprogress v0.0.1
	github.com/rabbitmq/amqp091-go v1.10.0
	github.com/uug-ai/models v1.1.3
	go.mongodb.org/mongo-driver v1.17.4
	golang.org/x/crypto v0.31.0
	gopkg.in/mgo.v2 v2.0.0-20190816093944-a6b53ec6cb22
)

require (
	github.com/golang/snappy v0.0.4 // indirect
	github.com/gosuri/uilive v0.0.4 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/montanaflynn/stats v0.7.1 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	golang.org/x/sync v0.10.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/text v0.21.0 // indirect
)
