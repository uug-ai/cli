//go:build integration

package actions

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRabbitMQDLQCLIEndToEnd(t *testing.T) {
	host := os.Getenv("RABBITMQ_INTEGRATION_HOST")
	if host == "" {
		t.Skip("RABBITMQ_INTEGRATION_HOST is not set")
	}
	username := envValue("RABBITMQ_INTEGRATION_USERNAME", "guest")
	password := envValue("RABBITMQ_INTEGRATION_PASSWORD", "guest")
	connection, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", username, password, host))
	if err != nil {
		t.Fatalf("connect RabbitMQ: %v", err)
	}
	defer connection.Close()
	channel, err := connection.Channel()
	if err != nil {
		t.Fatalf("open RabbitMQ channel: %v", err)
	}
	defer channel.Close()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	deadLetterQueue := "test-dlq-" + suffix
	monitorQueue := "test-monitor-" + suffix
	analysisQueue := "test-analysis-" + suffix
	queues := []string{deadLetterQueue, monitorQueue, analysisQueue}
	for _, name := range queues {
		if _, err := channel.QueueDeclare(name, true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
			t.Fatalf("declare queue %q: %v", name, err)
		}
	}
	defer func() {
		for _, name := range queues {
			if _, err := channel.QueueDelete(name, false, false, false); err != nil {
				t.Errorf("delete queue %q: %v", name, err)
			}
		}
	}()

	connectionArgs := []string{
		"--provider", "rabbitmq",
		"--dead-letter", deadLetterQueue,
		"--rabbitmq-host", host,
		"--rabbitmq-username", username,
		"--rabbitmq-password", password,
		"--timeout", "20s",
	}
	seedOutput := runDLQForTest(t, append([]string{"seed"},
		append(connectionArgs,
			"--sources", monitorQueue+","+analysisQueue,
			"--count", "6",
			"--execute",
		)...,
	))
	if !strings.Contains(seedOutput, "Published: 6") {
		t.Fatalf("seed output = %q", seedOutput)
	}

	inspectOutput := runDLQForTest(t, append([]string{"inspect"},
		append(connectionArgs, "--limit", "10")...,
	))
	for _, expected := range []string{monitorQueue + "   3", analysisQueue + "  3", "TOTAL", "6"} {
		if !strings.Contains(inspectOutput, expected) {
			t.Fatalf("inspect output %q does not contain %q", inspectOutput, expected)
		}
	}

	dryRunOutput := runDLQForTest(t, append([]string{"replay"},
		append(connectionArgs, "--limit", "10")...,
	))
	if !strings.Contains(dryRunOutput, "Planned: 6") || !strings.Contains(dryRunOutput, "Replayed: 0") {
		t.Fatalf("dry-run output = %q", dryRunOutput)
	}

	replayOutput := runDLQForTest(t, append([]string{"replay"},
		append(connectionArgs, "--limit", "10", "--execute")...,
	))
	if !strings.Contains(replayOutput, "Replayed: 6") {
		t.Fatalf("replay output = %q", replayOutput)
	}
	if countRabbitMessages(t, channel, monitorQueue) != 3 || countRabbitMessages(t, channel, analysisQueue) != 3 {
		t.Fatal("replayed messages were not distributed to their recorded source queues")
	}

	emptyOutput := runDLQForTest(t, append([]string{"inspect"},
		append(connectionArgs, "--limit", "10")...,
	))
	if !strings.Contains(emptyOutput, "Scanned: 0") {
		t.Fatalf("final inspect output = %q", emptyOutput)
	}
}

func runDLQForTest(t *testing.T, args []string) string {
	t.Helper()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if exitCode := RunDLQ(args, &stdout, &stderr); exitCode != 0 {
		t.Fatalf("RunDLQ(%v) exit=%d stderr=%q", args, exitCode, stderr.String())
	}
	return stdout.String()
}

func countRabbitMessages(t *testing.T, channel *amqp.Channel, queueName string) int {
	t.Helper()
	count := 0
	for {
		delivery, ok, err := channel.Get(queueName, true)
		if err != nil {
			t.Fatalf("read queue %q: %v", queueName, err)
		}
		if !ok {
			return count
		}
		if !strings.Contains(string(delivery.Body), `"synthetic":true`) {
			t.Fatalf("unexpected replay payload on %q: %s", queueName, delivery.Body)
		}
		count++
	}
}
