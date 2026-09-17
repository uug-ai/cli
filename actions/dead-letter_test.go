package actions

import (
	"bytes"
	"context"
	"flag"
	"strings"
	"testing"
	"time"

	sharedqueue "github.com/uug-ai/queue/pkg/queue"
)

type fakeDeadLetterAdmin struct {
	metadata []sharedqueue.DeadLetterMetadata
	payloads [][]byte
}

func (*fakeDeadLetterAdmin) InspectDeadLetters(context.Context, sharedqueue.DeadLetterInspectRequest) (sharedqueue.DeadLetterInspectResult, error) {
	return sharedqueue.DeadLetterInspectResult{}, nil
}

func (*fakeDeadLetterAdmin) ReplayDeadLetters(context.Context, sharedqueue.DeadLetterReplayRequest) (sharedqueue.DeadLetterReplayResult, error) {
	return sharedqueue.DeadLetterReplayResult{}, nil
}

func (f *fakeDeadLetterAdmin) PublishDeadLetter(_ context.Context, payload []byte, metadata sharedqueue.DeadLetterMetadata) error {
	f.payloads = append(f.payloads, append([]byte(nil), payload...))
	f.metadata = append(f.metadata, metadata)
	return nil
}

func TestParseReplayDefaultsToDryRun(t *testing.T) {
	config, err := parseDLQFlags("replay", []string{
		"--provider", "rabbitmq",
		"--dead-letter", "deadletter",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseDLQFlags: %v", err)
	}
	if config.execute {
		t.Fatal("replay must default to dry-run")
	}
}

func TestDLQHelpDoesNotExposeEnvironmentSecrets(t *testing.T) {
	secrets := []string{"rabbit-value-92", "kafka-value-47", "azure-value-31", "token-value-68", "vault-access-17", "vault-secret-53", "secret-vault-host"}
	t.Setenv("RABBITMQ_PASSWORD", secrets[0])
	t.Setenv("KAFKA_PASSWORD", secrets[1])
	t.Setenv("AZURE_EVENTHUB_CONNECTION_STRING", secrets[2])
	t.Setenv("SQS_SESSION_TOKEN", secrets[3])
	t.Setenv("KERBEROS_STORAGE_ACCESS_KEY", secrets[4])
	t.Setenv("KERBEROS_STORAGE_SECRET", secrets[5])
	t.Setenv("KERBEROS_STORAGE_URI", "https://"+secrets[6]+"/api")

	var output bytes.Buffer
	_, err := parseDLQFlags("recover", []string{"--help"}, &output)
	if err != flag.ErrHelp {
		t.Fatalf("parseDLQFlags error = %v, want flag.ErrHelp", err)
	}

	for _, secret := range secrets {
		if strings.Contains(output.String(), secret) {
			t.Fatalf("help output exposed secret %q", secret)
		}
	}
}

func TestParseRecoveryBatchFlags(t *testing.T) {
	config, err := parseDLQFlags("recover", []string{
		"--provider", "rabbitmq",
		"--dead-letter", "deadletter",
		"--destination", "kcloud-event-queue",
		"--limit", "30000",
		"--batch-size", "500",
		"--batch-delay", "2s",
		"--historical-tail-max-age", "30m",
		"--legacy-user-ownership",
		"--debug",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseDLQFlags: %v", err)
	}
	if config.execute || config.limit != 30000 || config.batchSize != 500 ||
		config.batchDelay != 2*time.Second || config.historicalTailMaxAge != 30*time.Minute ||
		config.allowHistoricalTail || !config.legacyUserOwnership || !config.debug {
		t.Fatalf("config = %+v", config)
	}
	if err := validateRecoveryConfig(config); err != nil {
		t.Fatalf("validateRecoveryConfig: %v", err)
	}
}

func TestParseDLQFlagsLoadsSecretsFromEnvironment(t *testing.T) {
	t.Setenv("RABBITMQ_PASSWORD", "rabbit-secret")
	t.Setenv("KAFKA_PASSWORD", "kafka-secret")
	t.Setenv("AZURE_EVENTHUB_CONNECTION_STRING", "azure-secret")
	t.Setenv("SQS_SESSION_TOKEN", "sqs-secret")

	config, err := parseDLQFlags("inspect", nil, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseDLQFlags: %v", err)
	}
	if config.rabbitPassword != "rabbit-secret" ||
		config.kafkaPassword != "kafka-secret" ||
		config.azureConnectionString != "azure-secret" ||
		config.sqsSessionToken != "sqs-secret" {
		t.Fatal("environment secrets were not loaded")
	}
}

func TestPrintInspectionGroupsSources(t *testing.T) {
	var output bytes.Buffer
	printInspection(&output, sharedqueue.DeadLetterInspectResult{
		Scanned: 3,
		Matched: 3,
		Legacy:  1,
		Groups: map[string]sharedqueue.DeadLetterGroup{
			"monitor": {
				Source: "monitor",
				Count:  2,
				Oldest: time.Date(2026, 9, 15, 1, 0, 0, 0, time.UTC),
				Newest: time.Date(2026, 9, 16, 1, 0, 0, 0, time.UTC),
			},
			sharedqueue.UnknownSourceQueue: {
				Source: sharedqueue.UnknownSourceQueue,
				Count:  1,
			},
		},
	})
	text := output.String()
	for _, expected := range []string{"SOURCE QUEUE", "monitor", "unknown", "TOTAL", "Legacy/unknown: 1"} {
		if !strings.Contains(text, expected) {
			t.Fatalf("output %q does not contain %q", text, expected)
		}
	}
}

func TestPrintReplayGroupsDestinations(t *testing.T) {
	var output bytes.Buffer
	printReplay(&output, sharedqueue.DeadLetterReplayResult{
		Scanned:  3,
		Matched:  3,
		Planned:  3,
		Retained: 3,
		Destinations: map[string]int{
			"kcloud-sequence-queue": 1,
			"kcloud-event-queue":    2,
		},
	}, false)
	text := output.String()
	for _, expected := range []string{
		"Mode: dry-run",
		"REPLAY DESTINATION",
		"kcloud-event-queue",
		"kcloud-sequence-queue",
		"Planned: 3",
		"No messages were moved",
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("output %q does not contain %q", text, expected)
		}
	}
	if strings.Index(text, "kcloud-event-queue") > strings.Index(text, "kcloud-sequence-queue") {
		t.Fatalf("destinations are not sorted: %q", text)
	}
}

func TestSeedDeadLettersDryRunGroupsSources(t *testing.T) {
	result, err := seedDeadLetters(context.Background(), nil, dlqCommandConfig{
		deadLetterQueue: "deadletter",
		sources:         "monitor, analysis,monitor",
		count:           5,
		reason:          string(sharedqueue.DeadLetterReasonHandlerError),
	})
	if err != nil {
		t.Fatalf("seedDeadLetters: %v", err)
	}
	if result.Planned != 5 || result.Published != 0 || result.BySource["monitor"] != 3 || result.BySource["analysis"] != 2 {
		t.Fatalf("seed result = %+v", result)
	}
}

func TestSeedDeadLettersPublishesThroughAdministrativeAPI(t *testing.T) {
	publisher := &fakeDeadLetterAdmin{}
	result, err := seedDeadLetters(context.Background(), publisher, dlqCommandConfig{
		deadLetterQueue: "deadletter",
		sources:         "monitor,analysis",
		count:           3,
		reason:          string(sharedqueue.DeadLetterReasonHandlerError),
		execute:         true,
	})
	if err != nil {
		t.Fatalf("seedDeadLetters: %v", err)
	}
	if result.Published != 3 || len(publisher.metadata) != 3 || len(publisher.payloads) != 3 {
		t.Fatalf("seed result=%+v metadata=%d payloads=%d", result, len(publisher.metadata), len(publisher.payloads))
	}
	if publisher.metadata[0].Source != "monitor" || publisher.metadata[1].Source != "analysis" ||
		publisher.metadata[2].Source != "monitor" {
		t.Fatalf("published metadata = %+v", publisher.metadata)
	}
}

func TestNewKafkaDLQOptionsDisablesAutoTopicCreation(t *testing.T) {
	options := newKafkaDLQOptions(dlqCommandConfig{
		deadLetterQueue: "deadletter",
		kafkaBroker:     "kafka:9092",
		kafkaGroupID:    "dlq-admin",
	})
	if !options.DisableAutoTopicCreation {
		t.Fatal("DLQ administrative Kafka clients must disable automatic topic creation")
	}
}

func TestNewDLQClientRequiresExistingRabbitQueues(t *testing.T) {
	client, err := newDLQClient(dlqCommandConfig{
		provider:          "rabbitmq",
		deadLetterQueue:   "deadletter",
		rabbitHost:        "rabbitmq:5672",
		rabbitUsername:    "guest",
		rabbitPassword:    "guest",
		rabbitVirtualHost: "tenant",
	})
	if err != nil {
		t.Fatalf("newDLQClient: %v", err)
	}
	options, ok := client.Options.(*sharedqueue.RabbitOptions)
	if !ok {
		t.Fatalf("options type = %T, want *queue.RabbitOptions", client.Options)
	}
	if !options.RequireExistingQueues {
		t.Fatal("DLQ administrative RabbitMQ clients must not declare missing queues")
	}
	if options.VirtualHost != "tenant" {
		t.Fatalf("virtual host = %q, want tenant", options.VirtualHost)
	}
}

func TestRunDLQRejectsUnknownAction(t *testing.T) {
	var stderr bytes.Buffer
	if exitCode := RunDLQ([]string{"unknown"}, &bytes.Buffer{}, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d, want 2", exitCode)
	}
	if !strings.Contains(stderr.String(), "cli dlq inspect") {
		t.Fatalf("usage output = %q", stderr.String())
	}
}
