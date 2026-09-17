package actions

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	sharedqueue "github.com/uug-ai/queue/pkg/queue"
)

type dlqCommandConfig struct {
	provider        string
	deadLetterQueue string
	limit           int
	source          string
	sources         string
	destination     string
	execute         bool
	count           int
	reason          string
	timeout         time.Duration
	idleTimeout     time.Duration
	batchSize       int
	batchDelay      time.Duration

	vaultURI       string
	vaultAccessKey string
	vaultSecret    string
	vaultProvider  string
	vaultURLExpiry string
	vaultAllowHTTP bool

	historicalTailMaxAge time.Duration
	allowHistoricalTail  bool
	legacyUserOwnership  bool

	rabbitHost        string
	rabbitUsername    string
	rabbitPassword    string
	rabbitCACertFile  string
	rabbitVirtualHost string

	kafkaBroker    string
	kafkaGroupID   string
	kafkaUsername  string
	kafkaPassword  string
	kafkaMechanism string
	kafkaSecurity  string

	azureNamespace        string
	azureConnectionString string

	sqsRegion          string
	sqsEndpoint        string
	sqsAccessKeyID     string
	sqsSecretAccessKey string
	sqsSessionToken    string
	sqsMessageGroupID  string
}

// RunDLQ runs dead-letter inspection, replay, recovery, and test seeding.
func RunDLQ(args []string, stdout, stderr io.Writer) int {
	if len(args) < 1 || (args[0] != "inspect" && args[0] != "replay" && args[0] != "recover" && args[0] != "seed") {
		printDLQUsage(stderr)
		return 2
	}
	action := args[0]
	config, err := parseDLQFlags(action, args[1:], stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintf(stderr, "invalid flags: %v\n", err)
		return 2
	}
	if config.deadLetterQueue == "" {
		fmt.Fprintln(stderr, "--dead-letter is required")
		return 2
	}
	if config.destination != "" && config.destination == config.deadLetterQueue {
		fmt.Fprintln(stderr, "--destination cannot be the dead-letter queue")
		return 2
	}
	if action == "recover" {
		if err := validateRecoveryConfig(config); err != nil {
			fmt.Fprintf(stderr, "invalid recovery configuration: %v\n", err)
			return 2
		}
	}
	if action == "seed" && !config.execute {
		result, err := seedDeadLetters(context.Background(), nil, config)
		if err != nil {
			fmt.Fprintf(stderr, "plan dead-letter seed: %v\n", err)
			return 1
		}
		printSeed(stdout, result, false)
		return 0
	}

	client, err := newDLQClient(config)
	if err != nil {
		fmt.Fprintf(stderr, "configure queue client: %v\n", err)
		return 1
	}
	if err := client.Client.Connect(); err != nil {
		fmt.Fprintf(stderr, "connect queue client: %v\n", err)
		return 1
	}
	defer client.Client.Close()
	admin, ok := client.Client.(sharedqueue.DeadLetterAdmin)
	if !ok {
		fmt.Fprintf(stderr, "queue provider %q does not support dead-letter administration\n", config.provider)
		return 1
	}

	if action == "recover" {
		var refresher vaultURLRefresher
		if config.execute {
			refresher, err = newVaultHTTPURLRefresher(config.vaultURI, config.vaultAccessKey, config.vaultSecret, config.vaultAllowHTTP, &http.Client{
				Timeout: config.timeout,
			})
			if err != nil {
				fmt.Fprintf(stderr, "configure Vault URL refresh: %v\n", err)
				return 1
			}
		}
		result, err := recoverDeadLetters(context.Background(), admin, refresher, config)
		printRecovery(stdout, result, config.execute)
		if err != nil {
			fmt.Fprintf(stderr, "recover dead-letter queue: %v\n", err)
			return 1
		}
		return 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), config.timeout)
	defer cancel()
	switch action {
	case "inspect":
		result, err := admin.InspectDeadLetters(ctx, sharedqueue.DeadLetterInspectRequest{
			Limit:       config.limit,
			Source:      config.source,
			IdleTimeout: config.idleTimeout,
		})
		if err != nil {
			fmt.Fprintf(stderr, "inspect dead-letter queue: %v\n", err)
			return 1
		}
		printInspection(stdout, result)
	case "replay":
		result, err := admin.ReplayDeadLetters(ctx, sharedqueue.DeadLetterReplayRequest{
			Limit:       config.limit,
			Source:      config.source,
			Destination: config.destination,
			Execute:     config.execute,
			IdleTimeout: config.idleTimeout,
		})
		if err != nil {
			fmt.Fprintf(stderr, "replay dead-letter queue: %v\n", err)
			return 1
		}
		printReplay(stdout, result, config.execute)
	case "seed":
		result, err := seedDeadLetters(ctx, admin, config)
		if err != nil {
			fmt.Fprintf(stderr, "seed dead-letter queue: %v\n", err)
			return 1
		}
		printSeed(stdout, result, config.execute)
	}
	return 0
}

func parseDLQFlags(action string, args []string, stderr io.Writer) (dlqCommandConfig, error) {
	config := dlqCommandConfig{}
	flags := flag.NewFlagSet("dlq "+action, flag.ContinueOnError)
	flags.SetOutput(stderr)
	limitDescription := "maximum messages to inspect or replay (1-10000)"
	if action == "recover" {
		limitDescription = "maximum messages to scan across recovery batches (1-1000000)"
	}
	flags.StringVar(&config.provider, "provider", envValue("QUEUE_PROVIDER", ""), "queue provider: rabbitmq, kafka, azure-event-hubs, or sqs")
	flags.StringVar(&config.deadLetterQueue, "dead-letter", envValue("DEAD_LETTER_QUEUE", ""), "dead-letter queue, topic, or Event Hub")
	flags.IntVar(&config.limit, "limit", 100, limitDescription)
	flags.StringVar(&config.source, "source", "", "only messages recorded from this source queue")
	timeoutDescription := "administrative operation timeout after connection setup"
	if action == "recover" {
		timeoutDescription = "timeout for each recovery batch"
	}
	flags.DurationVar(&config.timeout, "timeout", time.Minute, timeoutDescription)
	flags.DurationVar(&config.idleTimeout, "idle-timeout", 2*time.Second, "Kafka/Event Hubs idle time that marks the bounded scan complete")

	flags.StringVar(&config.rabbitHost, "rabbitmq-host", envValue("RABBITMQ_HOST", ""), "RabbitMQ host")
	flags.StringVar(&config.rabbitUsername, "rabbitmq-username", envValue("RABBITMQ_USERNAME", ""), "RabbitMQ username")
	flags.StringVar(&config.rabbitPassword, "rabbitmq-password", "", "RabbitMQ password")
	flags.StringVar(&config.rabbitCACertFile, "rabbitmq-ca-cert", envValue("RABBITMQ_CA_CERT_FILE", ""), "RabbitMQ CA certificate file")
	flags.StringVar(&config.rabbitVirtualHost, "rabbitmq-vhost", envValue("RABBITMQ_VHOST", "/"), "RabbitMQ virtual host")

	flags.StringVar(&config.kafkaBroker, "kafka-broker", envValue("KAFKA_BROKER", ""), "Kafka bootstrap server")
	flags.StringVar(&config.kafkaGroupID, "kafka-group-id", envValue("KAFKA_GROUP_ID", ""), "Kafka consumer group")
	flags.StringVar(&config.kafkaUsername, "kafka-username", envValue("KAFKA_USERNAME", ""), "Kafka SASL username")
	flags.StringVar(&config.kafkaPassword, "kafka-password", "", "Kafka SASL password")
	flags.StringVar(&config.kafkaMechanism, "kafka-mechanism", envValue("KAFKA_MECHANISM", ""), "Kafka SASL mechanism")
	flags.StringVar(&config.kafkaSecurity, "kafka-security", envValue("KAFKA_SECURITY_PROTOCOL", ""), "Kafka security protocol")

	flags.StringVar(&config.azureNamespace, "azure-namespace", envValue("AZURE_EVENTHUB_NAMESPACE", ""), "Azure Event Hubs namespace")
	flags.StringVar(&config.azureConnectionString, "azure-connection-string", "", "Azure Event Hubs connection string")

	flags.StringVar(&config.sqsRegion, "sqs-region", envValue("AWS_REGION", ""), "AWS region")
	flags.StringVar(&config.sqsEndpoint, "sqs-endpoint", envValue("SQS_ENDPOINT", ""), "optional SQS endpoint")
	flags.StringVar(&config.sqsAccessKeyID, "sqs-access-key-id", "", "optional static SQS access key ID")
	flags.StringVar(&config.sqsSecretAccessKey, "sqs-secret-access-key", "", "optional static SQS secret access key")
	flags.StringVar(&config.sqsSessionToken, "sqs-session-token", "", "optional static SQS session token")
	flags.StringVar(&config.sqsMessageGroupID, "sqs-message-group-id", envValue("SQS_MESSAGE_GROUP_ID", ""), "message group ID for FIFO replay destinations")

	if action == "replay" || action == "recover" {
		destinationDescription := "override replay destination; required for legacy messages"
		if action == "recover" {
			destinationDescription = "pipeline router destination for recovered events"
		}
		flags.StringVar(&config.destination, "destination", "", destinationDescription)
		flags.BoolVar(&config.execute, "execute", false, "publish and settle messages; otherwise perform a dry run")
	}
	if action == "recover" {
		flags.IntVar(&config.batchSize, "batch-size", 100, "messages to validate, refresh, and replay per batch (1-10000)")
		flags.DurationVar(&config.batchDelay, "batch-delay", time.Second, "delay between executed recovery batches")
		flags.StringVar(&config.vaultURI, "vault-uri", "", "Vault API base URI")
		flags.StringVar(&config.vaultAccessKey, "vault-access-key", "", "Vault storage access key")
		flags.StringVar(&config.vaultSecret, "vault-secret", "", "Vault storage secret")
		flags.StringVar(&config.vaultProvider, "vault-provider", envValue("KERBEROS_STORAGE_PROVIDER", ""), "fallback Vault provider when an event has none")
		flags.StringVar(&config.vaultURLExpiry, "vault-url-expiry", "", "optional signed URL duration such as 24h")
		flags.BoolVar(&config.vaultAllowHTTP, "vault-allow-insecure-http", false, "allow plaintext HTTP for a non-loopback Vault URI")
		flags.DurationVar(&config.historicalTailMaxAge, "historical-tail-max-age", defaultHistoricalTailMaxAge, "recording age after which throttler and notification stages are suppressed")
		flags.BoolVar(&config.allowHistoricalTail, "allow-historical-tail", false, "preserve throttler and notification stages for historical recordings (unsafe)")
		flags.BoolVar(&config.legacyUserOwnership, "legacy-user-ownership", false, "require canonical ownership to match monitor user ID for legacy workers")
	}
	if action == "seed" {
		flags.StringVar(&config.sources, "sources", "kcloud-monitor-queue,kcloud-analysis-queue", "comma-separated source queues to distribute synthetic messages across")
		flags.IntVar(&config.count, "count", 10, "number of synthetic dead-letter messages (1-1000)")
		flags.StringVar(&config.reason, "reason", string(sharedqueue.DeadLetterReasonHandlerError), "dead-letter reason")
		flags.BoolVar(&config.execute, "execute", false, "publish synthetic messages; otherwise perform a dry run")
	}
	if err := flags.Parse(args); err != nil {
		return config, err
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected arguments: %s\n", strings.Join(flags.Args(), " "))
		return config, fmt.Errorf("unexpected arguments")
	}
	if config.rabbitPassword == "" {
		config.rabbitPassword = envValue("RABBITMQ_PASSWORD", "")
	}
	if config.kafkaPassword == "" {
		config.kafkaPassword = envValue("KAFKA_PASSWORD", "")
	}
	if config.azureConnectionString == "" {
		config.azureConnectionString = envValue("AZURE_EVENTHUB_CONNECTION_STRING", "")
	}
	if config.sqsSessionToken == "" {
		config.sqsSessionToken = envValue("SQS_SESSION_TOKEN", "")
	}
	if config.vaultAccessKey == "" {
		config.vaultAccessKey = envValue("KERBEROS_STORAGE_ACCESS_KEY", "")
	}
	if config.vaultSecret == "" {
		config.vaultSecret = envValue("KERBEROS_STORAGE_SECRET", "")
	}
	if config.vaultURI == "" {
		config.vaultURI = envValue("KERBEROS_STORAGE_URI", "")
	}
	config.provider = strings.ToLower(strings.TrimSpace(config.provider))
	if config.kafkaGroupID == "" && config.provider == "kafka" {
		if action == "inspect" {
			config.kafkaGroupID = fmt.Sprintf("queue-dlq-inspect-%d", time.Now().UnixNano())
		} else {
			config.kafkaGroupID = "queue-dlq-replay"
		}
	}
	if config.timeout <= 0 {
		return config, fmt.Errorf("--timeout must be positive")
	}
	if action != "recover" && (config.limit < 1 || config.limit > 10000) {
		return config, fmt.Errorf("--limit must be between 1 and 10000")
	}
	if action == "seed" && (config.count < 1 || config.count > 1000) {
		return config, fmt.Errorf("--count must be between 1 and 1000")
	}
	return config, nil
}

func newDLQClient(config dlqCommandConfig) (*sharedqueue.Queue, error) {
	switch config.provider {
	case "rabbitmq":
		builder := sharedqueue.NewRabbitOptions().
			SetConsumerQueue(config.deadLetterQueue).
			SetDeadletterQueue(config.deadLetterQueue).
			SetHost(config.rabbitHost).
			SetUsername(config.rabbitUsername).
			SetPassword(config.rabbitPassword).
			SetVirtualHost(config.rabbitVirtualHost).
			SetRequireExistingQueues(true)
		if config.rabbitCACertFile != "" {
			builder.SetTLS(true).SetTLSCACertFile(config.rabbitCACertFile)
		}
		return sharedqueue.New(builder.Build())
	case "kafka":
		return sharedqueue.New(newKafkaDLQOptions(config))
	case "azure-event-hubs", "azure":
		return sharedqueue.New(sharedqueue.NewAzureEventHubOptions().
			SetNamespace(config.azureNamespace).
			SetConnectionString(config.azureConnectionString).
			SetConsumerEventHub(config.deadLetterQueue).
			SetDeadletterEventHub(config.deadLetterQueue).
			SetConsumerGroup(config.kafkaGroupID).
			SetAutoOffsetReset("earliest").
			Build())
	case "sqs":
		builder := sharedqueue.NewSQSOptions().
			SetConsumerQueue(config.deadLetterQueue).
			SetDeadletterQueue(config.deadLetterQueue).
			SetRegion(config.sqsRegion).
			SetEndpoint(config.sqsEndpoint).
			SetSessionToken(config.sqsSessionToken).
			SetMessageGroupID(config.sqsMessageGroupID)
		if config.sqsAccessKeyID != "" || config.sqsSecretAccessKey != "" {
			builder.SetCredentials(config.sqsAccessKeyID, config.sqsSecretAccessKey)
		}
		return sharedqueue.New(builder.Build())
	case "":
		return nil, fmt.Errorf("--provider is required")
	default:
		return nil, fmt.Errorf("unsupported provider %q", config.provider)
	}
}

func newKafkaDLQOptions(config dlqCommandConfig) *sharedqueue.KafkaOptions {
	return sharedqueue.NewKafkaOptions().
		SetConsumerTopic(config.deadLetterQueue).
		SetDeadletterTopic(config.deadLetterQueue).
		SetBroker(config.kafkaBroker).
		SetGroupID(config.kafkaGroupID).
		SetUsername(config.kafkaUsername).
		SetPassword(config.kafkaPassword).
		SetMechanism(config.kafkaMechanism).
		SetSecurity(config.kafkaSecurity).
		SetAutoOffsetReset("earliest").
		SetDisableAutoTopicCreation(true).
		Build()
}

func printInspection(output io.Writer, result sharedqueue.DeadLetterInspectResult) {
	sources := make([]string, 0, len(result.Groups))
	for source := range result.Groups {
		sources = append(sources, source)
	}
	sort.Strings(sources)

	writer := tabwriter.NewWriter(output, 0, 4, 2, ' ', 0)
	fmt.Fprintln(writer, "SOURCE QUEUE\tMESSAGES\tOLDEST\tNEWEST")
	for _, source := range sources {
		group := result.Groups[source]
		fmt.Fprintf(writer, "%s\t%d\t%s\t%s\n", source, group.Count, formatTimestamp(group.Oldest), formatTimestamp(group.Newest))
	}
	fmt.Fprintf(writer, "TOTAL\t%d\t\t\n", result.Matched)
	writer.Flush()
	fmt.Fprintf(output, "Scanned: %d\nLegacy/unknown: %d\n", result.Scanned, result.Legacy)
}

func printReplay(output io.Writer, result sharedqueue.DeadLetterReplayResult, execute bool) {
	mode := "dry-run"
	if execute {
		mode = "executed"
	}
	fmt.Fprintf(output, "Mode: %s\n", mode)
	printReplayDestinations(output, result.Destinations)
	fmt.Fprintf(output, "Scanned: %d\nMatched: %d\nPlanned: %d\nReplayed: %d\nRetained: %d\nLegacy/unknown: %d\nUnroutable: %d\n",
		result.Scanned,
		result.Matched,
		result.Planned,
		result.Replayed,
		result.Retained,
		result.Legacy,
		result.Unroutable,
	)
	if !execute {
		fmt.Fprintln(output, "No messages were moved. Pass --execute to replay.")
	}
}

func printReplayDestinations(output io.Writer, counts map[string]int) {
	destinations := make([]string, 0, len(counts))
	for destination := range counts {
		destinations = append(destinations, destination)
	}
	sort.Strings(destinations)
	if len(destinations) > 0 {
		writer := tabwriter.NewWriter(output, 0, 4, 2, ' ', 0)
		fmt.Fprintln(writer, "REPLAY DESTINATION\tMESSAGES")
		for _, destination := range destinations {
			fmt.Fprintf(writer, "%s\t%d\n", destination, counts[destination])
		}
		writer.Flush()
	}
}

type seedResult struct {
	Planned   int
	Published int
	BySource  map[string]int
}

func seedDeadLetters(ctx context.Context, publisher sharedqueue.DeadLetterAdmin, config dlqCommandConfig) (seedResult, error) {
	result := seedResult{BySource: make(map[string]int)}
	sources, err := parseSources(config.sources)
	if err != nil {
		return result, err
	}
	reason := sharedqueue.DeadLetterReason(strings.TrimSpace(config.reason))
	if reason == "" {
		return result, fmt.Errorf("--reason cannot be empty")
	}

	for index := 0; index < config.count; index++ {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		source := sources[index%len(sources)]
		result.Planned++
		result.BySource[source]++
		if !config.execute {
			continue
		}
		payload := []byte(fmt.Sprintf(`{"synthetic":true,"sequence":%d,"source":%q}`, index+1, source))
		if err := publisher.PublishDeadLetter(ctx, payload, sharedqueue.DeadLetterMetadata{
			Source:      source,
			Destination: config.deadLetterQueue,
			Service:     "queue-dlq-seed",
			Reason:      reason,
			Attempts:    1,
		}); err != nil {
			return result, fmt.Errorf("publish synthetic dead-letter message %d: %w", index+1, err)
		}
		result.Published++
	}
	return result, nil
}

func parseSources(value string) ([]string, error) {
	var sources []string
	seen := make(map[string]struct{})
	for _, candidate := range strings.Split(value, ",") {
		source := strings.TrimSpace(candidate)
		if source == "" {
			continue
		}
		if _, exists := seen[source]; exists {
			continue
		}
		seen[source] = struct{}{}
		sources = append(sources, source)
	}
	if len(sources) == 0 {
		return nil, fmt.Errorf("--sources must contain at least one queue")
	}
	return sources, nil
}

func printSeed(output io.Writer, result seedResult, execute bool) {
	mode := "dry-run"
	if execute {
		mode = "executed"
	}
	fmt.Fprintf(output, "Mode: %s\n", mode)
	sources := make([]string, 0, len(result.BySource))
	for source := range result.BySource {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	writer := tabwriter.NewWriter(output, 0, 4, 2, ' ', 0)
	fmt.Fprintln(writer, "SOURCE QUEUE\tMESSAGES")
	for _, source := range sources {
		fmt.Fprintf(writer, "%s\t%d\n", source, result.BySource[source])
	}
	fmt.Fprintf(writer, "TOTAL\t%d\n", result.Planned)
	writer.Flush()
	fmt.Fprintf(output, "Published: %d\n", result.Published)
	if !execute {
		fmt.Fprintln(output, "No messages were published. Pass --execute to seed the dead-letter queue.")
	}
}

func formatTimestamp(value time.Time) string {
	if value.IsZero() {
		return "-"
	}
	return value.UTC().Format(time.RFC3339)
}

func envValue(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func printDLQUsage(output io.Writer) {
	fmt.Fprintln(output, "Usage:")
	fmt.Fprintln(output, "  cli dlq inspect [flags]")
	fmt.Fprintln(output, "  cli dlq replay [flags]")
	fmt.Fprintln(output, "  cli dlq recover [flags]")
	fmt.Fprintln(output, "  cli dlq seed [flags]")
}
