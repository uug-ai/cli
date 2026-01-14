package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/uug-ai/cli/actions"
)

func promptAction() string {
	choices := []string{
		"vault-to-hub-migration",
		"reprocess-media",
		"generate-default-labels",
		"check-indexes",
		"seed-media",
		"seed-users",
		"seed-devices",
		"seed-groups",
		"seed-markers",
	}
	fmt.Println("Select an action:")
	for i, c := range choices {
		fmt.Printf("  %d) %s\n", i+1, c)
	}
	var sel int
	for {
		fmt.Print("Enter number: ")
		_, err := fmt.Scanln(&sel)
		if err == nil && sel >= 1 && sel <= len(choices) {
			return choices[sel-1]
		}
		fmt.Println("Invalid choice.")
	}
}

func main() {

	fmt.Println(`
     _    _ _    _  _____     _____ _ _ 
    | |  | | |  | |/ ____|   / ____(_) |
    | |  | | |  | | |  __   | |     _| |
    | |  | | |  | | | |_ |  | |    | | |
    | |__| | |__| | |__| |  | |____| | |
    \____/ \____/ \_____|   \_____|_|_|
                                        
    `)

	action := flag.String("action", "", "Action to take")

	mongodbURI := flag.String("mongodb-uri", "", "MongoDB URI")
	mongodbHost := flag.String("mongodb-host", "", "MongoDB Host")
	mongodbPort := flag.String("mongodb-port", "", "MongoDB Port")
	mongodbSourceDatabase := flag.String("mongodb-source-database", "", "MongoDB Source Database")
	mongodbDestinationDatabase := flag.String("mongodb-destination-database", "", "MongoDB Destination Database")
	mongodbDatabaseCredentials := flag.String("mongodb-database-credentials", "", "MongoDB Database Credentials")
	mongodbUsername := flag.String("mongodb-username", "", "MongoDB Username")
	mongodbPassword := flag.String("mongodb-password", "", "MongoDB Password")
	queueName := flag.String("queue", "", "The queue used to transfer the data")
	vaultUrl := flag.String("vault-url", "", "Vault API URL override (e.g. https://vault.example.com/api)")
	username := flag.String("username", "", "Specific username to target")
	startTimestamp := flag.Int64("start-timestamp", 0, "Start Timestamp")
	endTimestamp := flag.Int64("end-timestamp", 0, "End Timestamp")
	timezone := flag.String("timezone", "", "Timezone")
	mode := flag.String("mode", "dry-run", "Mode")
	pipeline := flag.String("pipeline", "monitor,sequence", "Provide the pipeline to execute")
	operationCount := flag.Int("operation-count", 0, "Minimum resolved operations required to keep an existing analysis")
	batchSize := flag.Int("batch-size", 10, "Batch Size")
	batchDelay := flag.Int("batch-delay", 1000, "Batch Delay in milliseconds")
	labelNames := flag.String("label-names", "", "Names of the labels to generate separated by comma")
	target := flag.Int("target", 0, "Total documents to insert (required)")
	parallel := flag.Int("parallel", 0, "Concurrent batch workers (required)")
	dbName := flag.String("db", "", "Database name (required)")
	mediaCollName := flag.String("media-collection", "media", "Media collection name (required)")
	userCollName := flag.String("user-collection", "users", "User collection name")
	deviceCollName := flag.String("device-collection", "devices", "Device collection name")
	subscriptionCollName := flag.String("subscription-collection", "subscriptions", "Subscription collection name")
	settingsCollName := flag.String("settings-collection", "settings", "Settings collection name")
	noIndex := flag.Bool("no-index", false, "Skip index creation")
	reportEvery := flag.Int("report-every", 10, "Report progress every N batches")
	userId := flag.String("user-id", "", "User ID to link media to")
	userName := flag.String("user-name", "", "User name for the media user")
	userPassword := flag.String("user-password", "", "User password for the media user")
	userEmail := flag.String("user-email", "", "User email for the media user")
	deviceCount := flag.Int("device-count", 0, "Number of devices to simulate")
	days := flag.Int("days", 7, "Number of past days to spread the media over")
	groupCollName := flag.String("groups-collection", "", "Groups collection name")
	markerCollName := flag.String("marker-collection", "", "Marker collection name")
	markerOptCollName := flag.String("marker-option-collection", "marker_options", "Marker option collection name")
	tagOptCollName := flag.String("tag-option-collection", "", "Tag option collection name")
	eventOptCollName := flag.String("event-option-collection", "", "Event type option collection name")
	markerOptRangesCollName := flag.String("marker-option-ranges-collection", "", "Marker option ranges collection name")
	tagOptRangesCollName := flag.String("tag-option-ranges-collection", "", "Tag option ranges collection name")
	eventTypeOptRangesCollName := flag.String("event-option-ranges-collection", "", "Event type option ranges collection name")
	existingUserIDHex := flag.String("existing-user-id", "", "Existing user ID to use as organisationId for markers")
	markerBatchSize := flag.Int("marker-batch-size", 100, "Batch size for marker inserts")
	markerTarget := flag.Int("marker-target", 0, "Total markers to insert (required)")
	userPrefix := flag.String("user-prefix", "user", "Prefix for random users")
	// useExistingDevices := flag.Bool("use-existing-devices", false, "Use existing devices instead of creating new ones")
	collections := flag.String("collections", "", "Comma separated list of collections to migrate (default all)")
	indexVersion := flag.String("index-version", "", "Path to the indexes specification file")

	flag.Parse()

	if strings.TrimSpace(*action) == "" {
		*action = promptAction()
	}

	switch *action {
	case "vault-to-hub-migration":
		fmt.Println("Starting Vault to Hub migration...")
		actions.VaultToHubMigration(*mode,
			*mongodbURI,
			*mongodbHost,
			*mongodbPort,
			*mongodbSourceDatabase,
			*mongodbDestinationDatabase,
			*mongodbDatabaseCredentials,
			*mongodbUsername,
			*mongodbPassword,
			*queueName,
			*vaultUrl,
			*username,
			*startTimestamp,
			*endTimestamp,
			*timezone,
			*pipeline,
			*operationCount,
			*batchSize,
			*batchDelay,
		)
	case "reprocess-media":
		fmt.Println("Reprocessing media for analysis...")
		actions.ReprocessMedia(*mode,
			*mongodbURI,
			*mongodbHost,
			*mongodbPort,
			*mongodbSourceDatabase,
			*mongodbDestinationDatabase,
			*mongodbDatabaseCredentials,
			*mongodbUsername,
			*mongodbPassword,
			*queueName,
			*userId,
			*startTimestamp,
			*endTimestamp,
			*timezone,
			*batchSize,
			*batchDelay,
		)
	case "generate-default-labels":
		fmt.Println("Generating default labels...")
		actions.GenerateDefaultLabels(*mode,
			*mongodbURI,
			*mongodbHost,
			*mongodbPort,
			*mongodbSourceDatabase,
			*mongodbDatabaseCredentials,
			*mongodbUsername,
			*mongodbPassword,
			*labelNames,
			*username,
		)
	case "check-indexes":
		fmt.Println("Checking indexes...")
		actions.CheckIndexes(
			*mongodbURI,
			*mongodbDestinationDatabase,
			*collections,
			*mode,
			*indexVersion,
		)
	case "seed-media":
		fmt.Println("Seeding synthetic media...")
		actions.SeedMedia(
			*target,
			*batchSize,
			*parallel,
			*mongodbURI,
			*dbName,
			*mediaCollName,
			*userCollName,
			*deviceCollName,
			*subscriptionCollName,
			*settingsCollName,
			*noIndex,
			*reportEvery,
			*userId,
			*userName,
			*userPassword,
			*userEmail,
			*deviceCount,
			*days,
		)
	case "seed-users":
		fmt.Println("Seeding synthetic users...")
		actions.SeedUsers(
			*userPrefix,
			*target,
			*mongodbURI,
			*dbName,
			*userCollName,
			*subscriptionCollName,
			*settingsCollName,
		)
	case "seed-devices":
		fmt.Println("Seeding synthetic devices...")
		actions.SeedDevices(
			*target,
			*mongodbURI,
			*dbName,
			*userCollName,
			*deviceCollName,
			*subscriptionCollName,
			*settingsCollName,
			*userId,
			*userName,
			*userPassword,
			*userEmail,
			*days,
			*noIndex,
		)
	case "seed-groups":
		fmt.Println("Seeding synthetic groups...")
		actions.SeedGroups(
			*target,
			*mongodbURI,
			*dbName,
			*groupCollName,
			*deviceCollName,
			*userId,
		)
	case "seed-markers":
		fmt.Println("Seeding synthetic markers...")
		actions.SeedMarkers(
			*markerTarget,
			*markerBatchSize,
			*parallel,
			*mongodbURI,
			*dbName,
			*deviceCollName,
			*groupCollName,
			*markerCollName,
			*markerOptCollName,
			*tagOptCollName,
			*eventOptCollName,
			*markerOptRangesCollName,
			*tagOptRangesCollName,
			*eventTypeOptRangesCollName,
			*existingUserIDHex,
			*deviceCount,
			*days,
		)
	default:
		fmt.Println("Invalid action.")
	}
}
