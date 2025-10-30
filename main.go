package main

import (
	"flag"
	"fmt"

	"github.com/uug-ai/cli/actions"
)

func main() {

	fmt.Println(`
    _    _ _    _  _____     _____ _ _ 
    | |  | | |  | |/ ____|   / ____(_) |
    | |  | | |  | | |  __   | |     _| |
    | |  | | |  | | | |_ |  | |    | | |
    | |__| | |__| | |__| |  | |____| | |
    \____/ \____/ \_____|   \_____|_|_|
                                        
    `)

	// Define command-line arguments
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
	username := flag.String("username", "", "Specific username to target")
	startTimestamp := flag.Int64("start-timestamp", 0, "Start Timestamp")
	endTimestamp := flag.Int64("end-timestamp", 0, "End Timestamp")
	timezone := flag.String("timezone", "", "Timezone")
	mode := flag.String("mode", "dry-run", "Mode")
	pipeline := flag.String("pipeline", "monitor,sequence", "Provide the pipeline to execute")
	batchSize := flag.Int("batch-size", 10, "Batch Size")
	batchDelay := flag.Int("batch-delay", 1000, "Batch Delay in milliseconds")
	labelNames := flag.String("label-names", "", "Names of the labels to generate separated by comma")

	flag.Parse()

	// If a step is provided, execute it directly
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
			*username,
			*startTimestamp,
			*endTimestamp,
			*timezone,
			*pipeline,
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
	case "seed-media":
		fmt.Println("Seeding synthetic media...")
		actions.SeedMedia()
	default:
		fmt.Println("Please provide a valid action.")
		fmt.Println("Available actions:")
		fmt.Println("  -action vault-to-hub-migration")
		fmt.Println("  -action generate-default-labels")
		fmt.Println("  -action seed-media")
	}

}
