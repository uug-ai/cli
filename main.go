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
	flag.Parse()

	// If a step is provided, execute it directly
	switch *action {
	case "vault-to-hub-migration":
		fmt.Println("Starting Vault to Hub migration...")
		actions.VaultToHubMigration(mongodbURI,
			mongodbHost,
			mongodbPort,
			mongodbSourceDatabase,
			mongodbDestinationDatabase,
			mongodbDatabaseCredentials,
			mongodbUsername,
			mongodbPassword)
	default:
		fmt.Println("Please provide a valid action.")
		fmt.Println("Available actions:")
		fmt.Println("  -action vault-to-hub-migration")
	}

}
