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
		"generate-default-labels",
		"seed-media",
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
	username := flag.String("username", "", "Specific username to target")
	startTimestamp := flag.Int64("start-timestamp", 0, "Start Timestamp")
	endTimestamp := flag.Int64("end-timestamp", 0, "End Timestamp")
	timezone := flag.String("timezone", "", "Timezone")
	mode := flag.String("mode", "dry-run", "Mode")
	pipeline := flag.String("pipeline", "monitor,sequence", "Provide the pipeline to execute")
	batchSize := flag.Int("batch-size", 10, "Batch Size")
	batchDelay := flag.Int("batch-delay", 1000, "Batch Delay in milliseconds")
	labelNames := flag.String("label-names", "", "Names of the labels to generate separated by comma")
	target := flag.Int("target", 0, "Total documents to insert (required)")
	parallel := flag.Int("parallel", 0, "Concurrent batch workers (required)")
	dbName := flag.String("db", "Kerberos", "Database name (required)")
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
		)
	default:
		fmt.Println("Invalid action.")
	}
}


{
  "_id": {
    "$oid": "67540dd969eae168b528544b"
  },
  "key": "camera3",
  "analytics": [
    {
      "cloudpublickey": "AKIA5V6EGLUWSI42UQFH",
      "encrypted": false,
      "encrypteddata": {
        "$binary": {
          "base64": "",
          "subType": "00"
        }
      },
      "key": "camera3",
      "hub_encryption": "true",
      "e2e_encryption": "false",
      "enterprise": false,
      "hash": "",
      "version": "3.5.0",
      "release": "1f9772d",
      "mac_list": [
        "02:42:ac:12:00:0c"
      ],
      "ip_list": [
        "127.0.0.1/8",
        "172.18.0.12/16"
      ],
      "cameraname": "office-camera103",
      "cameratype": "IPCamera",
      "architecture": "x86_64",
      "hostname": "b26c2bfe0dc9",
      "freeMemory": "711192576",
      "totalMemory": "16515977216",
      "usedMemory": "15804784640",
      "processMemory": "29523968",
      "kubernetes": false,
      "docker": true,
      "kios": false,
      "raspberrypi": false,
      "uptime": "5 days ",
      "boot_time": "5 days ",
      "timestamp": {
        "$numberLong": "1762258685"
      },
      "onvif": "false",
      "onvif_zoom": "false",
      "onvif_pantilt": "false",
      "onvif_presets": "false",
      "cameraConnected": "false",
      "hasBackChannel": "false"
    }
  ],
  "featurePermissions": {
    "ptz": 0,
    "liveview": 0,
    "remote_config": 0,
    "io": 0
  },
  "isActive": false,

  "status": "idle",
  "user_id": "6754047347321302010cafee",
  "latestMediaTimestamp": {
    "$numberLong": "1756853815"
  }
}


