package actions

import (
	"context"
	"encoding/base32"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/uug-ai/cli/database"
	"github.com/uug-ai/cli/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/crypto/bcrypt"
)

var stopFlag int32

const DAYS = 30

func HandleSignals() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\n[signal] Received interrupt, stopping after current batches...")
		atomic.StoreInt32(&stopFlag, 1)
	}()
}

func GenerateKey(keyType string, client *mongo.Client, dbName, userCollName string) (string, error) {
	const keyLen = 20
	maxRetries := 10
	for attempts := 0; attempts < maxRetries; attempts++ {
		b := make([]byte, keyLen)
		_, err := rand.Read(b)
		if err != nil {
			return "", err
		}
		key := strings.TrimRight(base32.StdEncoding.EncodeToString(b), "=")
		isDup, err := database.IsDuplicateKey(keyType, key, client, dbName, userCollName)
		if err != nil {
			return "", err
		}
		if !isDup {
			return key, nil
		}
	}
	return "", fmt.Errorf("failed to generate unique key after max retries")
}

func PromptInt(prompt string) int {
	for {
		if atomic.LoadInt32(&stopFlag) != 0 {
			fmt.Println("\n[info] Interrupt received, exiting prompt.")
			os.Exit(130)
		}
		fmt.Print(prompt)
		var input string
		_, err := fmt.Scanln(&input)
		if err != nil {
			return 0
		}
		if input == "" {
			return 0
		}
		var val int
		_, err = fmt.Sscanf(input, "%d", &val)
		if err == nil && val > 0 {
			return val
		}
		fmt.Println("Please enter a positive integer or press Enter for default.")
	}
}

func PromptString(prompt string) string {
	if atomic.LoadInt32(&stopFlag) != 0 {
		fmt.Println("\n[info] Interrupt received, exiting prompt.")
		os.Exit(130)
	}
	fmt.Print(prompt)
	var val string
	_, _ = fmt.Scanln(&val)
	return val
}

func Hash(str string) (string, error) {
	hashed, err := bcrypt.GenerateFromPassword([]byte(str), bcrypt.DefaultCost)
	return string(hashed), err
}

func WasFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func SeedMedia(
	target int,
	batchSize int,
	parallel int,
	uri string,
	dbName string,
	mediaCollName string,
	userCollName string,
	deviceCollName string,
	subscriptionCollName string,
	settingsCollName string,
	noIndex bool,
	reportEvery int,
	userId string,
	userName string,
	userPassword string,
	userEmail string,
	deviceCount int,
) {
	rand.Seed(time.Now().UnixNano())
	HandleSignals()

	fmt.Printf("[info] Skip any prompt to use default values.")

	// -------- Prompts / defaults --------
	if !WasFlagPassed("target") {
		target = PromptInt("Enter total amount of documents to insert (--target): ")
		if target == 0 {
			target = 100000
			fmt.Printf("[info] Using default target: %d\n", target)
		}
	}
	if !WasFlagPassed("batch-size") {
		batchSize = PromptInt("Enter documents per batch (--batch-size) (1-100,000): ")
		if batchSize == 0 {
			switch {
			case target <= 10000:
				batchSize = 500
			case target <= 100000:
				batchSize = 2000
			default:
				batchSize = 5000
			}
			fmt.Printf("[info] Using recommended batch size: %d\n", batchSize)
		}
	} else if batchSize < 1 {
		batchSize = 1
	} else if batchSize > 100000 {
		batchSize = 100000
	}
	if batchSize > target {
		batchSize = target
		fmt.Printf("[info] Batch size limited to target: %d\n", batchSize)
	}
	if !WasFlagPassed("parallel") {
		parallel = PromptInt("Enter concurrent batch workers (--parallel) (1-16): ")
		if parallel == 0 {
			switch {
			case target <= 10000:
				parallel = 2
			case target <= 100000:
				parallel = 4
			default:
				parallel = 8
			}
			fmt.Printf("[info] Using recommended parallel value: %d\n", parallel)
		}
	} else if parallel < 1 {
		parallel = 1
	} else if parallel > 16 {
		parallel = 16
	}
	if !WasFlagPassed("uri") {
		uri = PromptString("Enter MongoDB URI (--uri): ")
		if uri == "" {
			uri = "mongodb://localhost:27017"
			fmt.Printf("[info] Using default MongoDB URI: %s\n", uri)
		}
	}
	if !WasFlagPassed("db") {
		dbName = PromptString("Enter database name (--db): ")
		if dbName == "" {
			dbName = "Kerberos"
			fmt.Printf("[info] Using default database name: %s\n", dbName)
		}
	}
	if !WasFlagPassed("media-collection") {
		mediaCollName = PromptString("Enter target media collection name (--media-collection): ")
		if mediaCollName == "" {
			mediaCollName = "media"
			fmt.Printf("[info] Using default media collection name: %s\n", mediaCollName)
		}
	}
	if !WasFlagPassed("user-id") {
		userId = PromptString("Enter user ID (--user-id), keep empty to create new user: ")
	}
	if !WasFlagPassed("user-id") && userId == "" {
		if userName == "" {
			userName = PromptString("Enter user name (--user-name): ")
			if userName == "" {
				userName = "media-user"
				fmt.Printf("[info] Using default user name: %s\n", userName)
			}
		}
		if userPassword == "" {
			userPassword = PromptString("Enter user password (--user-password): ")
			if userPassword == "" {
				userPassword = "media-password"
				fmt.Printf("[info] Using default user password: %s\n", userPassword)
			}
		}
		if userEmail == "" {
			userEmail = PromptString("Enter user email (--user-email): ")
			if userEmail == "" {
				userEmail = "example-media-user@email.com"
				fmt.Printf("[info] Using default user email: %s\n", userEmail)
			}
		}
		if !WasFlagPassed("settings-collection") {
			settingsCollName = PromptString("Enter target settings collection name (--settings-collection): ")
			if settingsCollName == "" {
				settingsCollName = "settings"
				fmt.Printf("[info] Using default settings collection name: %s\n", settingsCollName)
			}
		}
		if subscriptionCollName == "" {
			subscriptionCollName = PromptString("Enter target subscription collection name (--subscription-collection): ")
			if subscriptionCollName == "" {
				subscriptionCollName = "subscriptions"
				fmt.Printf("[info] Using default subscription collection name: %s\n", subscriptionCollName)
			}
		}
	} else {
		fmt.Printf("[info] Using existing user ID: %s\n", userId)
	}
	if !WasFlagPassed("user-collection") {
		userCollName = PromptString("Enter target user collection name (--user-collection): ")
		if userCollName == "" {
			userCollName = "users"
			fmt.Printf("[info] Using default user collection name: %s\n", userCollName)
		}
	}
	if !WasFlagPassed("device-collection") {
		deviceCollName = PromptString("Enter target device collection name (--device-collection): ")
		if deviceCollName == "" {
			deviceCollName = "devices"
			fmt.Printf("[info] Using default device collection name: %s\n", deviceCollName)
		}
	}
	if !WasFlagPassed("device-count") {
		deviceCount = PromptInt("Enter number of devices to simulate (--device-count) (1-50): ")
	}
	if deviceCount < 1 {
		deviceCount = 1
	} else if deviceCount > 50 {
		deviceCount = 50
	}

	// -------- Connect --------
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetServerSelectionTimeout(10*time.Second))
	if err != nil {
		fmt.Printf("[error] MongoDB connect: %v\n", err)
		os.Exit(1)
	}

	// -------- Settings --------
	if err := database.EnsureSettings(ctx, client, dbName, settingsCollName); err != nil {
		fmt.Printf("[error] ensure settings: %v\n", err)
		os.Exit(1)
	}

	// -------- User & subscription --------
	var userObjectID primitive.ObjectID
	var amazonSecretAccessKey, amazonAccessKeyID string

	if userId != "" {
		userObjectID, err = primitive.ObjectIDFromHex(userId)
		if err != nil {
			fmt.Printf("[error] invalid userId: %v\n", err)
			os.Exit(1)
		}
		var userDoc bson.M
		err = client.Database(dbName).Collection(userCollName).FindOne(ctx, bson.M{"_id": userObjectID}).Decode(&userDoc)
		if err != nil {
			fmt.Printf("[error] fetch user: %v\n", err)
			os.Exit(1)
		}
		amazonSecretAccessKey, _ = userDoc["amazon_secret_access_key"].(string)
		amazonAccessKeyID, _ = userDoc["amazon_access_key_id"].(string)
		if amazonSecretAccessKey == "" || amazonAccessKeyID == "" {
			fmt.Printf("[error] user missing keys\n")
			os.Exit(1)
		}
	} else {
		// check if user with same name exists
		existingUser := database.GetUserFromMongodb(client, dbName, userName, userCollName)
		if existingUser.Username != "" {
			fmt.Printf("[error] user with name %s already exists (ID: %s)\n", userName, existingUser.Id.Hex())
			os.Exit(1)
		}

		amazonSecretAccessKey, err = GenerateKey("private", client, dbName, userCollName)
		if err != nil {
			fmt.Printf("[error] generate private key: %v\n", err)
			os.Exit(1)
		}
		amazonAccessKeyID, err = GenerateKey("public", client, dbName, userCollName)
		if err != nil {
			fmt.Printf("[error] generate public key: %v\n", err)
			os.Exit(1)
		}
		hashedPassword, err := Hash(userPassword)
		if err != nil {
			fmt.Printf("[error] hash password: %v\n", err)
			os.Exit(1)
		}
		userInfo := models.InsertUserInfo{
			UserName:              userName,
			UserEmail:             userEmail,
			UserPassword:          hashedPassword,
			AmazonSecretAccessKey: amazonSecretAccessKey,
			AmazonAccessKeyID:     amazonAccessKeyID,
			Days:                  DAYS,
		}
		userObjectID, userDoc := database.BuildUserDoc(userInfo)
		if err := database.InsertOne(ctx, client, dbName, userCollName, userDoc); err != nil {
			fmt.Printf("[error] insert user: %v\n", err)
			os.Exit(1)
		}
		subDoc := database.BuildSubscriptionDoc(userObjectID)
		if err := database.InsertOne(ctx, client, dbName, subscriptionCollName, subDoc); err != nil {
			fmt.Printf("[error] insert subscription: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("[info] Created new user with enterprise subscription.\n")
	}

	// -------- Devices --------
	deviceDocs, deviceIDs := database.BuildDeviceDocs(deviceCount, userObjectID, amazonSecretAccessKey)
	if len(deviceDocs) == 0 {
		fmt.Printf("[error] no devices generated\n")
		os.Exit(1)
	}
	if err := database.InsertMany(ctx, client, dbName, deviceCollName, deviceDocs); err != nil {
		fmt.Printf("[error] insert devices: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("[info] Created %d devices for user %s\n", len(deviceDocs), userObjectID.Hex())

	// -------- Media collection & indexes --------
	mediaColl := client.Database(dbName).Collection(mediaCollName)
	if !noIndex {
		database.CreateIndexes(ctx, mediaColl)
	}

	// -------- Batch channel & workers --------
	var (
		totalInserted int64
		batchCounter  int64
		startTime     = time.Now()
		batchCh       = make(chan []interface{}, parallel*2)
	)

	fmt.Printf("[start] target=%d batch=%d parallel=%d uri=%s db=%s.%s\n",
		target, batchSize, parallel, uri, dbName, mediaCollName)

	// Use RunBatchWorkers
	doneWorkers := make(chan struct{})
	go func() {
		database.RunBatchWorkers(ctx, parallel, mediaColl, batchCh, &stopFlag, &totalInserted)
		close(doneWorkers)
	}()

	// -------- Produce batches --------
	var totalQueued int64
	for atomic.LoadInt64(&totalQueued) < int64(target) && atomic.LoadInt32(&stopFlag) == 0 {
		remaining := int64(target) - atomic.LoadInt64(&totalQueued)
		current := batchSize
		if remaining < int64(batchSize) {
			current = int(remaining)
		}
		docs := database.BuildBatchDocs(current, DAYS, userObjectID, deviceIDs)
		batchCh <- docs
		atomic.AddInt64(&batchCounter, 1)
		atomic.AddInt64(&totalQueued, int64(current))

		if atomic.LoadInt64(&batchCounter)%int64(reportEvery) == 0 {
			elapsed := time.Since(startTime).Seconds()
			rate := float64(atomic.LoadInt64(&totalInserted)) / elapsed
			pct := 100 * float64(atomic.LoadInt64(&totalInserted)) / float64(target)
			fmt.Printf("[progress] batches=%d inserted=%d (%.2f%%) rate=%.0f/s\n",
				atomic.LoadInt64(&batchCounter), atomic.LoadInt64(&totalInserted), pct, rate)
		}
	}

	// Finish
	close(batchCh)
	<-doneWorkers

	// -------- Summary --------
	if err := client.Disconnect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "[warn] disconnect MongoDB: %v\n", err)
	}
	elapsed := time.Since(startTime).Seconds()
	rate := float64(atomic.LoadInt64(&totalInserted)) / elapsed
	fmt.Printf("[done] inserted=%d elapsed=%.2fs rate=%.0f/s stop_flag=%v\n",
		atomic.LoadInt64(&totalInserted), elapsed, rate, atomic.LoadInt32(&stopFlag) != 0)

	if atomic.LoadInt32(&stopFlag) != 0 {
		os.Exit(130)
	} else {
		os.Exit(0)
	}
}
