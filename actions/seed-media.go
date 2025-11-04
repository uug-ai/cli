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

	"github.com/gosuri/uiprogress"
	"github.com/uug-ai/cli/database"
	"github.com/uug-ai/cli/models"
	"github.com/uug-ai/cli/utils"

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
		if err == nil && val >= 0 {
			return val
		}
		fmt.Println("Enter a non-negative integer or press Enter for default.")
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
	return strings.TrimSpace(val)
}

func PromptBool(prompt string, def bool) bool {
	if atomic.LoadInt32(&stopFlag) != 0 {
		fmt.Println("\n[info] Interrupt received, exiting prompt.")
		os.Exit(130)
	}
	fmt.Print(prompt)
	var val string
	_, _ = fmt.Scanln(&val)
	val = strings.ToLower(strings.TrimSpace(val))
	if val == "" {
		return def
	}
	if val == "y" || val == "yes" || val == "1" || val == "true" {
		return true
	}
	if val == "n" || val == "no" || val == "0" || val == "false" {
		return false
	}
	fmt.Println("[warn] Invalid boolean input, using default.")
	return def
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

	fmt.Println("[info] Configuration phase starting...")

	// --- db ---
	if WasFlagPassed("db") {
		fmt.Printf("[info] using flag --db=%s\n", dbName)
	} else {
		fmt.Printf("[info] using default --db=%s (no flag)\n", dbName)
	}

	// --- media-collection ---
	if WasFlagPassed("media-collection") {
		fmt.Printf("[info] using flag --media-collection=%s\n", mediaCollName)
	} else {
		fmt.Printf("[info] using default --media-collection=%s\n", mediaCollName)
	}

	// --- user-collection ---
	if WasFlagPassed("user-collection") {
		fmt.Printf("[info] using flag --user-collection=%s\n", userCollName)
	} else {
		fmt.Printf("[info] using default --user-collection=%s\n", userCollName)
	}

	// --- device-collection ---
	if WasFlagPassed("device-collection") {
		fmt.Printf("[info] using flag --device-collection=%s\n", deviceCollName)
	} else {
		fmt.Printf("[info] using default --device-collection=%s\n", deviceCollName)
	}

	// --- subscription-collection ---
	if WasFlagPassed("subscription-collection") {
		fmt.Printf("[info] using flag --subscription-collection=%s\n", subscriptionCollName)
	} else {
		fmt.Printf("[info] using default --subscription-collection=%s\n", subscriptionCollName)
	}

	// --- settings-collection ---
	if WasFlagPassed("settings-collection") {
		fmt.Printf("[info] using flag --settings-collection=%s\n", settingsCollName)
	} else {
		fmt.Printf("[info] using default --settings-collection=%s\n", settingsCollName)
	}

	// --- target ---
	if WasFlagPassed("target") {
		if target <= 0 {
			target = 100000
		}
		fmt.Printf("[info] using flag --target=%d\n", target)
	} else {
		val := PromptInt("Total documents (--target, default 100000): ")
		if val <= 0 {
			target = 100000
			fmt.Printf("[info] using default --target=%d\n", target)
		} else {
			target = val
			fmt.Printf("[info] using input --target=%d\n", target)
		}
	}

	// --- batch-size ---
	if WasFlagPassed("batch-size") {
		if batchSize < 1 {
			batchSize = 1
		} else if batchSize > 100000 {
			batchSize = 100000
		}
		if batchSize > target {
			batchSize = target
		}
		fmt.Printf("[info] using flag --batch-size=%d\n", batchSize)
	} else {
		val := PromptInt("Documents per batch (--batch-size, auto if empty): ")
		if val <= 0 {
			switch {
			case target <= 10000:
				batchSize = 500
			case target <= 100000:
				batchSize = 2000
			default:
				batchSize = 5000
			}
			if batchSize > target {
				batchSize = target
			}
			fmt.Printf("[info] using default/auto --batch-size=%d\n", batchSize)
		} else {
			if val > 100000 {
				val = 100000
			}
			if val > target {
				val = target
			}
			batchSize = val
			fmt.Printf("[info] using input --batch-size=%d\n", batchSize)
		}
	}

	// --- parallel ---
	if WasFlagPassed("parallel") {
		if parallel < 1 {
			parallel = 1
		} else if parallel > 16 {
			parallel = 16
		}
		fmt.Printf("[info] using flag --parallel=%d\n", parallel)
	} else {
		val := PromptInt("Concurrent workers (--parallel, auto if empty): ")
		if val <= 0 {
			switch {
			case target <= 10000:
				parallel = 2
			case target <= 100000:
				parallel = 4
			default:
				parallel = 8
			}
			fmt.Printf("[info] using default/auto --parallel=%d\n", parallel)
		} else {
			if val < 1 {
				val = 1
			} else if val > 16 {
				val = 16
			}
			parallel = val
			fmt.Printf("[info] using input --parallel=%d\n", parallel)
		}
	}

	// --- uri ---
	if WasFlagPassed("mongodb-uri") {
		if uri == "" {
			uri = "mongodb://localhost:27017"
		}
		fmt.Printf("[info] using flag --mongodb-uri=%s\n", uri)
	} else {
		val := PromptString("MongoDB URI (--mongodb-uri, default mongodb://localhost:27017): ")
		if val == "" {
			uri = "mongodb://localhost:27017"
			fmt.Printf("[info] using default --mongodb-uri=%s\n", uri)
		} else {
			uri = val
			fmt.Printf("[info] using input --mongodb-uri=%s\n", uri)
		}
	}

	// --- no-index (boolean) ---
	if WasFlagPassed("no-index") {
		fmt.Printf("[info] using flag --no-index=%v\n", noIndex)
	} else {
		val := PromptBool("Skip index creation? (--no-index y/N, default N): ", false)
		noIndex = val
		fmt.Printf("[info] using input --no-index=%v\n", noIndex)
	}

	// --- user-id (decides user creation flow) ---
	if WasFlagPassed("user-id") {
		fmt.Printf("[info] using flag --user-id=%s\n", userId)
	} else {
		val := PromptString("Existing user ID (--user-id, empty to create new): ")
		userId = val
		if userId == "" {
			fmt.Printf("[info] no --user-id provided, will create new user\n")
		} else {
			fmt.Printf("[info] using input --user-id=%s\n", userId)
		}
	}

	// --- user details if creating new user ---
	if userId == "" {
		// user-name
		if WasFlagPassed("user-name") {
			if userName == "" {
				userName = "media-user"
			}
			fmt.Printf("[info] using flag --user-name=%s\n", userName)
		} else {
			if userName == "" {
				userName = utils.GenerateRandomUsername("media-")
				fmt.Printf("[info] generated random --user-name=%s\n", userName)
			} else {
				fmt.Printf("[info] using input --user-name=%s\n", userName)
			}
		}

		// user-password
		if WasFlagPassed("user-password") {
			if userPassword == "" {
				userPassword = "media-password"
			}
			fmt.Printf("[info] using flag --user-password=[hidden]\n")
		} else {
			val := PromptString("User password (--user-password, default media-password): ")
			if val == "" {
				userPassword = "media-password"
				fmt.Printf("[info] using default --user-password=[hidden]\n")
			} else {
				userPassword = val
				fmt.Printf("[info] using input --user-password=[hidden]\n")
			}
		}

		// user-email
		if WasFlagPassed("user-email") {
			if userEmail == "" {
				userEmail = "example-media-user@email.com"
			}
			fmt.Printf("[info] using flag --user-email=%s\n", userEmail)
		} else {
			val := PromptString("User email (--user-email, default example-media-user@email.com): ")
			if val == "" {
				userEmail = "example-media-user@email.com"
				fmt.Printf("[info] using default --user-email=%s\n", userEmail)
			} else {
				userEmail = val
				fmt.Printf("[info] using input --user-email=%s\n", userEmail)
			}
		}
	}

	// --- device-count ---
	if WasFlagPassed("device-count") {
		if deviceCount < 1 {
			deviceCount = 1
		} else if deviceCount > 50 {
			deviceCount = 50
		}
		fmt.Printf("[info] using flag --device-count=%d\n", deviceCount)
	} else {
		val := PromptInt("Number of devices (--device-count, default 1, max 50): ")
		if val <= 0 {
			deviceCount = 1
			fmt.Printf("[info] using default --device-count=%d\n", deviceCount)
		} else {
			if val > 50 {
				val = 50
			}
			deviceCount = val
			fmt.Printf("[info] using input --device-count=%d\n", deviceCount)
		}
	}

	// reportEvery (legacy, optional)
	if WasFlagPassed("report-every") {
		fmt.Printf("[info] using flag --report-every=%d\n", reportEvery)
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
		fmt.Printf("[info] created new user & enterprise subscription\n")
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
	fmt.Printf("[info] inserted %d devices\n", len(deviceDocs))

	// -------- Media collection & indexes --------
	mediaColl := client.Database(dbName).Collection(mediaCollName)
	if !noIndex {
		database.CreateIndexes(ctx, mediaColl)
		fmt.Printf("[info] indexes ensured\n")
	} else {
		fmt.Printf("[info] skipping index creation (--no-index)\n")
	}

	// -------- Batch channel & workers --------
	var (
		totalInserted int64
		batchCounter  int64
		startTime     = time.Now()
		batchCh       = make(chan []interface{}, parallel*2)
	)

	uiprogress.Start()
	bar := uiprogress.AddBar(target).AppendCompleted().PrependElapsed()
	bar.PrependFunc(func(b *uiprogress.Bar) string {
		inserted := atomic.LoadInt64(&totalInserted)
		pct := 100 * float64(inserted) / float64(target)
		elapsed := time.Since(startTime).Seconds()
		rate := float64(inserted)
		if elapsed > 0 {
			rate = float64(inserted) / elapsed
		}
		return fmt.Sprintf("Inserted %d/%d (%.1f%%) %.0f/s", inserted, target, pct, rate)
	})

	progressCh := make(chan int, parallel*2)

	doneWorkers := make(chan struct{})
	go func() {
		database.RunBatchWorkers(ctx, parallel, mediaColl, batchCh, &stopFlag, &totalInserted, progressCh)
		close(doneWorkers)
	}()

	go func() {
		for total := range progressCh {
			if total > target {
				total = target
			}
			bar.Set(total)
		}
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
	}

	// Finish
	close(batchCh)
	<-doneWorkers
	finalTotal := int(atomic.LoadInt64(&totalInserted))
	if finalTotal > target {
		finalTotal = target
	}
	bar.Set(finalTotal)
	uiprogress.Stop()

	// -------- Summary --------
	if err := client.Disconnect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "[warn] disconnect MongoDB: %v\n", err)
	}
	elapsed := time.Since(startTime).Seconds()
	rate := float64(atomic.LoadInt64(&totalInserted)) / elapsed
	fmt.Printf("[done] inserted=%d elapsed=%.2fs rate=%.0f/s stop_flag=%v batches=%d\n",
		atomic.LoadInt64(&totalInserted), elapsed, rate, atomic.LoadInt32(&stopFlag) != 0, batchCounter)

	if atomic.LoadInt32(&stopFlag) != 0 {
		os.Exit(130)
	} else {
		os.Exit(0)
	}
}
