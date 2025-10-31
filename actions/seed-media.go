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
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/crypto/bcrypt"
)

// Pools (static)
var (
	DETECTION_POOL = []string{
		"animal", "pedestrian", "cyclist", "motorbike", "lorry", "car", "handbag", "suitcase", "cell phone",
	}
	TAG_POOL = []string{
		"outdoor", "indoor", "evening", "sunny", "crowd", "single-subject", "normal", "rainy", "night", "vehicle",
		"urban", "rural", "busy", "quiet", "sports", "event", "construction", "park", "school", "shopping", "office",
		"residential", "traffic", "festival", "emergency", "public-transport", "parking-lot", "playground", "market",
		"bridge", "tunnel",
	}
	COLOR_POOL = []string{"red", "blue", "green", "gray", "black", "white", "yellow"}
	VIDEO_POOL = []string{
		"demo/1751987393_3-641_falcon_420-234-408-321_397_29896.mp4",
		"demo/1751987410_3-505_dublin_1596-648-78-118_1105_26520.mp4",
		"demo/1751987440_3-425_dublin_1594-708-57-29_1252_29880.mp4",
		"demo/1751987476_3-482_nashville_1134-654-205-45_691_30440.mp4",
		"demo/1751987663_3-913_falcon_622-257-301-322_7130_29897.mp4",
		"demo/1751987924_3-818_nashville_651-649-688-332_9458_30394.mp4",
	}
)

var stopFlag int32
var DAYS = 30

func HandleSignals() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\n[signal] Received interrupt, stopping after current batches...")
		atomic.StoreInt32(&stopFlag, 1)
	}()
}

func InsertSettings(settingsColl *mongo.Collection) error {
	settingsDoc := bson.M{
		"key": "plan",
		"map": bson.M{
			"basic": bson.M{
				"level":         1,
				"uploadLimit":   100,
				"videoLimit":    100,
				"usage":         500,
				"analysisLimit": 0,
				"dayLimit":      3,
			},
			"premium": bson.M{
				"level":         2,
				"uploadLimit":   500,
				"videoLimit":    500,
				"usage":         1000,
				"analysisLimit": 0,
				"dayLimit":      7,
			},
			"gold": bson.M{
				"level":         3,
				"uploadLimit":   1000,
				"videoLimit":    1000,
				"usage":         3000,
				"analysisLimit": 1000,
				"dayLimit":      30,
			},
			"business": bson.M{
				"level":         4,
				"uploadLimit":   99999999,
				"videoLimit":    99999999,
				"usage":         10000,
				"analysisLimit": 1000,
				"dayLimit":      30,
			},
			"enterprise": bson.M{
				"level":         5,
				"uploadLimit":   99999999,
				"videoLimit":    99999999,
				"usage":         99999999,
				"analysisLimit": 5000,
				"dayLimit":      30,
			},
		},
	}

	fmt.Println("[info] Inserting settings document...")
	_, err := settingsColl.InsertOne(context.Background(), settingsDoc)
	if err != nil {
		return fmt.Errorf("inserting settings document: %w", err)
	}
	fmt.Println("[info] Settings document inserted successfully.")
	return nil
}

func InsertUser(userColl *mongo.Collection, userName, userEmail, userPassword string, amazonSecretAccessKey, amazonAccessKeyID string, days int) (primitive.ObjectID, error) {
	hashedPassword, err := Hash(userPassword)
	if err != nil {
		return primitive.NilObjectID, fmt.Errorf("hashing password: %w", err)
	}
	now := time.Now()
	userObjectID := primitive.NewObjectID()

	var dates []string
	if days > 0 {
		dates = make([]string, days)
		for i := 0; i < days; i++ {
			date := time.Now().AddDate(0, 0, -i).Format("02-01-2006")
			dates[i] = date
		}
	}

	userDoc := bson.M{
		"_id":                      userObjectID,
		"username":                 userName,
		"email":                    userEmail,
		"password":                 hashedPassword,
		"role":                     "owner",
		"google2fa_enabled":        false,
		"timezone":                 "Europe/Brussels",
		"isActive":                 int64(1),
		"registerToken":            "",
		"updated_at":               now,
		"created_at":               now,
		"amazon_secret_access_key": amazonSecretAccessKey,
		"amazon_access_key_id":     amazonAccessKeyID,
		"card_brand":               "Visa",
		"card_last_four":           "0000",
		"card_status":              "ok",
		"card_status_message":      nil,
		"days":                     dates,
	}
	_, err = userColl.InsertOne(context.Background(), userDoc)
	if err != nil {
		return primitive.NilObjectID, fmt.Errorf("creating user: %w", err)
	}
	fmt.Printf("[info] Created user %s\n", userObjectID.Hex())
	return userObjectID, nil
}

func InsertSubscription(subColl *mongo.Collection, userObjectID string) (string, error) {

	subDoc := bson.M{
		"_id":           primitive.NewObjectID(),
		"name":          "default",
		"stripe_id":     "sub_9ECyjjMz3R7etK",
		"stripe_plan":   "enterprise",
		"quantity":      1,
		"trial_ends_at": nil,
		"ends_at":       nil,
		"user_id":       userObjectID,
		"updated_at":    time.Now(),
		"created_at":    time.Now(),
		"stripe_status": "active",
	}
	_, err := subColl.InsertOne(context.Background(), subDoc)
	if err != nil {
		return "", fmt.Errorf("creating subscription: %w", err)
	}
	fmt.Printf("[info] Created subscription for user %s\n", userObjectID)
	return userObjectID, nil
}

func CreateUser(
	ctx context.Context,
	userColl *mongo.Collection,
	subColl *mongo.Collection,
	userName, userEmail, userPassword string,
	amazonSecretAccessKey, amazonAccessKeyID string,
) (primitive.ObjectID, error) {

	userObjectID, err := InsertUser(userColl, userName, userEmail, userPassword, amazonSecretAccessKey, amazonAccessKeyID, DAYS)
	if err != nil {
		return primitive.NilObjectID, err
	}

	_, err = InsertSubscription(subColl, userObjectID.Hex())
	if err != nil {
		return primitive.NilObjectID, err
	}

	return userObjectID, nil
}

func SampleUnique(pool []string, n int) []string {
	if n <= 0 {
		return []string{}
	}
	if n > len(pool) {
		n = len(pool)
	}
	perm := rand.Perm(len(pool))
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = pool[perm[i]]
	}
	return out
}

func BuildDeviceDocs(deviceCount int, userObjectID primitive.ObjectID, amazonSecretAccessKey string) ([]interface{}, []primitive.ObjectID) {
	devices := make([]interface{}, 0, deviceCount)
	deviceIDs := make([]primitive.ObjectID, 0, deviceCount)
	for i := 0; i < deviceCount; i++ {
		deviceID := primitive.NewObjectID()
		deviceDoc := bson.M{
			"_id":     deviceID,
			"key":     amazonSecretAccessKey,
			"user_id": userObjectID.Hex(),
			"status":  "inactive",
			"featurePermissions": bson.M{
				"ptz":           0,
				"liveview":      0,
				"remote_config": 0,
			},
			"isActive": false,
			"analytics": []bson.M{
				{
					"cloudpublickey":  amazonSecretAccessKey,
					"encrypted":       false,
					"encrypteddata":   []byte{}, // or primitive.Binary{Subtype: 0, Data: []byte{}}
					"key":             fmt.Sprintf("camera%d", i+1),
					"hub_encryption":  "true",
					"e2e_encryption":  "false",
					"enterprise":      false,
					"hash":            "",
					"version":         "3.5.0",
					"release":         "1f9772d",
					"mac_list":        []string{},
					"ip_list":         []string{"192.168.1.100", "10.0.0.100"},
					"cameraname":      fmt.Sprintf("camera%d", 100+i),
					"cameratype":      "IPCamera",
					"architecture":    "x86_64",
					"hostname":        fmt.Sprintf("host-%d", i+1),
					"freeMemory":      "481275904",
					"totalMemory":     "16515977216",
					"usedMemory":      "16034701312",
					"processMemory":   "65097728",
					"kubernetes":      false,
					"docker":          true,
					"kios":            false,
					"raspberrypi":     false,
					"uptime":          "1 day ",
					"boot_time":       "1 day ",
					"timestamp":       time.Now().Unix(),
					"onvif":           "false",
					"onvif_zoom":      "false",
					"onvif_pantilt":   "false",
					"onvif_presets":   "false",
					"cameraConnected": "false",
					"hasBackChannel":  "false",
				},
			},
		}
		devices = append(devices, deviceDoc)
		deviceIDs = append(deviceIDs, deviceID)
	}
	return devices, deviceIDs
}

func BuildBatchDocs(n int, days int, userObjectID primitive.ObjectID, deviceIDs []primitive.ObjectID) []interface{} {
	if days < 1 {
		days = 1
	}
	docs := make([]interface{}, 0, n)
	secondsInDays := int64(days) * 86400
	now := time.Now().Unix()
	for i := 0; i < n; i++ {
		offset := rand.Int63n(secondsInDays)
		st := now - offset
		en := st + int64(rand.Intn(21)+5)
		deviceID := deviceIDs[rand.Intn(len(deviceIDs))]
		doc := bson.M{
			"_id":             primitive.NewObjectID(),
			"startTimestamp":  st,
			"endTimestamp":    en,
			"duration":        en - st,
			"deviceId":        deviceID,
			"organisationId":  fmt.Sprintf("ORG-%03d", rand.Intn(100)+1),
			"storageSolution": "kstorage",
			"videoProvider":   "azure-production",
			"videoFile":       VIDEO_POOL[rand.Intn(len(VIDEO_POOL))],
			"analysisId":      fmt.Sprintf("AN-%06d", rand.Intn(5001)),
			"description":     "synthetic media sample for load test",
			"detections":      SampleUnique(DETECTION_POOL, rand.Intn(4)+1),
			"dominantColors":  SampleUnique(COLOR_POOL, rand.Intn(3)+1),
			"count":           rand.Intn(11) - 5,
			"tags":            SampleUnique(TAG_POOL, rand.Intn(4)+1),
			"metadata": bson.M{
				"tags":            SampleUnique(TAG_POOL, rand.Intn(3)+1),
				"classifications": []string{"normal_activity"},
			},
			"userId": userObjectID.Hex(),
		}
		docs = append(docs, doc)
	}
	return docs
}

func InsertBatch(ctx context.Context, col *mongo.Collection, docs []interface{}) int {
	if len(docs) == 0 {
		return 0
	}
	_, err := col.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	if err != nil {
		fmt.Printf("[warn] batch error: %v\n", err)
		return 0
	}
	return len(docs)
}

func CreateIndexes(ctx context.Context, col *mongo.Collection) {
	indexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "startTimestamp", Value: 1}}},
		{Keys: bson.D{{Key: "deviceId", Value: 1}}},
		{Keys: bson.D{{Key: "organisationId", Value: 1}}},
		{Keys: bson.D{{Key: "tags", Value: 1}}},
		{Keys: bson.D{{Key: "detections", Value: 1}}},
		{Keys: bson.D{{Key: "duration", Value: 1}}},
	}
	_, err := col.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		fmt.Printf("[info] index creation skipped: %v\n", err)
	}
}

func GenerateKey(keyType string, userColl *mongo.Collection) (string, error) {
	const keyLen = 20
	maxRetries := 10
	for attempts := 0; attempts < maxRetries; attempts++ {
		// Generate random bytes
		b := make([]byte, keyLen)
		_, err := rand.Read(b)
		if err != nil {
			return "", err
		}
		key := strings.TrimRight(base32.StdEncoding.EncodeToString(b), "=")
		// Check for duplicates
		isDup, err := IsDuplicateKeyInMongodb(keyType, key, userColl)
		if err != nil {
			return "", err
		}
		if !isDup {
			return key, nil
		}
	}
	return "", fmt.Errorf("failed to generate unique key after max retries")
}

func IsDuplicateKeyInMongodb(keyType string, keyValue string, userColl *mongo.Collection) (bool, error) {
	var filter bson.M
	switch keyType {
	case "public":
		filter = bson.M{"amazon_access_key_id": keyValue}
	case "private":
		filter = bson.M{"amazon_secret_access_key": keyValue}
	default:
		return false, fmt.Errorf("invalid key type: %s", keyType)
	}
	count, err := userColl.CountDocuments(context.Background(), filter)
	if err != nil {
		return false, err
	}
	return count > 0, nil
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

func SeedMedia() {
	rand.Seed(time.Now().UnixNano())
	HandleSignals()

	var (
		target               = flag.Int("target", 0, "Total documents to insert (required)")
		batchSize            = flag.Int("batch", 0, "Documents per batch (required)")
		parallel             = flag.Int("parallel", 0, "Concurrent batch workers (required)")
		uri                  = flag.String("uri", "", "MongoDB URI (required)")
		dbName               = flag.String("db", "", "Database name (required)")
		mediaCollName        = flag.String("media-collection", "", "Media collection name (required)")
		userCollName         = flag.String("user-collection", "", "User collection name")
		deviceCollName       = flag.String("device-collection", "", "Device collection name")
		subscriptionCollName = flag.String("subscription-collection", "", "Subscription collection name")
		settingsCollName     = flag.String("settings-collection", "settings", "Settings collection name")
		noIndex              = flag.Bool("no-index", false, "Skip index creation")
		reportEvery          = flag.Int("report-every", 10, "Report progress every N batches")
		userId               = flag.String("user-id", "", "User ID to linki media to")
		userName             = flag.String("user-name", "", "User name for the media user")
		userPassword         = flag.String("user-password", "", "User password for the media user")
		userEmail            = flag.String("user-email", "", "User email for the media user")

		deviceCount = flag.Int("device-count", 0, "Number of devices to simulate")
	)
	flag.Parse()

	fmt.Printf("[info] Skip any prompt to use default values.")

	if !WasFlagPassed("target") {
		*target = PromptInt("Enter total documents to insert (--target): ")
		if *target == 0 {
			*target = 100000 // default target
			fmt.Printf("[info] Using default target: %d\n", *target)
		}
	}

	if !WasFlagPassed("batch") {
		*batchSize = PromptInt("Enter documents per batch (--batch) (1-100,000): ")
		if *batchSize == 0 {
			switch {
			case *target <= 10000:
				*batchSize = 500
			case *target <= 100000:
				*batchSize = 2000
			default:
				*batchSize = 5000
			}
			fmt.Printf("[info] Using recommended batch size: %d\n", *batchSize)
		}
	} else if *batchSize < 0 {
		*batchSize = 1
	} else if *batchSize > 100000 {
		*batchSize = 100000
	}

	if !WasFlagPassed("parallel") {
		*parallel = PromptInt("Enter concurrent batch workers (--parallel) (1-16): ")
		if *parallel == 0 {
			switch {
			case *target <= 10000:
				*parallel = 2
			case *target <= 100000:
				*parallel = 4
			default:
				*parallel = 8
			}
			fmt.Printf("[info] Using recommended parallel value: %d\n", *parallel)
		}
	} else if *parallel < 1 {
		*parallel = 1
	} else if *parallel > 16 {
		*parallel = 16
	}

	if !WasFlagPassed("uri") {
		*uri = PromptString("Enter MongoDB URI (--uri): ")
		if *uri == "" {
			*uri = "mongodb://localhost:27017"
			fmt.Printf("[info] Using default MongoDB URI: %s\n", *uri)
		}
	}
	if !WasFlagPassed("db") {
		*dbName = PromptString("Enter database name (--db): ")
		if *dbName == "" {
			*dbName = "Kerberos"
			fmt.Printf("[info] Using default database name: %s\n", *dbName)
		}
	}
	if !WasFlagPassed("media-collection") {
		*mediaCollName = PromptString("Enter target media collection name (--media-collection): ")
		if *mediaCollName == "" {
			*mediaCollName = "media"
			fmt.Printf("[info] Using default media collection name: %s\n", *mediaCollName)
		}
	}
	if !WasFlagPassed("user-id") {
		*userId = PromptString("Enter user ID (--user-id), keep empty to create new user: ")
	}
	if !WasFlagPassed("user-id") && *userId == "" {
		if *userName == "" {
			*userName = PromptString("Enter user name (--user-name): ")
			if *userName == "" {
				*userName = "media-user"
				fmt.Printf("[info] Using default user name: %s\n", *userName)
			}
		}
		if *userPassword == "" {
			*userPassword = PromptString("Enter user password (--user-password): ")
			if *userPassword == "" {
				*userPassword = "media-password"
				fmt.Printf("[info] Using default user password: %s\n", *userPassword)
			}
		}
		if *userEmail == "" {
			*userEmail = PromptString("Enter user email (--user-email): ")
			if *userEmail == "" {
				*userEmail = "example-media-user@email.com"
				fmt.Printf("[info] Using default user email: %s\n", *userEmail)
			}
		}
		if !WasFlagPassed("settings-collection") {
			*settingsCollName = PromptString("Enter target settings collection name (--settings-collection): ")
			if *settingsCollName == "" {
				*settingsCollName = "settings"
				fmt.Printf("[info] Using default settings collection name: %s\n", *settingsCollName)
			}
		}
		if *subscriptionCollName == "" {
			*subscriptionCollName = PromptString("Enter target subscription collection name, needed to log in (--subscription-collection): ")
			if *subscriptionCollName == "" {
				*subscriptionCollName = "subscriptions"
				fmt.Printf("[info] Using default subscription collection name: %s\n", *subscriptionCollName)
			}
		}
	} else {
		fmt.Printf("[info] Using existing user ID: %s\n", *userId)
	}
	if !WasFlagPassed("user-collection") {
		*userCollName = PromptString("Enter target user collection name (--user-collection): ")
		if *userCollName == "" {
			*userCollName = "users"
			fmt.Printf("[info] Using default user collection name: %s\n", *userCollName)
		}
	}
	if !WasFlagPassed("device-collection") {
		*deviceCollName = PromptString("Enter target device collection name (--device-collection): ")
		if *deviceCollName == "" {
			*deviceCollName = "devices"
			fmt.Printf("[info] Using default device collection name: %s\n", *deviceCollName)
		}
	}
	if !WasFlagPassed("device-count") {
		*deviceCount = PromptInt("Enter number of devices to simulate (--device-count) (1-50): ")
		if *deviceCount < 1 {
			*deviceCount = 1
		} else if *deviceCount > 50 {
			*deviceCount = 50
		}
		if *deviceCount == 0 {
			*deviceCount = 2
			fmt.Printf("[info] Using default device count: %d\n", *deviceCount)
		}
	}

	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(*uri).SetServerSelectionTimeout(10*time.Second))
	if err != nil {
		fmt.Printf("[error] MongoDB connect: %v\n", err)
		os.Exit(1)
	}

	userColl := client.Database(*dbName).Collection(*userCollName)
	subColl := client.Database(*dbName).Collection(*subscriptionCollName)
	mediaColl := client.Database(*dbName).Collection(*mediaCollName)
	settingsColl := client.Database(*dbName).Collection(*settingsCollName)

	// Ensure settings document exists
	var settingsDoc bson.M
	err = settingsColl.FindOne(ctx, bson.M{"key": "plan", "map.enterprise": bson.M{"$exists": true}}).Decode(&settingsDoc)
	if err == mongo.ErrNoDocuments {
		if err := InsertSettings(settingsColl); err != nil {
			fmt.Printf("[error] Inserting settings: %v\n", err)
			os.Exit(1)
		}
	} else if err != nil {
		fmt.Printf("[error] Checking settings: %v\n", err)
		os.Exit(1)
	}

	var userObjectID primitive.ObjectID
	var amazonSecretAccessKey, amazonAccessKeyID string

	if *userId != "" {
		// Fetch the user and use their keys
		userObjectID, err = primitive.ObjectIDFromHex(*userId)
		if err != nil {
			fmt.Printf("[error] Invalid userId: %v\n", err)
			os.Exit(1)
		}
		var userDoc bson.M
		err = userColl.FindOne(ctx, bson.M{"_id": userObjectID}).Decode(&userDoc)
		if err != nil {
			fmt.Printf("[error] Could not find user with id %s: %v\n", *userId, err)
			os.Exit(1)
		}
		amazonSecretAccessKey, _ = userDoc["amazon_secret_access_key"].(string)
		amazonAccessKeyID, _ = userDoc["amazon_access_key_id"].(string)
		if amazonSecretAccessKey == "" || amazonAccessKeyID == "" {
			fmt.Printf("[error] User is missing AWS keys.\n")
			os.Exit(1)
		}
	} else {
		amazonSecretAccessKey, err = GenerateKey("private", userColl)
		if err != nil {
			fmt.Printf("[error] Generating amazon_secret_access_key: %v\n", err)
			os.Exit(1)
		}
		amazonAccessKeyID, err = GenerateKey("public", userColl)
		if err != nil {
			fmt.Printf("[error] Generating amazon_access_key_id: %v\n", err)
			os.Exit(1)
		}

		// Create new user with subscription
		userObjectID, err = InsertUser(
			userColl,
			*userName,
			*userEmail,
			*userPassword,
			amazonSecretAccessKey,
			amazonAccessKeyID,
			DAYS,
		)
		if err != nil {
			fmt.Printf("[error] %v\n", err)
			os.Exit(1)
		}

		_, err = InsertSubscription(subColl, userObjectID.Hex())
		if err != nil {
			fmt.Printf("[error] %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("[info] Created new user with enterprise subscription.\n")
	}

	deviceColl := client.Database(*dbName).Collection(*deviceCollName)

	deviceDocs, deviceIDs := BuildDeviceDocs(*deviceCount, userObjectID, amazonSecretAccessKey)
	if len(deviceDocs) > 0 {
		_, err := deviceColl.InsertMany(ctx, deviceDocs)
		if err != nil {
			fmt.Printf("[error] Creating devices: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("[info] Created %d devices for user %s\n", len(deviceDocs), userObjectID.Hex())
	}

	col := mediaColl
	if !*noIndex {
		CreateIndexes(ctx, col)
	}

	var (
		totalInserted int64
		batchCounter  int64
		startTime     = time.Now()
		wg            sync.WaitGroup
		batchCh       = make(chan []interface{}, *parallel*2)
	)

	fmt.Printf("[start] target=%d batch=%d parallel=%d uri=%s db=%s.%s\n",
		*target, *batchSize, *parallel, *uri, *dbName, *mediaCollName)

	// Workers
	for i := 0; i < *parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for docs := range batchCh {
				if atomic.LoadInt32(&stopFlag) != 0 {
					return
				}
				inserted := InsertBatch(ctx, col, docs)
				atomic.AddInt64(&totalInserted, int64(inserted))
			}
		}()
	}

	var totalQueued int64

mainLoop:
	for atomic.LoadInt64(&totalQueued) < int64(*target) && atomic.LoadInt32(&stopFlag) == 0 {
		remaining := int64(*target) - atomic.LoadInt64(&totalQueued)
		current := *batchSize
		if remaining < int64(*batchSize) {
			current = int(remaining)
		}
		docs := BuildBatchDocs(current, DAYS, userObjectID, deviceIDs)
		batchCh <- docs
		atomic.AddInt64(&batchCounter, 1)
		atomic.AddInt64(&totalQueued, int64(current))

		if atomic.LoadInt64(&batchCounter)%int64(*reportEvery) == 0 {
			elapsed := time.Since(startTime).Seconds()
			rate := float64(atomic.LoadInt64(&totalInserted)) / elapsed
			pct := 100 * float64(atomic.LoadInt64(&totalInserted)) / float64(*target)
			fmt.Printf("[progress] batches=%d inserted=%d (%.2f%%) rate=%.0f/s\n",
				atomic.LoadInt64(&batchCounter), atomic.LoadInt64(&totalInserted), pct, rate)
		}
		if atomic.LoadInt32(&stopFlag) != 0 {
			break mainLoop
		}
	}
	close(batchCh)
	wg.Wait()
	if err := client.Disconnect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "[warn] error disconnecting MongoDB: %v\n", err)
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
