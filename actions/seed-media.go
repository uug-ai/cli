package actions

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gosuri/uiprogress"
	"github.com/uug-ai/cli/database"
	"github.com/uug-ai/cli/models"
	"github.com/uug-ai/cli/utils"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	DefaultTarget         = 100000
	SmallTargetThreshold  = 10000
	MediumTargetThreshold = 100000

	DefaultBatchSizeSmall  = 500
	DefaultBatchSizeMedium = 2000
	DefaultBatchSizeLarge  = 5000

	MinParallel        = 1
	MaxParallel        = 16
	AutoParallelSmall  = 2
	AutoParallelMedium = 4
	AutoParallelLarge  = 8

	MinDeviceCount = 1
	MaxDeviceCount = 50

	DefaultDays = 7
	MaxDays     = 30

	ServerSelectionTimeoutSeconds = 10
)

type SeedMediaConfig struct {
	Target               int
	BatchSize            int
	Parallel             int
	URI                  string
	DBName               string
	MediaColl            string
	UserColl             string
	DeviceColl           string
	SubscriptionColl     string
	SettingsColl         string
	NoIndex              bool
	ExistingUserIDHex    string
	NewUserName          string
	NewUserPasswordPlain string
	NewUserEmail         string
	DeviceCount          int
	Days                 int
	UseExistingDevices   bool
}

func (c *SeedMediaConfig) ApplyDefaults() {
	if c.Target <= 0 {
		c.Target = DefaultTarget
	}
	if c.BatchSize <= 0 {
		switch {
		case c.Target <= SmallTargetThreshold:
			c.BatchSize = DefaultBatchSizeSmall
		case c.Target <= MediumTargetThreshold:
			c.BatchSize = DefaultBatchSizeMedium
		default:
			c.BatchSize = DefaultBatchSizeLarge
		}
	}
	if c.BatchSize > c.Target {
		c.BatchSize = c.Target
	}
	if c.Parallel <= 0 {
		switch {
		case c.Target <= SmallTargetThreshold:
			c.Parallel = AutoParallelSmall
		case c.Target <= MediumTargetThreshold:
			c.Parallel = AutoParallelMedium
		default:
			c.Parallel = AutoParallelLarge
		}
	}
	if c.Parallel < MinParallel {
		c.Parallel = MinParallel
	}
	if c.Parallel > MaxParallel {
		c.Parallel = MaxParallel
	}
	if c.URI == "" {
		c.URI = "mongodb://localhost:27017"
	}
	if c.DBName == "" {
		c.DBName = "Kerberos"
	}
	if c.MediaColl == "" {
		c.MediaColl = "media"
	}
	if c.UserColl == "" {
		c.UserColl = "users"
	}
	if c.DeviceColl == "" {
		c.DeviceColl = "devices"
	}
	if c.SubscriptionColl == "" {
		c.SubscriptionColl = "subscriptions"
	}
	if c.SettingsColl == "" {
		c.SettingsColl = "settings"
	}
	if c.DeviceCount <= 0 {
		c.DeviceCount = MinDeviceCount
	}
	if c.DeviceCount > MaxDeviceCount {
		c.DeviceCount = MaxDeviceCount
	}
	if c.Days <= 0 {
		c.Days = DefaultDays
	}
	if c.Days > MaxDays {
		c.Days = MaxDays
	}
	if c.NewUserName == "" && c.ExistingUserIDHex == "" {
		c.NewUserName = utils.GenerateRandomUsername("media")
	}
	if c.NewUserPasswordPlain == "" {
		c.NewUserPasswordPlain = "media-password"
	}
	if c.NewUserEmail == "" {
		c.NewUserEmail = "example-media-user@email.com"
	}
}

func RunSeedMedia(cfg SeedMediaConfig) error {
	HandleSignals()
	cfg.ApplyDefaults()

	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().
		ApplyURI(cfg.URI).
		SetServerSelectionTimeout(ServerSelectionTimeoutSeconds*time.Second))
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}
	defer client.Disconnect(ctx)

	if err := database.EnsureSettings(ctx, client, cfg.DBName, cfg.SettingsColl); err != nil {
		return fmt.Errorf("ensure settings: %w", err)
	}

	var userObjectID primitive.ObjectID
	var amazonSecretAccessKey, amazonAccessKeyID string

	if cfg.ExistingUserIDHex != "" {
		userObjectID, err = primitive.ObjectIDFromHex(cfg.ExistingUserIDHex)
		if err != nil {
			return fmt.Errorf("invalid existing user id: %w", err)
		}
		var userDoc bson.M
		err = client.Database(cfg.DBName).Collection(cfg.UserColl).
			FindOne(ctx, bson.M{"_id": userObjectID}).Decode(&userDoc)
		if err != nil {
			return fmt.Errorf("fetch existing user: %w", err)
		}
		amazonSecretAccessKey, _ = userDoc["amazon_secret_access_key"].(string)
		amazonAccessKeyID, _ = userDoc["amazon_access_key_id"].(string)
		if amazonSecretAccessKey == "" || amazonAccessKeyID == "" {
			return fmt.Errorf("existing user missing keys")
		}
	} else {
		existing := database.GetUserFromMongodb(client, cfg.DBName, cfg.NewUserName, cfg.UserColl)
		if existing.Username != "" {
			return fmt.Errorf("user with name %s already exists", cfg.NewUserName)
		}
		amazonSecretAccessKey, err = database.GenerateKey("private", client, cfg.DBName, cfg.UserColl)
		if err != nil {
			return fmt.Errorf("generate private key: %w", err)
		}
		amazonAccessKeyID, err = database.GenerateKey("public", client, cfg.DBName, cfg.UserColl)
		if err != nil {
			return fmt.Errorf("generate public key: %w", err)
		}
		hashed, err := utils.Hash(cfg.NewUserPasswordPlain)
		if err != nil {
			return fmt.Errorf("hash password: %w", err)
		}
		info := models.InsertUserInfo{
			UserName:              cfg.NewUserName,
			UserEmail:             cfg.NewUserEmail,
			UserPassword:          hashed,
			AmazonSecretAccessKey: amazonSecretAccessKey,
			AmazonAccessKeyID:     amazonAccessKeyID,
			Days:                  cfg.Days,
		}
		uid, userDoc := database.BuildUserDoc(info)
		userObjectID = uid

		res, err := client.Database(cfg.DBName).Collection(cfg.UserColl).InsertOne(ctx, userDoc)
		if err != nil {
			return fmt.Errorf("insert user: %w", err)
		}
		if userObjectID == primitive.NilObjectID {
			if oid, ok := res.InsertedID.(primitive.ObjectID); ok {
				userObjectID = oid
			} else {
				return fmt.Errorf("insert user returned non-ObjectID _id (%T)", res.InsertedID)
			}
		}
		subDoc := database.BuildSubscriptionDoc(userObjectID)
		if err := database.InsertOne(ctx, client, cfg.DBName, cfg.SubscriptionColl, subDoc); err != nil {
			return fmt.Errorf("insert subscription: %w", err)
		}
		fmt.Printf("[info] created new user _id = %s username = %s\n", userObjectID.Hex(), cfg.NewUserName)
	}

	var groups []bson.M
	var deviceKeys []string

	if cfg.UseExistingDevices && cfg.ExistingUserIDHex != "" {
		// Query groups for the user
		groupCursor, err := client.Database(cfg.DBName).Collection("groups").
			Find(ctx, bson.M{"organisationId": userObjectID.Hex()})
		if err != nil {
			return fmt.Errorf("find groups: %w", err)
		}
		defer groupCursor.Close(ctx)
		for groupCursor.Next(ctx) {
			var group bson.M
			if err := groupCursor.Decode(&group); err == nil {
				groups = append(groups, group)
			}
		}
		// Pass groups to batch generator for random group/device selection
	} else {
		// Generate new devices
		deviceDocs, _ := database.BuildDeviceDocs(cfg.DeviceCount, userObjectID, amazonSecretAccessKey)
		if len(deviceDocs) == 0 {
			return fmt.Errorf("no devices generated")
		}
		if err := database.InsertMany(ctx, client, cfg.DBName, cfg.DeviceColl, deviceDocs); err != nil {
			return fmt.Errorf("insert devices: %w", err)
		}
		// Collect device keys from generated devices
		for _, doc := range deviceDocs {
			if m, ok := doc.(bson.M); ok {
				if key, ok := m["key"].(string); ok {
					deviceKeys = append(deviceKeys, key)
				}
			}
		}
		// Pass deviceKeys to batch generator for random device selection
	}

	mediaColl := client.Database(cfg.DBName).Collection(cfg.MediaColl)
	if !cfg.NoIndex {
		database.CreateMediaIndexes(ctx, mediaColl)
	}

	var (
		totalInserted int64
		startTime     = time.Now()
		batchCh       = make(chan []interface{}, cfg.Parallel*2)
		progressCh    = make(chan int, cfg.Parallel*2)
	)

	uiprogress.Start()
	bar := uiprogress.AddBar(cfg.Target).AppendCompleted().PrependElapsed()
	bar.PrependFunc(func(b *uiprogress.Bar) string {
		inserted := atomic.LoadInt64(&totalInserted)
		pct := 100 * float64(inserted) / float64(cfg.Target)
		el := time.Since(startTime).Seconds()
		rate := float64(inserted)
		if el > 0 {
			rate = float64(inserted) / el
		}
		return fmt.Sprintf("Inserted %d/%d (%.1f%%) %.0f/s", inserted, cfg.Target, pct, rate)
	})

	doneWorkers := make(chan struct{})
	go func() {
		database.RunBatchWorkers(ctx, cfg.Parallel, mediaColl, batchCh, &stopFlag, &totalInserted, progressCh)
		close(doneWorkers)
	}()

	go func() {
		for total := range progressCh {
			if total > cfg.Target {
				total = cfg.Target
			}
			bar.Set(total)
		}
	}()

	var queued int64

	for atomic.LoadInt64(&queued) < int64(cfg.Target) && atomic.LoadInt32(&stopFlag) == 0 {
		remaining := int64(cfg.Target) - atomic.LoadInt64(&queued)
		current := cfg.BatchSize
		if remaining < int64(current) {
			current = int(remaining)
		}
		var docs []interface{}
		if cfg.UseExistingDevices && cfg.ExistingUserIDHex != "" {
			docs = database.GenerateBatchMediaWithGroups(current, cfg.Days, userObjectID, groups)
		} else {
			docs = database.GenerateBatchMedia(current, cfg.Days, userObjectID, deviceKeys)
		}
		batchCh <- docs
		atomic.AddInt64(&queued, int64(current))
	}

	close(batchCh)
	<-doneWorkers
	finalTotal := int(atomic.LoadInt64(&totalInserted))
	if finalTotal > cfg.Target {
		finalTotal = cfg.Target
	}
	bar.Set(finalTotal)
	uiprogress.Stop()

	el := time.Since(startTime).Seconds()
	rate := float64(finalTotal) / el
	fmt.Printf("[done] inserted=%d elapsed=%.2fs rate=%.0f/s stop=%v\n",
		finalTotal, el, rate, atomic.LoadInt32(&stopFlag) != 0)

	if atomic.LoadInt32(&stopFlag) != 0 {
		return fmt.Errorf("interrupted")
	}
	return nil
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
	days int,
) {
	HandleSignals()
	flag.Parse()

	// --- db (no prompt by design) ---
	if WasFlagPassed("db") {
		fmt.Printf("[info] using flag -db=%s\n", dbName)
	} else {
		fmt.Printf("[info] using default -db=%s (no flag)\n", dbName)
	}
	// --- collections (no prompt) ---
	if WasFlagPassed("media-collection") {
		fmt.Printf("[info] using flag -media-collection=%s\n", mediaCollName)
	} else {
		fmt.Printf("[info] using default -media-collection=%s\n", mediaCollName)
	}
	if WasFlagPassed("user-collection") {
		fmt.Printf("[info] using flag -user-collection=%s\n", userCollName)
	} else {
		fmt.Printf("[info] using default -user-collection=%s\n", userCollName)
	}
	if WasFlagPassed("device-collection") {
		fmt.Printf("[info] using flag -device-collection=%s\n", deviceCollName)
	} else {
		fmt.Printf("[info] using default -device-collection=%s\n", deviceCollName)
	}
	if WasFlagPassed("subscription-collection") {
		fmt.Printf("[info] using flag -subscription-collection=%s\n", subscriptionCollName)
	} else {
		fmt.Printf("[info] using default -subscription-collection=%s\n", subscriptionCollName)
	}
	if WasFlagPassed("settings-collection") {
		fmt.Printf("[info] using flag -settings-collection=%s\n", settingsCollName)
	} else {
		fmt.Printf("[info] using default -settings-collection=%s\n", settingsCollName)
	}

	// --- target ---
	if WasFlagPassed("target") {
		if target <= 0 {
			target = DefaultTarget
		} else if target > 10_000_000 { // original max clamp
			target = 10_000_000
		}
		fmt.Printf("[info] using flag -target=%d\n", target)
	} else {
		val := PromptInt(fmt.Sprintf("Total documents (-target, default %d, max 10000000): ", DefaultTarget))
		if val <= 0 {
			target = DefaultTarget
			fmt.Printf("[info] using default -target=%d\n", target)
		} else {
			if val > 10_000_000 {
				val = 10_000_000
				fmt.Printf("[warn] clamped -target to %d\n", val)
			}
			target = val
			fmt.Printf("[info] using input -target=%d\n", target)
		}
	}

	// --- batch-size ---
	if WasFlagPassed("batch-size") {
		if batchSize < 1 {
			batchSize = 1
		} else if batchSize > 100_000 {
			batchSize = 100_000
		}
		if batchSize > target {
			batchSize = target
		}
		fmt.Printf("[info] using flag -batch-size=%d\n", batchSize)
	} else {
		val := PromptInt("Documents per batch (-batch-size, auto if empty): ")
		if val <= 0 {
			switch {
			case target <= SmallTargetThreshold:
				batchSize = DefaultBatchSizeSmall
			case target <= MediumTargetThreshold:
				batchSize = DefaultBatchSizeMedium
			default:
				batchSize = DefaultBatchSizeLarge
			}
			if batchSize > target {
				batchSize = target
			}
			fmt.Printf("[info] using auto -batch-size=%d\n", batchSize)
		} else {
			if val > 100_000 {
				val = 100_000
				fmt.Printf("[warn] clamped -batch-size to %d\n", val)
			}
			if val > target {
				val = target
			}
			batchSize = val
			fmt.Printf("[info] using input -batch-size=%d\n", batchSize)
		}
	}

	// --- parallel ---
	if WasFlagPassed("parallel") {
		if parallel < MinParallel {
			parallel = MinParallel
		} else if parallel > MaxParallel {
			parallel = MaxParallel
		}
		fmt.Printf("[info] using flag -parallel=%d\n", parallel)
	} else {
		val := PromptInt("Concurrent workers (-parallel, auto if empty): ")
		if val <= 0 {
			switch {
			case target <= SmallTargetThreshold:
				parallel = AutoParallelSmall
			case target <= MediumTargetThreshold:
				parallel = AutoParallelMedium
			default:
				parallel = AutoParallelLarge
			}
			fmt.Printf("[info] using auto -parallel=%d\n", parallel)
		} else {
			if val < MinParallel {
				val = MinParallel
			} else if val > MaxParallel {
				val = MaxParallel
			}
			parallel = val
			fmt.Printf("[info] using input -parallel=%d\n", parallel)
		}
	}

	// --- days ---
	if WasFlagPassed("days") {
		if days < 1 {
			days = 1
		} else if days > MaxDays {
			days = MaxDays
		}
		fmt.Printf("[info] using flag -days=%d\n", days)
	} else {
		val := PromptInt(fmt.Sprintf("Number of past days (-days, default %d, max %d): ", DefaultDays, MaxDays))
		if val <= 0 {
			days = DefaultDays
			fmt.Printf("[info] using default -days=%d\n", days)
		} else {
			if val > MaxDays {
				val = MaxDays
				fmt.Printf("[warn] clamped -days to %d\n", val)
			}
			days = val
			fmt.Printf("[info] using input -days=%d\n", days)
		}
	}

	// --- mongodb-uri ---
	if WasFlagPassed("mongodb-uri") {
		if uri == "" {
			uri = "mongodb://localhost:27017"
		}
		fmt.Printf("[info] using flag -mongodb-uri=%s\n", uri)
	} else {
		in := PromptString("MongoDB URI (-mongodb-uri, default mongodb://localhost:27017): ")
		if strings.TrimSpace(in) == "" {
			uri = "mongodb://localhost:27017"
			fmt.Printf("[info] using default -mongodb-uri=%s\n", uri)
		} else {
			uri = in
			fmt.Printf("[info] using input -mongodb-uri=%s\n", uri)
		}
	}

	// --- no-index ---
	if WasFlagPassed("no-index") {
		fmt.Printf("[info] using flag -no-index=%v\n", noIndex)
	} else {
		noIndex = PromptBool("Skip index creation? (-no-index y/N, default N): ", false)
		fmt.Printf("[info] using input -no-index=%v\n", noIndex)
	}

	var useExistingDevices bool = false

	// --- user-id (existing vs create new) ---
	if WasFlagPassed("user-id") {
		fmt.Printf("[info] using flag -user-id=%s\n", userId)
	} else {
		in := PromptString("Existing user ID (-user-id, empty to create new): ")
		userId = strings.TrimSpace(in)
		if userId == "" {
			fmt.Println("[info] no -user-id provided; will create new user")
		} else {
			fmt.Printf("[info] using input -user-id=%s\n", userId)
		}
	}

	// --- new user fields if creating ---
	if userId == "" {
		if WasFlagPassed("user-name") {
			if userName == "" {
				userName = "media-user"
			}
			fmt.Printf("[info] using flag -user-name=%s\n", userName)
		} else {
			if userName == "" {
				userName = utils.GenerateRandomUsername("media-")
				fmt.Printf("[info] generated random -user-name=%s\n", userName)
			} else {
				fmt.Printf("[info] using input -user-name=%s\n", userName)
			}
		}

		if WasFlagPassed("user-password") {
			if userPassword == "" {
				userPassword = "media-password"
			}
			fmt.Printf("[info] using flag -user-password=[hidden]\n")
		} else {
			in := PromptString("User password (-user-password, default media-password): ")
			if strings.TrimSpace(in) == "" {
				userPassword = "media-password"
				fmt.Printf("[info] using default -user-password=[hidden]\n")
			} else {
				userPassword = in
				fmt.Printf("[info] using input -user-password=[hidden]\n")
			}
		}

		if WasFlagPassed("user-email") {
			if userEmail == "" {
				userEmail = "example-media-user@email.com"
			}
			fmt.Printf("[info] using flag -user-email=%s\n", userEmail)
		} else {
			in := PromptString("User email (-user-email, default example-media-user@email.com): ")
			if strings.TrimSpace(in) == "" {
				userEmail = "example-media-user@email.com"
				fmt.Printf("[info] using default -user-email=%s\n", userEmail)
			} else {
				userEmail = in
				fmt.Printf("[info] using input -user-email=%s\n", userEmail)
			}
		}
	} else {
		if WasFlagPassed("use-existing-devices") {
			useExistingDevices = true
			fmt.Printf("[info] using flag -use-existing-devices=%v\n", useExistingDevices)
		} else {
			useExistingDevices = PromptBool("Use existing devices for user? (-use-existing-devices Y/n, default Y): ", true)
			fmt.Printf("[info] using input -use-existing-devices=%v\n", useExistingDevices)
		}

		if !useExistingDevices {
			if WasFlagPassed("device-count") {
				if deviceCount < MinDeviceCount {
					deviceCount = MinDeviceCount
				} else if deviceCount > MaxDeviceCount {
					deviceCount = MaxDeviceCount
				}
				fmt.Printf("[info] using flag -device-count=%d\n", deviceCount)
			} else {
				val := PromptInt(fmt.Sprintf("Number of devices (-device-count, default %d, max %d): ", MinDeviceCount, MaxDeviceCount))
				if val <= 0 {
					deviceCount = MinDeviceCount
					fmt.Printf("[info] using default -device-count=%d\n", deviceCount)
				} else {
					if val > MaxDeviceCount {
						val = MaxDeviceCount
						fmt.Printf("[warn] clamped -device-count to %d\n", val)
					}
					deviceCount = val
					fmt.Printf("[info] using input -device-count=%d\n", deviceCount)
				}
			}
		}
	}

	// Build config and run
	cfg := SeedMediaConfig{
		Target:               target,
		BatchSize:            batchSize,
		Parallel:             parallel,
		URI:                  uri,
		DBName:               dbName,
		MediaColl:            mediaCollName,
		UserColl:             userCollName,
		DeviceColl:           deviceCollName,
		SubscriptionColl:     subscriptionCollName,
		SettingsColl:         settingsCollName,
		NoIndex:              noIndex,
		ExistingUserIDHex:    userId,
		NewUserName:          userName,
		NewUserPasswordPlain: userPassword,
		NewUserEmail:         userEmail,
		DeviceCount:          deviceCount,
		Days:                 days,
		UseExistingDevices:   useExistingDevices,
	}
	if err := RunSeedMedia(cfg); err != nil {
		fmt.Printf("[error] seed media: %v\n", err)
		os.Exit(1)
	}
}
