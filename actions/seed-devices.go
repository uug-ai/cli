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
	MinSeedDeviceCount     = 1
	MaxSeedDeviceCount     = 5000
	DefaultSeedDeviceCount = 10

	DefaultDeviceDays = 7
	MaxDeviceDays     = 30

	ServerSelectionTimeoutDevices = 10
)

type SeedDevicesConfig struct {
	Count                int
	URI                  string
	DBName               string
	UserColl             string
	DeviceColl           string
	SubscriptionColl     string
	SettingsColl         string
	ExistingUserIDHex    string
	NewUserName          string
	NewUserPasswordPlain string
	NewUserEmail         string
	UserDays             int
	SkipIndexes          bool
}

func (c *SeedDevicesConfig) ApplyDefaults() {
	if c.Count <= 0 {
		c.Count = DefaultSeedDeviceCount
	}
	if c.Count > MaxSeedDeviceCount {
		c.Count = MaxSeedDeviceCount
	}
	if c.URI == "" {
		c.URI = "mongodb://localhost:27017"
	}
	if c.DBName == "" {
		c.DBName = "Kerberos"
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
	if c.UserDays <= 0 {
		c.UserDays = DefaultDeviceDays
	}
	if c.UserDays > MaxDeviceDays {
		c.UserDays = MaxDeviceDays
	}
	if c.NewUserName == "" && c.ExistingUserIDHex == "" {
		c.NewUserName = utils.GenerateRandomUsername("devices")
	}
	if c.NewUserPasswordPlain == "" {
		c.NewUserPasswordPlain = "devices-password"
	}
	if c.NewUserEmail == "" {
		c.NewUserEmail = "example-devices-user@email.com"
	}
}

func RunSeedDevices(cfg SeedDevicesConfig) error {
	HandleSignals()
	cfg.ApplyDefaults()

	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().
		ApplyURI(cfg.URI).
		SetServerSelectionTimeout(ServerSelectionTimeoutDevices*time.Second))
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}
	defer client.Disconnect(ctx)

	if err := database.EnsureSettings(ctx, client, cfg.DBName, cfg.SettingsColl); err != nil {
		return fmt.Errorf("ensure settings: %w", err)
	}

	var (
		userObjectID          primitive.ObjectID
		amazonSecretAccessKey string
		amazonAccessKeyID     string
		finalUserIDHex        string
	)

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
		finalUserIDHex = userObjectID.Hex()
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
			Days:                  cfg.UserDays,
		}
		userObjectID, userDoc := database.BuildUserDoc(info)
		if userObjectID == primitive.NilObjectID {
			return fmt.Errorf("internal: BuildUserDoc returned nil objectID")
		}
		if err := database.InsertOne(ctx, client, cfg.DBName, cfg.UserColl, userDoc); err != nil {
			return fmt.Errorf("insert user: %w", err)
		}
		subDoc := database.BuildSubscriptionDoc(userObjectID)
		if err := database.InsertOne(ctx, client, cfg.DBName, cfg.SubscriptionColl, subDoc); err != nil {
			return fmt.Errorf("insert subscription: %w", err)
		}
		finalUserIDHex = userObjectID.Hex()
		fmt.Printf("[info] created new user _id=%s username=%s\n", finalUserIDHex, cfg.NewUserName)
	}

	if finalUserIDHex == "" || userObjectID == primitive.NilObjectID {
		return fmt.Errorf("internal: userObjectID became nil before device creation (finalUserIDHex=%q)", finalUserIDHex)
	}

	deviceDocs, _ := database.BuildDeviceDocs(cfg.Count, userObjectID, amazonSecretAccessKey)
	if len(deviceDocs) == 0 {
		return fmt.Errorf("no devices generated")
	}
	if err := database.InsertMany(ctx, client, cfg.DBName, cfg.DeviceColl, deviceDocs); err != nil {
		return fmt.Errorf("insert devices: %w", err)
	}

	uiprogress.Start()
	bar := uiprogress.AddBar(cfg.Count).AppendCompleted().PrependElapsed()
	var inserted int64
	bar.PrependFunc(func(b *uiprogress.Bar) string {
		cur := atomic.LoadInt64(&inserted)
		pct := 100 * float64(cur) / float64(cfg.Count)
		return fmt.Sprintf("Devices %d/%d (%.1f%%)", cur, cfg.Count, pct)
	})
	atomic.StoreInt64(&inserted, int64(cfg.Count))
	bar.Set(cfg.Count)
	uiprogress.Stop()

	// Use finalUserIDHex instead of userObjectID (in case of unexpected mutation).
	fmt.Printf("[done] devices_inserted=%d user_id=%s stop=%v\n",
		cfg.Count, finalUserIDHex, atomic.LoadInt32(&stopFlag) != 0)

	if atomic.LoadInt32(&stopFlag) != 0 {
		return fmt.Errorf("interrupted")
	}
	return nil
}

func SeedDevices(
	count int,
	mongodbURI string,
	dbName string,
	userCollName string,
	deviceCollName string,
	subscriptionCollName string,
	settingsCollName string,
	userId string,
	userName string,
	userPassword string,
	userEmail string,
	userDays int,
	noIndex bool,
) {
	HandleSignals()
	flag.Parse()

	// No prompts for db / collection names (consistent)
	if WasFlagPassed("db") {
		fmt.Printf("[info] using flag -db=%s\n", dbName)
	} else {
		fmt.Printf("[info] using default -db=%s (no flag)\n", dbName)
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

	// mongodb-uri
	if WasFlagPassed("mongodb-uri") {
		if mongodbURI == "" {
			mongodbURI = "mongodb://localhost:27017"
		}
		fmt.Printf("[info] using flag -mongodb-uri=%s\n", mongodbURI)
	} else {
		in := PromptString("MongoDB URI (-mongodb-uri, default mongodb://localhost:27017): ")
		if in == "" {
			mongodbURI = "mongodb://localhost:27017"
			fmt.Printf("[info] using default -mongodb-uri=%s\n", mongodbURI)
		} else {
			mongodbURI = in
			fmt.Printf("[info] using input -mongodb-uri=%s\n", mongodbURI)
		}
	}

	// user-id
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

	// device-count
	if WasFlagPassed("device-count") {
		if count < MinSeedDeviceCount {
			count = MinSeedDeviceCount
		} else if count > MaxSeedDeviceCount {
			count = MaxSeedDeviceCount
		}
		fmt.Printf("[info] using flag -device-count=%d\n", count)
	} else {
		val := PromptInt(fmt.Sprintf("Number of devices (-device-count, default %d, max %d): ", DefaultSeedDeviceCount, MaxSeedDeviceCount))
		if val <= 0 {
			count = DefaultSeedDeviceCount
			fmt.Printf("[info] using default -device-count=%d\n", count)
		} else {
			if val > MaxSeedDeviceCount {
				val = MaxSeedDeviceCount
				fmt.Printf("[warn] clamped -device-count to %d\n", val)
			}
			count = val
			fmt.Printf("[info] using input -device-count=%d\n", count)
		}
	}

	// days (for created user)
	if userId == "" {
		if WasFlagPassed("days") {
			if userDays < 1 {
				userDays = 1
			} else if userDays > MaxDeviceDays {
				userDays = MaxDeviceDays
			}
			fmt.Printf("[info] using flag -days=%d\n", userDays)
		} else {
			val := PromptInt(fmt.Sprintf("Days for new user (-days, default %d, max %d): ", DefaultDeviceDays, MaxDeviceDays))
			if val <= 0 {
				userDays = DefaultDeviceDays
				fmt.Printf("[info] using default -days=%d\n", userDays)
			} else {
				if val > MaxDeviceDays {
					val = MaxDeviceDays
					fmt.Printf("[warn] clamped -days to %d\n", val)
				}
				userDays = val
				fmt.Printf("[info] using input -days=%d\n", userDays)
			}
		}
	}

	// new user fields
	if userId == "" {
		if WasFlagPassed("user-name") {
			if userName == "" {
				userName = "devices-user"
			}
			fmt.Printf("[info] using flag -user-name=%s\n", userName)
		} else {
			if userName == "" {
				userName = utils.GenerateRandomUsername("devices-")
				fmt.Printf("[info] generated random -user-name=%s\n", userName)
			} else {
				fmt.Printf("[info] using input -user-name=%s\n", userName)
			}
		}

		if WasFlagPassed("user-password") {
			if userPassword == "" {
				userPassword = "devices-password"
			}
			fmt.Printf("[info] using flag -user-password=[hidden]\n")
		} else {
			in := PromptString("User password (-user-password, default devices-password): ")
			if strings.TrimSpace(in) == "" {
				userPassword = "devices-password"
				fmt.Printf("[info] using default -user-password=[hidden]\n")
			} else {
				userPassword = in
				fmt.Printf("[info] using input -user-password=[hidden]\n")
			}
		}

		if WasFlagPassed("user-email") {
			if userEmail == "" {
				userEmail = "example-devices-user@email.com"
			}
			fmt.Printf("[info] using flag -user-email=%s\n", userEmail)
		} else {
			in := PromptString("User email (-user-email, default example-devices-user@email.com): ")
			if strings.TrimSpace(in) == "" {
				userEmail = "example-devices-user@email.com"
				fmt.Printf("[info] using default -user-email=%s\n", userEmail)
			} else {
				userEmail = in
				fmt.Printf("[info] using input -user-email=%s\n", userEmail)
			}
		}
	}

	cfg := SeedDevicesConfig{
		Count:                count,
		URI:                  mongodbURI,
		DBName:               dbName,
		UserColl:             userCollName,
		DeviceColl:           deviceCollName,
		SubscriptionColl:     subscriptionCollName,
		SettingsColl:         settingsCollName,
		ExistingUserIDHex:    userId,
		NewUserName:          userName,
		NewUserPasswordPlain: userPassword,
		NewUserEmail:         userEmail,
		UserDays:             userDays,
		SkipIndexes:          noIndex,
	}
	if err := RunSeedDevices(cfg); err != nil {
		fmt.Printf("[error] seed devices: %v\n", err)
		os.Exit(1)
	}
}
