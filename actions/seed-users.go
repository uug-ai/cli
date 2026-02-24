package actions

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/gosuri/uiprogress"
	"github.com/uug-ai/cli/database"
	"github.com/uug-ai/cli/models"
	"github.com/uug-ai/cli/utils"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Constants (mirroring style of seed-media).
const (
	DefaultUserCount = 100
	MaxUserCount     = 10_000_000
	DefaultBatchSize = 1000
	MinBatchSize     = 1
	MaxBatchSize     = 100_000
	DefaultDaysUsers = 7
	MaxDaysUsers     = 30
)

type SeedUsersConfig struct {
	Prefix           string
	Count            int
	MongoURI         string
	DBName           string
	UserColl         string
	SubscriptionColl string
	SettingsColl     string
	FixedDays        int
	UseGeneratedKeys bool
	PasswordFromName bool
	BatchSize        int
}

func (c *SeedUsersConfig) ApplyDefaults() {
	if c.Prefix == "" {
		c.Prefix = "user"
	}
	if c.Count <= 0 {
		c.Count = DefaultUserCount
	} else if c.Count > MaxUserCount {
		c.Count = MaxUserCount
	}
	if c.MongoURI == "" {
		c.MongoURI = "mongodb://localhost:27017"
	}
	if c.DBName == "" {
		c.DBName = "Kerberos_test"
	}
	if c.UserColl == "" {
		c.UserColl = "users"
	}
	if c.SubscriptionColl == "" {
		c.SubscriptionColl = "subscriptions"
	}
	if c.SettingsColl == "" {
		c.SettingsColl = "settings"
	}
	if c.FixedDays <= 0 {
		c.FixedDays = DefaultDaysUsers
	} else if c.FixedDays > MaxDaysUsers {
		c.FixedDays = MaxDaysUsers
	}
	if c.BatchSize <= 0 {
		c.BatchSize = DefaultBatchSize
	} else {
		if c.BatchSize < MinBatchSize {
			c.BatchSize = MinBatchSize
		}
		if c.BatchSize > MaxBatchSize {
			c.BatchSize = MaxBatchSize
		}
		if c.BatchSize > c.Count {
			c.BatchSize = c.Count
		}
	}
}

func RunSeedUsers(cfg SeedUsersConfig) error {
	HandleSignals()
	cfg.ApplyDefaults()

	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().
		ApplyURI(cfg.MongoURI).
		SetServerSelectionTimeout(10*time.Second))
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}
	defer client.Disconnect(ctx)

	if err := database.EnsureSettings(ctx, client, cfg.DBName, cfg.SettingsColl); err != nil {
		return fmt.Errorf("ensure settings: %w", err)
	}

	userColl := client.Database(cfg.DBName).Collection(cfg.UserColl)
	_ = database.CreateUserIndexes(ctx, userColl)

	var inserted int64
	batchSize := cfg.BatchSize
	if cfg.Count < batchSize {
		batchSize = cfg.Count
	}

	uiprogress.Start()
	bar := uiprogress.AddBar(cfg.Count).AppendCompleted().PrependElapsed()
	bar.PrependFunc(func(b *uiprogress.Bar) string {
		cur := atomic.LoadInt64(&inserted)
		pct := 100 * float64(cur) / float64(cfg.Count)
		return fmt.Sprintf("Users %d/%d (%.1f%%)", cur, cfg.Count, pct)
	})

	batch := make([]interface{}, 0, batchSize)
	userIDs := make([]primitive.ObjectID, 0, cfg.Count)
	seen := make(map[string]struct{})

	for i := 0; i < cfg.Count; i++ {
		if atomic.LoadInt32(&stopFlag) != 0 {
			break
		}
		var uname string
		for {
			uname = utils.GenerateRandomUsername(cfg.Prefix)
			if _, ok := seen[uname]; !ok {
				seen[uname] = struct{}{}
				break
			}
		}

		var secretKey, publicKey string
		if cfg.UseGeneratedKeys {
			secretKey, err = database.GenerateKey("private", client, cfg.DBName, cfg.UserColl)
			if err != nil {
				return fmt.Errorf("gen private key: %w", err)
			}
			publicKey, err = database.GenerateKey("public", client, cfg.DBName, cfg.UserColl)
			if err != nil {
				return fmt.Errorf("gen public key: %w", err)
			}
		} else {
			secretKey = "AKIAIOSFODNN7EXAMPLE"
			publicKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
		}

		passBase := uname
		if !cfg.PasswordFromName {
			passBase = "default-password"
		}
		hashed, err := utils.Hash(passBase)
		if err != nil {
			return fmt.Errorf("hash password: %w", err)
		}

		info := models.InsertUserInfo{
			UserName:              uname,
			UserEmail:             uname + "@example.com",
			UserPassword:          hashed,
			AmazonSecretAccessKey: secretKey,
			AmazonAccessKeyID:     publicKey,
			Days:                  cfg.FixedDays,
		}
		uid, doc := database.BuildUserDoc(info)
		batch = append(batch, doc)
		userIDs = append(userIDs, uid)

		if len(batch) == batchSize {
			if err := database.InsertMany(ctx, client, cfg.DBName, cfg.UserColl, batch); err != nil {
				return fmt.Errorf("insert users batch: %w", err)
			}
			atomic.AddInt64(&inserted, int64(len(batch)))
			bar.Set(int(inserted))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := database.InsertMany(ctx, client, cfg.DBName, cfg.UserColl, batch); err != nil {
			return fmt.Errorf("insert users final batch: %w", err)
		}
		atomic.AddInt64(&inserted, int64(len(batch)))
		bar.Set(int(inserted))
	}

	// Subscriptions
	subBatch := make([]interface{}, 0, 1000)
	for _, uid := range userIDs {
		subBatch = append(subBatch, database.BuildSubscriptionDoc(uid))
		if len(subBatch) == 1000 {
			if err := database.InsertMany(ctx, client, cfg.DBName, cfg.SubscriptionColl, subBatch); err != nil {
				return fmt.Errorf("insert subscriptions batch: %w", err)
			}
			subBatch = subBatch[:0]
		}
	}
	if len(subBatch) > 0 {
		if err := database.InsertMany(ctx, client, cfg.DBName, cfg.SubscriptionColl, subBatch); err != nil {
			return fmt.Errorf("insert subscriptions final batch: %w", err)
		}
	}

	uiprogress.Stop()
	fmt.Printf("[done] users_inserted=%d subscriptions=%d stop=%v\n",
		inserted, len(userIDs), atomic.LoadInt32(&stopFlag) != 0)

	if atomic.LoadInt32(&stopFlag) != 0 {
		return fmt.Errorf("interrupted")
	}
	return nil
}

func SeedUsers(
	prefix string,
	count int,
	mongodbURI string,
	dbName string,
	userCollName string,
	subscriptionCollName string,
	settingsCollName string,
) {
	HandleSignals()
	flag.Parse()

	// db and collection names: no prompts (consistent with media)
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

	// user-prefix
	if WasFlagPassed("user-prefix") {
		if prefix == "" {
			prefix = "user"
		}
		fmt.Printf("[info] using flag -user-prefix=%s\n", prefix)
	} else {
		in := PromptString("User prefix (-user-prefix, default user): ")
		if in == "" {
			prefix = "user"
			fmt.Printf("[info] using default -user-prefix=%s\n", prefix)
		} else {
			prefix = in
			fmt.Printf("[info] using input -user-prefix=%s\n", prefix)
		}
	}

	// user-count
	if WasFlagPassed("user-count") {
		if count <= 0 {
			count = DefaultUserCount
		} else if count > MaxUserCount {
			count = MaxUserCount
		}
		fmt.Printf("[info] using flag -user-count=%d\n", count)
	} else {
		in := PromptInt(fmt.Sprintf("User count (-user-count, default %d, max %d): ", DefaultUserCount, MaxUserCount))
		if in <= 0 {
			count = DefaultUserCount
			fmt.Printf("[info] using default -user-count=%d\n", count)
		} else {
			if in > MaxUserCount {
				in = MaxUserCount
				fmt.Printf("[warn] clamped -user-count to %d\n", in)
			}
			count = in
			fmt.Printf("[info] using input -user-count=%d\n", count)
		}
	}

	// batch-size (optional prompt)
	var batchSize int
	if WasFlagPassed("batch-size") {
		batchSize = DefaultBatchSize // flag value would be wired externally
		if batchSize < MinBatchSize {
			batchSize = MinBatchSize
		} else if batchSize > MaxBatchSize {
			batchSize = MaxBatchSize
		}
		if batchSize > count {
			batchSize = count
		}
		fmt.Printf("[info] using flag -batch-size=%d\n", batchSize)
	} else {
		val := PromptInt(fmt.Sprintf("Batch size (-batch-size, default %d, max %d, auto if empty): ", DefaultBatchSize, MaxBatchSize))
		if val <= 0 {
			batchSize = DefaultBatchSize
			if batchSize > count {
				batchSize = count
			}
			fmt.Printf("[info] using default -batch-size=%d\n", batchSize)
		} else {
			if val > MaxBatchSize {
				val = MaxBatchSize
				fmt.Printf("[warn] clamped -batch-size to %d\n", val)
			}
			if val > count {
				val = count
			}
			batchSize = val
			fmt.Printf("[info] using input -batch-size=%d\n", batchSize)
		}
	}

	// days (FixedDays)
	var days = DefaultDaysUsers
	if WasFlagPassed("days") {
		if days < 1 {
			days = 1
		} else if days > MaxDaysUsers {
			days = MaxDaysUsers
		}
		fmt.Printf("[info] using flag -days=%d\n", days)
	} else {
		val := PromptInt(fmt.Sprintf("Days (-days, default %d, max %d): ", DefaultDaysUsers, MaxDaysUsers))
		if val <= 0 {
			days = DefaultDaysUsers
			fmt.Printf("[info] using default -days=%d\n", days)
		} else {
			if val > MaxDaysUsers {
				val = MaxDaysUsers
				fmt.Printf("[warn] clamped -days to %d\n", val)
			}
			days = val
			fmt.Printf("[info] using input -days=%d\n", days)
		}
	}

	// use-generated-keys
	useGenerated := false
	if WasFlagPassed("use-generated-keys") {
		useGenerated = true
		fmt.Printf("[info] using flag -use-generated-keys=%v\n", useGenerated)
	} else {
		useGenerated = PromptBool("Generate unique access keys? (-use-generated-keys y/N, default N): ", false)
		fmt.Printf("[info] using input -use-generated-keys=%v\n", useGenerated)
	}

	// password-from-name
	passFromName := true
	if WasFlagPassed("password-from-name") {
		fmt.Printf("[info] using flag -password-from-name=%v\n", passFromName)
	} else {
		passFromName = PromptBool("Use username as password? (-password-from-name y/N, default Y): ", true)
		fmt.Printf("[info] using input -password-from-name=%v\n", passFromName)
	}

	cfg := SeedUsersConfig{
		Prefix:           prefix,
		Count:            count,
		MongoURI:         mongodbURI,
		DBName:           dbName,
		UserColl:         userCollName,
		SubscriptionColl: subscriptionCollName,
		SettingsColl:     settingsCollName,
		FixedDays:        days,
		UseGeneratedKeys: useGenerated,
		PasswordFromName: passFromName,
		BatchSize:        batchSize,
	}

	if err := RunSeedUsers(cfg); err != nil {
		fmt.Printf("[error] seed users: %v\n", err)
		os.Exit(1)
	}
}
