package actions

import (
	"context"
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

func SeedUsers(
	prefix string,
	count int,
	mongodbURI string,
	dbName string,
	userCollName string,
	subscriptionCollName string,
	settingsCollName string,
) {
	if WasFlagPassed("mongodb-uri") {
		fmt.Printf("[info] using flag -mongodb-uri = %s\n", mongodbURI)
	} else {
		input := PromptString("MongoDB URI (default 'mongodb://localhost:27017'): ")
		if input != "" {
			mongodbURI = input
			fmt.Printf("[info] using input -mongodb-uri = %s\n", mongodbURI)
		} else if mongodbURI == "" {
			mongodbURI = "mongodb://localhost:27017"
			fmt.Printf("[info] using default -mongodb-uri = %s\n", mongodbURI)
		}
	}

	// --- db name ---
	if WasFlagPassed("db-name") {
		fmt.Printf("[info] using flag -db-name = %s\n", dbName)
	} else {
		fmt.Printf("[info] using default -db-name = %s\n", dbName)
	}

	// --- user collection ---
	if WasFlagPassed("user-collection") {
		fmt.Printf("[info] using flag -user-collection = %s\n", userCollName)
	} else {
		fmt.Printf("[info] using default -user-collection = %s\n", userCollName)
	}

	// --- settings collection ---
	if WasFlagPassed("settings-collection") {
		fmt.Printf("[info] using flag -settings-collection = %s\n", settingsCollName)
	} else {
		fmt.Printf("[info] using default -settings-collection = %s\n", settingsCollName)
	}

	// --- subscription collection ---
	if WasFlagPassed("subscription-collection") {
		fmt.Printf("[info] using flag -subscription-collection = %s\n", subscriptionCollName)
	} else {
		fmt.Printf("[info] using default -subscription-collection = %s\n", subscriptionCollName)
	}

	// --- user prefix ---
	if WasFlagPassed("user-prefix") {
		fmt.Printf("[info] using flag -user-prefix = %s\n", prefix)
	} else {
		input := PromptString("User prefix (default 'user'): ")
		if input != "" {
			prefix = input
			fmt.Printf("[info] using input -user-prefix = %s\n", prefix)
		} else if input == "" {
			prefix = "user"
			fmt.Printf("[info] using default -user-prefix = %s\n", prefix)
		}
	}

	// --- user count ---
	if WasFlagPassed("user-count") {
		if count < 1 || count > 10000000 {
			fmt.Printf("[error] -user-count must be between 1 and 10000000, got %d\n", count)
			count = 100
			fmt.Printf("[info] using default -user-count = %d\n", count)
		} else {
			fmt.Printf("[info] using flag -user-count = %d\n", count)
		}
	} else {
		input := PromptInt("Number of users to create (default 100): ")
		if input != 0 {
			if input < 1 || input > 10000000 {
				fmt.Printf("[error] user count must be between 1 and 10000000, got %d\n", input)
				count = 100
				fmt.Printf("[info] using default -user-count = %d\n", count)
			} else {
				count = input
				fmt.Printf("[info] using input -user-count = %d\n", count)
			}
		} else if count == 0 {
			count = 100
			fmt.Printf("[info] using default -user-count = %d\n", count)
		}
	}

	fmt.Printf("[info] Seeding %d users with prefix '%s'...\n", count, prefix)

	// -------- Connect --------
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongodbURI).SetServerSelectionTimeout(10*time.Second))
	if err != nil {
		fmt.Printf("[error] connect mongo: %v\n", err)
		os.Exit(1)
	}

	// -------- Settings (ensure once) --------
	if err := database.EnsureSettings(ctx, client, dbName, settingsCollName); err != nil {
		fmt.Printf("[error] ensure settings: %v\n", err)
		os.Exit(1)
	}

	userColl := client.Database(dbName).Collection(userCollName)

	// Optional: ensure basic unique indexes
	_ = database.CreateUserIndexes(ctx, userColl)

	// -------- Build & Insert Users --------
	var inserted int64
	batchSize := 1000
	if count < batchSize {
		batchSize = count
	}

	uiprogress.Start()
	bar := uiprogress.AddBar(count).AppendCompleted().PrependElapsed()
	bar.PrependFunc(func(b *uiprogress.Bar) string {
		cur := atomic.LoadInt64(&inserted)
		pct := 100 * float64(cur) / float64(count)
		return fmt.Sprintf("Users %d/%d (%.1f%%)", cur, count, pct)
	})

	batch := make([]interface{}, 0, batchSize)
	userIDs := make([]primitive.ObjectID, 0, count)
	nameSet := make(map[string]struct{})

	for i := 0; i < count; i++ {
		if atomic.LoadInt32(&stopFlag) != 0 {
			fmt.Println("[info] interrupted; stopping build loop")
			break
		}

		var username string
		for {
			username = utils.GenerateRandomUsername(prefix)
			if _, exists := nameSet[username]; !exists {
				nameSet[username] = struct{}{}
				break
			}
		}

		// Keys
		// amazonSecretAccessKey, err := database.GenerateKey("private", client, dbName, userCollName)
		// if err != nil {
		// 	fmt.Printf("[error] generate private key: %v\n", err)
		// 	os.Exit(1)
		// }
		// amazonAccessKeyID, err := database.GenerateKey("public", client, dbName, userCollName)
		// if err != nil {
		// 	fmt.Printf("[error] generate public key: %v\n", err)
		// 	os.Exit(1)
		// }

		// Using fixed keys for now
		amazonSecretAccessKey := "AKIAIOSFODNN7EXAMPLE"
		amazonAccessKeyID := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

		// Password (hashed)
		hashed, err := utils.Hash(username)
		if err != nil {
			fmt.Printf("[error] hash password: %v\n", err)
			os.Exit(1)
		}

		info := models.InsertUserInfo{
			UserName:              username,
			UserEmail:             fmt.Sprintf("%s@example.com", username),
			UserPassword:          hashed,
			AmazonSecretAccessKey: amazonSecretAccessKey,
			AmazonAccessKeyID:     amazonAccessKeyID,
			Days:                  7,
		}

		uid, doc := database.BuildUserDoc(info)
		batch = append(batch, doc)
		userIDs = append(userIDs, uid)

		if len(batch) == batchSize {
			if err := database.InsertMany(ctx, client, dbName, userCollName, batch); err != nil {
				fmt.Printf("[error] insert users batch: %v\n", err)
				os.Exit(1)
			}
			atomic.AddInt64(&inserted, int64(len(batch)))
			bar.Set(int(inserted))
			batch = batch[:0]
		}
	}

	// Flush remainder
	if len(batch) > 0 {
		if err := database.InsertMany(ctx, client, dbName, userCollName, batch); err != nil {
			fmt.Printf("[error] insert users final batch: %v\n", err)
			os.Exit(1)
		}
		atomic.AddInt64(&inserted, int64(len(batch)))
		bar.Set(int(inserted))
	}

	// -------- Subscriptions --------
	subBatch := make([]interface{}, 0, 1000)
	for _, uid := range userIDs {
		subBatch = append(subBatch, database.BuildSubscriptionDoc(uid))
		if len(subBatch) == 1000 {
			if err := database.InsertMany(ctx, client, dbName, subscriptionCollName, subBatch); err != nil {
				fmt.Printf("[error] insert subscriptions batch: %v\n", err)
				os.Exit(1)
			}
			subBatch = subBatch[:0]
		}
	}
	if len(subBatch) > 0 {
		if err := database.InsertMany(ctx, client, dbName, subscriptionCollName, subBatch); err != nil {
			fmt.Printf("[error] insert subscriptions final batch: %v\n", err)
			os.Exit(1)
		}
	}

	uiprogress.Stop()

	if err := client.Disconnect(ctx); err != nil {
		fmt.Printf("[warn] disconnect mongo: %v\n", err)
	}

	fmt.Printf("[done] users_inserted=%d subscriptions=%d stop_flag=%v\n",
		inserted, len(userIDs), atomic.LoadInt32(&stopFlag) != 0)
	if atomic.LoadInt32(&stopFlag) != 0 {
		os.Exit(130)
	}

}
