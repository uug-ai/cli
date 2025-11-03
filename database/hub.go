package database

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uug-ai/cli/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// --- Queries (read-only) ---

func GetUsersFromMongodb(client *mongo.Client, DatabaseName string) []models.User {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	db := client.Database(DatabaseName)
	accountsCollection := db.Collection("users")

	var users []models.User

	match := bson.M{}
	cursor, err := accountsCollection.Find(ctx, match)
	if err != nil {
		log.Println(err)
	}
	defer cursor.Close(ctx)
	err = cursor.All(ctx, &users)
	if err != nil {
		log.Println(err)
	}

	return users
}

func GetUserFromMongodb(client *mongo.Client, DatabaseName string, username string, userCollName ...string) models.User {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	collName := "users"
	if len(userCollName) > 0 && userCollName[0] != "" {
		collName = userCollName[0]
	}
	db := client.Database(DatabaseName)
	accountsCollection := db.Collection(collName)

	var user models.User

	match := bson.M{"username": username}
	err := accountsCollection.FindOne(ctx, match).Decode(&user)
	if err != nil {
		log.Println(err)
	}

	return user
}

func GetSequencesFromMongodb(client *mongo.Client, DatabaseName string, userId string, startTimestamp int64, endTimestamp int64) []models.Sequences {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	db := client.Database(DatabaseName)
	sequenceCollection := db.Collection("sequences")

	var sequences []models.Sequences
	match := bson.M{
		"user_id": userId,
		"start": bson.M{
			"$lte": endTimestamp,
		},
		"end": bson.M{
			"$gte": startTimestamp,
		},
	}
	cursor, err := sequenceCollection.Find(ctx, match)
	if err != nil {
		log.Println(err)
	}
	defer cursor.Close(ctx)
	err = cursor.All(ctx, &sequences)
	if err != nil {
		log.Println(err)
	}

	return sequences
}

func IsDuplicateKey(keyType, keyValue string, client *mongo.Client, dbName, userCollName string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()

	userColl := client.Database(dbName).Collection(userCollName)

	var filter bson.M
	switch keyType {
	case "public":
		filter = bson.M{"amazon_access_key_id": keyValue}
	case "private":
		filter = bson.M{"amazon_secret_access_key": keyValue}
	default:
		return false, fmt.Errorf("invalid key type: %s", keyType)
	}
	count, err := userColl.CountDocuments(ctx, filter)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// --- Document builders ---

func BuildUserDoc(info models.InsertUserInfo) (primitive.ObjectID, bson.M) {
	userID := primitive.NewObjectID()
	var days []string
	if info.Days > 0 {
		days = make([]string, info.Days)
		for i := 0; i < info.Days; i++ {
			days[i] = time.Now().AddDate(0, 0, -i).Format("02-01-2006")
		}
	}
	now := time.Now()
	doc := bson.M{
		"_id":                      userID,
		"username":                 info.UserName,
		"email":                    info.UserEmail,
		"password":                 info.UserPassword,
		"role":                     "owner",
		"google2fa_enabled":        false,
		"timezone":                 "Europe/Brussels",
		"isActive":                 int64(1),
		"registerToken":            "",
		"updated_at":               now,
		"created_at":               now,
		"amazon_secret_access_key": info.AmazonSecretAccessKey,
		"amazon_access_key_id":     info.AmazonAccessKeyID,
		"card_brand":               "Visa",
		"card_last_four":           "0000",
		"card_status":              "ok",
		"card_status_message":      nil,
		"days":                     days,
	}
	return userID, doc
}

func BuildSubscriptionDoc(userID primitive.ObjectID) bson.M {
	now := time.Now()
	return bson.M{
		"_id":           primitive.NewObjectID(),
		"name":          "default",
		"stripe_id":     "sub_9ECyjjMz3R7etK",
		"stripe_plan":   "enterprise",
		"quantity":      1,
		"trial_ends_at": nil,
		"ends_at":       nil,
		"user_id":       userID.Hex(),
		"updated_at":    now,
		"created_at":    now,
		"stripe_status": "active",
	}
}

func BuildSettingsDoc() bson.M {
	return bson.M{
		"key": "plan",
		"map": bson.M{
			"basic":      bson.M{"level": 1, "uploadLimit": 100, "videoLimit": 100, "usage": 500, "analysisLimit": 0, "dayLimit": 3},
			"premium":    bson.M{"level": 2, "uploadLimit": 500, "videoLimit": 500, "usage": 1000, "analysisLimit": 0, "dayLimit": 7},
			"gold":       bson.M{"level": 3, "uploadLimit": 1000, "videoLimit": 1000, "usage": 3000, "analysisLimit": 1000, "dayLimit": 30},
			"business":   bson.M{"level": 4, "uploadLimit": 99999999, "videoLimit": 99999999, "usage": 10000, "analysisLimit": 1000, "dayLimit": 30},
			"enterprise": bson.M{"level": 5, "uploadLimit": 99999999, "videoLimit": 99999999, "usage": 99999999, "analysisLimit": 5000, "dayLimit": 30},
		},
	}
}

func BuildDeviceDocs(count int, userID primitive.ObjectID, key string) ([]interface{}, []primitive.ObjectID) {
	if count < 1 {
		count = 1
	}
	docs := make([]interface{}, 0, count)
	ids := make([]primitive.ObjectID, 0, count)
	for i := 0; i < count; i++ {
		id := primitive.NewObjectID()
		doc := bson.M{
			"_id":      id,
			"key":      key,
			"user_id":  userID.Hex(),
			"status":   "inactive",
			"isActive": false,
			"featurePermissions": bson.M{
				"ptz": 0, "liveview": 0, "remote_config": 0,
			},
			"analytics": []bson.M{{
				"cloudpublickey": key,
				"key":            fmt.Sprintf("camera%d", i+1),
				"timestamp":      time.Now().Unix(),
				"version":        "3.5.0",
				"release":        "1f9772d",
			}},
		}
		docs = append(docs, doc)
		ids = append(ids, id)
	}
	return docs, ids
}

func BuildBatchDocs(
	n int,
	days int,
	userObjectID primitive.ObjectID,
	deviceIDs []primitive.ObjectID,
) []interface{} {

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

	if days < 1 {
		days = 1
	}
	if len(deviceIDs) == 0 {
		return nil
	}
	docs := make([]interface{}, 0, n)
	secondsInDays := int64(days) * 86400
	now := time.Now().Unix()

	for i := 0; i < n; i++ {
		offset := rand.Int63n(secondsInDays)
		st := now - offset
		en := st + int64(rand.Intn(21)+5)

		deviceID := deviceIDs[rand.Intn(len(deviceIDs))].Hex()

		doc := bson.M{
			"_id":             primitive.NewObjectID(),
			"startTimestamp":  st,
			"endTimestamp":    en,
			"duration":        en - st,
			"deviceId":        deviceID,
			"organisationId":  fmt.Sprintf("ORG-%03d", rand.Intn(100)+1),
			"storageSolution": "kstorage",
			"videoProvider":   "azure-production",
			"videoFile":       pickOne(VIDEO_POOL),
			"analysisId":      fmt.Sprintf("AN-%06d", rand.Intn(5001)),
			"description":     "synthetic media sample for load test",
			"detections":      sampleUnique(DETECTION_POOL, rand.Intn(4)+1),
			"dominantColors":  sampleUnique(COLOR_POOL, rand.Intn(3)+1),
			"count":           rand.Intn(11) - 5,
			"tags":            sampleUnique(TAG_POOL, rand.Intn(4)+1),
			"metadata": bson.M{
				"tags":            sampleUnique(TAG_POOL, rand.Intn(3)+1),
				"classifications": []string{"normal_activity"},
			},
			"userId": userObjectID.Hex(),
		}
		docs = append(docs, doc)
	}
	return docs
}

func pickOne(pool []string) string {
	if len(pool) == 0 {
		return ""
	}
	return pool[rand.Intn(len(pool))]
}

func sampleUnique(pool []string, n int) []string {
	if n <= 0 || len(pool) == 0 {
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

// --- Inserts ---

func InsertOne(ctx context.Context, client *mongo.Client, dbName, collName string, doc interface{}) error {
	_, err := client.Database(dbName).Collection(collName).InsertOne(ctx, doc)
	return err
}

func InsertMany(ctx context.Context, client *mongo.Client, dbName, collName string, docs []interface{}) error {
	if len(docs) == 0 {
		return nil
	}
	_, err := client.Database(dbName).Collection(collName).InsertMany(ctx, docs)
	return err
}

func EnsureSettings(ctx context.Context, client *mongo.Client, dbName, collName string) error {
	coll := client.Database(dbName).Collection(collName)
	err := coll.FindOne(ctx, bson.M{"key": "plan", "map.enterprise": bson.M{"$exists": true}}).Err()
	if err == nil {
		return nil
	}
	if err != mongo.ErrNoDocuments {
		return err
	}
	_, err = coll.InsertOne(ctx, BuildSettingsDoc())
	return err
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

func RunBatchWorkers(
	ctx context.Context,
	parallel int,
	coll *mongo.Collection,
	batchCh <-chan []interface{},
	stopFlag *int32,
	totalInserted *int64,
	progressCh chan<- int,
) {
	var wg sync.WaitGroup
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for docs := range batchCh {
				if atomic.LoadInt32(stopFlag) != 0 {
					return
				}
				inserted := InsertBatch(ctx, coll, docs)
				if inserted > 0 {
					atomic.AddInt64(totalInserted, int64(inserted))
					if progressCh != nil {
						select {
						case progressCh <- inserted:
						default:
							// Do nothing
						}
					}
				}
			}
		}()
	}
	wg.Wait()
	if progressCh != nil {
		close(progressCh)
	}
}
