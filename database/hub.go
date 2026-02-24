package database

import (
	"context"
	crand "crypto/rand"
	"encoding/base32"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cliModels "github.com/uug-ai/cli/models"
	"github.com/uug-ai/cli/utils"
	"github.com/uug-ai/models/pkg/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// --- Queries (read-only) ---

func GetUsersFromMongodb(client *mongo.Client, DatabaseName string) []cliModels.User {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	db := client.Database(DatabaseName)
	accountsCollection := db.Collection("users")

	var users []cliModels.User

	match := bson.M{}
	cursor, err := accountsCollection.Find(ctx, match)
	if err != nil {
		log.Println(err)
		return users
	}
	if cursor != nil {
		defer cursor.Close(ctx)
	}
	err = cursor.All(ctx, &users)
	if err != nil {
		log.Println(err)
	}

	return users
}

func GetUserFromMongodb(client *mongo.Client, DatabaseName string, username string, userCollName ...string) cliModels.User {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	collName := "users"
	if len(userCollName) > 0 && userCollName[0] != "" {
		collName = userCollName[0]
	}
	db := client.Database(DatabaseName)
	accountsCollection := db.Collection(collName)

	var user cliModels.User

	match := bson.M{"username": username}
	err := accountsCollection.FindOne(ctx, match).Decode(&user)
	if err != nil {
		log.Println(err)
	}

	return user
}

func GetPipelineUserFromMongodb(client *mongo.Client, DatabaseName string, username string) models.User {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	db := client.Database(DatabaseName)
	accountsCollection := db.Collection("users")

	var user models.User

	match := bson.M{"username": username}
	err := accountsCollection.FindOne(ctx, match).Decode(&user)
	if err != nil {
		log.Println(err)
	}

	return user
}

func GetActiveSubscriptionFromMongodb(client *mongo.Client, DatabaseName string, userId string) models.Subscription {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	db := client.Database(DatabaseName)
	subscriptionCollection := db.Collection("subscriptions")

	var subscription models.Subscription
	now := time.Now()
	match := bson.M{
		"user_id": userId,
		"$or": []bson.M{
			{"ends_at": bson.M{"$gt": now}},
			{"ends_at": nil},
		},
	}
	err := subscriptionCollection.FindOne(ctx, match).Decode(&subscription)
	if err != nil {
		log.Println(err)
	}

	return subscription
}

type planSettings struct {
	Key string                 `json:"key" bson:"key"`
	Map map[string]interface{} `json:"map" bson:"map"`
}

type analysisInfo struct {
	ID            string
	Key           string
	ResolvedCount int
}

func GetPlansFromMongodb(client *mongo.Client, DatabaseName string, user models.User) map[string]interface{} {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	db := client.Database(DatabaseName)
	settingsCollection := db.Collection("settings")

	var settings planSettings
	err := settingsCollection.FindOne(ctx, bson.M{"key": "plan"}).Decode(&settings)
	if err != nil {
		log.Println(err)
		return map[string]interface{}{}
	}

	plans := settings.Map
	if user.Settings != nil {
		if userSettings, ok := user.Settings["plan"].(map[string]interface{}); ok {
			for k, v := range userSettings {
				plans[k] = v
			}
		}
	}

	return plans
}

func AnalysisExistsByID(client *mongo.Client, DatabaseName string, userId string, analysisId string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	db := client.Database(DatabaseName)
	analysisCollection := db.Collection("analysis")

	objectID, err := primitive.ObjectIDFromHex(analysisId)
	if err != nil {
		log.Println(err)
		return false
	}

	match := bson.M{
		"_id": objectID,
		"$or": []bson.M{
			{"user_id": userId},
			{"userid": userId},
		},
	}
	err = analysisCollection.FindOne(ctx, match).Err()
	if err != nil {
		return false
	}
	return true
}

func AnalysisIDsExistMap(client *mongo.Client, DatabaseName string, userId string, analysisIds []string) map[string]bool {
	exists := make(map[string]bool)
	if len(analysisIds) == 0 {
		return exists
	}

	objectIDs := make([]primitive.ObjectID, 0, len(analysisIds))
	for _, analysisId := range analysisIds {
		objectID, err := primitive.ObjectIDFromHex(analysisId)
		if err != nil {
			log.Println(err)
			continue
		}
		objectIDs = append(objectIDs, objectID)
	}
	if len(objectIDs) == 0 {
		return exists
	}

	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	db := client.Database(DatabaseName)
	analysisCollection := db.Collection("analysis")

	match := bson.M{
		"_id": bson.M{"$in": objectIDs},
		"$or": []bson.M{
			{"user_id": userId},
			{"userid": userId},
		},
	}

	cursor, err := analysisCollection.Find(ctx, match)
	if err != nil {
		log.Println(err)
		return exists
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var doc struct {
			Id primitive.ObjectID `bson:"_id"`
		}
		if err := cursor.Decode(&doc); err != nil {
			log.Println(err)
			continue
		}
		exists[doc.Id.Hex()] = true
	}

	return exists
}

func AnalysisKeysExistMap(client *mongo.Client, DatabaseName string, userId string, keys []string) map[string]bool {
	exists := make(map[string]bool)
	if len(keys) == 0 {
		return exists
	}

	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	db := client.Database(DatabaseName)
	analysisCollection := db.Collection("analysis")

	match := bson.M{
		"key": bson.M{"$in": keys},
		"$or": []bson.M{
			{"user_id": userId},
			{"userid": userId},
		},
	}

	cursor, err := analysisCollection.Find(ctx, match)
	if err != nil {
		log.Println(err)
		return exists
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var doc struct {
			Key string `bson:"key"`
		}
		if err := cursor.Decode(&doc); err != nil {
			log.Println(err)
			continue
		}
		exists[doc.Key] = true
	}

	return exists
}

func AnalysisInfoByIDs(client *mongo.Client, DatabaseName string, userId string, analysisIds []string) map[string]analysisInfo {
	info := make(map[string]analysisInfo)
	if len(analysisIds) == 0 {
		return info
	}

	objectIDs := make([]primitive.ObjectID, 0, len(analysisIds))
	for _, analysisId := range analysisIds {
		objectID, err := primitive.ObjectIDFromHex(analysisId)
		if err != nil {
			log.Println(err)
			continue
		}
		objectIDs = append(objectIDs, objectID)
	}
	if len(objectIDs) == 0 {
		return info
	}

	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	db := client.Database(DatabaseName)
	analysisCollection := db.Collection("analysis")

	match := bson.M{
		"_id": bson.M{"$in": objectIDs},
		"$or": []bson.M{
			{"user_id": userId},
			{"userid": userId},
		},
	}

	cursor, err := analysisCollection.Find(ctx, match)
	if err != nil {
		log.Println(err)
		return info
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var doc struct {
			Id                 primitive.ObjectID `bson:"_id"`
			Key                string             `bson:"key"`
			ResolvedOperations []string           `bson:"resolvedoperations"`
		}
		if err := cursor.Decode(&doc); err != nil {
			log.Println(err)
			continue
		}
		info[doc.Id.Hex()] = analysisInfo{
			ID:            doc.Id.Hex(),
			Key:           doc.Key,
			ResolvedCount: len(doc.ResolvedOperations),
		}
	}

	return info
}

func AnalysisInfoByKeys(client *mongo.Client, DatabaseName string, userId string, keys []string) map[string]analysisInfo {
	info := make(map[string]analysisInfo)
	if len(keys) == 0 {
		return info
	}

	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	db := client.Database(DatabaseName)
	analysisCollection := db.Collection("analysis")

	match := bson.M{
		"key": bson.M{"$in": keys},
		"$or": []bson.M{
			{"user_id": userId},
			{"userid": userId},
		},
	}

	cursor, err := analysisCollection.Find(ctx, match)
	if err != nil {
		log.Println(err)
		return info
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var doc struct {
			Id                 primitive.ObjectID `bson:"_id"`
			Key                string             `bson:"key"`
			ResolvedOperations []string           `bson:"resolvedoperations"`
		}
		if err := cursor.Decode(&doc); err != nil {
			log.Println(err)
			continue
		}
		info[doc.Key] = analysisInfo{
			ID:            doc.Id.Hex(),
			Key:           doc.Key,
			ResolvedCount: len(doc.ResolvedOperations),
		}
	}

	return info
}

func DeleteAnalysisByID(client *mongo.Client, DatabaseName string, userId string, analysisId string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	db := client.Database(DatabaseName)
	analysisCollection := db.Collection("analysis")

	objectID, err := primitive.ObjectIDFromHex(analysisId)
	if err != nil {
		log.Println(err)
		return false
	}

	match := bson.M{
		"_id": objectID,
		"$or": []bson.M{
			{"user_id": userId},
			{"userid": userId},
		},
	}
	result, err := analysisCollection.DeleteOne(ctx, match)
	if err != nil {
		log.Println(err)
		return false
	}
	return result.DeletedCount > 0
}

func DeleteAnalysisByKey(client *mongo.Client, DatabaseName string, userId string, key string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	db := client.Database(DatabaseName)
	analysisCollection := db.Collection("analysis")

	match := bson.M{
		"key": key,
		"$or": []bson.M{
			{"user_id": userId},
			{"userid": userId},
		},
	}
	result, err := analysisCollection.DeleteOne(ctx, match)
	if err != nil {
		log.Println(err)
		return false
	}
	return result.DeletedCount > 0
}

func GetSequencesFromMongodb(client *mongo.Client, DatabaseName string, userId string, startTimestamp int64, endTimestamp int64) []cliModels.Sequences {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	db := client.Database(DatabaseName)
	sequenceCollection := db.Collection("sequences")

	var sequences []cliModels.Sequences
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
		return sequences
	}
	if cursor != nil {
		defer cursor.Close(ctx)
	}
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

func GenerateKey(keyType string, client *mongo.Client, dbName, userCollName string) (string, error) {
	const keyLen = 20
	maxRetries := 10
	for attempts := 0; attempts < maxRetries; attempts++ {
		b := make([]byte, keyLen)
		_, err := crand.Read(b)
		if err != nil {
			return "", err
		}
		key := strings.TrimRight(base32.StdEncoding.EncodeToString(b), "=")
		isDup, err := IsDuplicateKey(keyType, key, client, dbName, userCollName)
		if err != nil {
			return "", err
		}
		if !isDup {
			return key, nil
		}
	}
	return "", fmt.Errorf("failed to generate unique key after max retries")
}

func BuildUserDoc(info cliModels.InsertUserInfo) (primitive.ObjectID, bson.M) {
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
		"stripe_plan":   "enterprise", // Always enterprise for now
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

func BuildDeviceDocs(count int, userID primitive.ObjectID, cloudKey string) ([]interface{}, []string) {
	if count < 1 {
		count = 1
	}
	docs := make([]interface{}, 0, count)
	keys := make([]string, 0, count)
	for i := 0; i < count; i++ {
		id := primitive.NewObjectID()
		suffix := rand.Intn(100000000)

		key := fmt.Sprintf("camera-key-%d", suffix)
		nowMs := time.Now().UnixMilli()
		heartbeat := []models.DeprecatedHeartbeat{{
			CloudPublicKey: cloudKey,
			Key:            key,
			Timestamp:      time.Now().Unix(),
			Version:        "3.5.0",
			Release:        "1f9772d",
			Encrypted:      false,
			EncryptedData:  []byte{},
			HubEncryption:  "true",
			E2EEncryption:  "false",
			Enterprise:     false,
			Hash:           "",
			MACs:           []string{"02:42:ac:12:00:0c"},
			IPs:            []string{"127.0.0.1/8", "172.18.0.12/16"},
			CameraName:     fmt.Sprintf("camera-%d", suffix),
			CameraType:     "IPCamera",
			Architecture:   "x86_64",
			Hostname:       fmt.Sprintf("%x", rand.Uint64()),
			FreeMemory:     fmt.Sprintf("%d", 650000000+rand.Intn(100000000)),
			TotalMemory:    "16515977216",
			UsedMemory:     fmt.Sprintf("%d", 15800000000+rand.Intn(100000000)),
			ProcessMemory:  fmt.Sprintf("%d", 28000000+rand.Intn(100000000)),
			Kubernetes:     false,
			Docker:         true,
			Kios:           false,
			Raspberrypi:    false,
			Uptime:         "5 days",
			BootTime:       "5 days",
			ONVIF:          "false",
			ONVIFZoom:      "false",
			ONVIFPanTilt:   "false",
			ONVIFPresets:   "false",
			CameraConnected: []string{
				"true", "false",
			}[rand.Intn(2)],
			HasBackChannel: "false",
		}}

		doc := models.Device{
			Id:                    id,
			Key:                   key,
			Name:                  fmt.Sprintf("Camera %d", i+1),
			Type:                  "camera",
			Repository:            "https://github.com/uug-ai/agent",
			Version:               "3.5.0",
			Deployment:            "docker",
			OrganisationId:        userID.Hex(),
			SiteIds:               []string{},
			GroupIds:              []string{},
			UserId:                userID.Hex(), // Legacy compatibility.
			ConnectionStart:       nowMs,
			DeviceLastSeen:        nowMs,
			AgentLastSeen:         nowMs,
			IdleThreshold:         60000,
			DisconnectedThreshold: 300000,
			HealthyThreshold:      120000,
			Metadata: &models.DeviceMetadata{
				Architecture: "x86_64",
				Hostname:     fmt.Sprintf("%x", rand.Uint64()),
				FreeMemory:   fmt.Sprintf("%d", 650000000+rand.Intn(100000000)),
				TotalMemory:  "16515977216",
				UsedMemory:   fmt.Sprintf("%d", 15800000000+rand.Intn(100000000)),
				ProcessMemory: fmt.Sprintf(
					"%d", 28000000+rand.Intn(100000000),
				),
				Encrypted:     false,
				EncryptedData: []byte{},
				HubEncryption: "true",
				E2EEncryption: "false",
				IPAddress:     "127.0.0.1",
				MacAddress:    "02:42:ac:12:00:0c",
				BootTime:      "5 days",
			},
			CameraMetadata: &models.CameraMetadata{
				Resolution:     "1920x1080",
				FrameRate:      15,
				Codec:          "H.264",
				HasOnvif:       false,
				HasAudio:       false,
				HasZoom:        false,
				HasBackChannel: false,
				HasPanTilt:     false,
				HasPresets:     false,
				HasIO:          false,
			},
			FeaturePermissions: &models.DeviceFeaturePermissions{
				PTZ:          models.PermissionNone,
				Liveview:     models.PermissionNone,
				RemoteConfig: models.PermissionNone,
				IO:           models.PermissionNone,
				FloorPlans:   models.PermissionNone,
			},
			AtRuntimeMetadata: &models.DeviceAtRuntimeMetadata{
				Status: "disconnected",
			},
			DeprecatedAnalytics: &heartbeat,
		}
		docs = append(docs, doc)
		keys = append(keys, key)
	}
	return docs, keys
}

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

func GenerateBatchMedia(
	n int,
	days int,
	userObjectID primitive.ObjectID,
	deviceIDs []string,
) []interface{} {

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

		deviceID := deviceIDs[rand.Intn(len(deviceIDs))]
		docs = append(docs, buildSyntheticMedia(st, en, userObjectID.Hex(), deviceID, ""))
	}
	return docs
}

func GenerateBatchMediaWithGroups(
	n int,
	days int,
	userObjectID primitive.ObjectID,
	groups []bson.M,
) []interface{} {

	if days < 1 {
		days = 1
	}
	if len(groups) == 0 {
		return nil
	}
	docs := make([]interface{}, 0, n)
	secondsInDays := int64(days) * 86400
	now := time.Now().Unix()

	for i := 0; i < n; i++ {
		offset := rand.Int63n(secondsInDays)
		st := now - offset
		en := st + int64(rand.Intn(21)+5)

		// Pick a random group
		group := groups[rand.Intn(len(groups))]
		var groupId string
		var deviceKey string

		// Get groupId
		if id, ok := group["_id"].(primitive.ObjectID); ok {
			groupId = id.Hex()
		} else if idStr, ok := group["_id"].(string); ok {
			groupId = idStr
		}

		// Get devices array and pick a random device key.
		switch devArr := group["devices"].(type) {
		case bson.A:
			if len(devArr) > 0 {
				dev := devArr[rand.Intn(len(devArr))]
				if key, ok := dev.(string); ok {
					deviceKey = key
				}
			}
		case []string:
			if len(devArr) > 0 {
				deviceKey = devArr[rand.Intn(len(devArr))]
			}
		}

		docs = append(docs, buildSyntheticMedia(st, en, userObjectID.Hex(), deviceKey, groupId))
	}
	return docs
}

func buildSyntheticMedia(startTimestamp, endTimestamp int64, organisationId, deviceKey, groupId string) models.Media {
	return models.Media{
		Id:              primitive.NewObjectID(),
		StartTimestamp:  startTimestamp,
		EndTimestamp:    endTimestamp,
		Duration:        int(endTimestamp - startTimestamp),
		DeviceId:        deviceKey, // legacy compatibility
		DeviceKey:       deviceKey,
		GroupId:         groupId,
		OrganisationId:  organisationId,
		StorageSolution: "kstorage",
		VideoProvider:   "azure-production",
		VideoFile:       utils.PickOne(VIDEO_POOL),
		Metadata: &models.MediaMetadata{
			AnalysisId:     fmt.Sprintf("AN-%06d", rand.Intn(5001)),
			Description:    "synthetic media sample for load test",
			Detections:     utils.SampleUnique(DETECTION_POOL, rand.Intn(4)+1),
			DominantColors: utils.SampleUnique(COLOR_POOL, rand.Intn(3)+1),
			Count:          rand.Intn(11) - 5,
			Tags:           utils.SampleUnique(TAG_POOL, rand.Intn(4)+1),
			Classifications: []models.Classification{
				{
					Key:       "normal_activity",
					Centroids: [][2]float64{},
				},
			},
		},
	}
}

func CreateMediaIndexes(ctx context.Context, col *mongo.Collection) {
	indexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "startTimestamp", Value: 1}}},
		{Keys: bson.D{{Key: "deviceId", Value: 1}}},
		{Keys: bson.D{{Key: "deviceKey", Value: 1}}},
		{Keys: bson.D{{Key: "organisationId", Value: 1}}},
		{Keys: bson.D{{Key: "metadata.tags", Value: 1}}},
		{Keys: bson.D{{Key: "metadata.detections", Value: 1}}},
		{Keys: bson.D{{Key: "duration", Value: 1}}},
	}
	_, err := col.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		fmt.Printf("[info] index creation skipped: %v\n", err)
	}
}

func CreateUserIndexes(ctx context.Context, coll *mongo.Collection) error {
	models := []mongo.IndexModel{
		{Keys: bson.D{{Key: "username", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "email", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "amazon_access_key_id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "amazon_secret_access_key", Value: 1}}, Options: options.Index().SetUnique(true)},
	}
	_, err := coll.Indexes().CreateMany(ctx, models)
	return err
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
	progressCh chan<- int, // cumulative total after each batch
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
					total := atomic.AddInt64(totalInserted, int64(inserted))
					if progressCh != nil {
						progressCh <- int(total)
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
