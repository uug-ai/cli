package database

import (
	"context"
	"fmt"
	"time"

	"github.com/uug-ai/cli/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// --- Queries (read-only) ---

func GetUsersFromMongodb(client *mongo.Client, dbName string) ([]models.User, error) {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	coll := client.Database(dbName).Collection("users")
	var users []models.User
	cur, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	if err = cur.All(ctx, &users); err != nil {
		return nil, err
	}
	return users, nil
}

func GetUserFromMongodb(client *mongo.Client, dbName string, username string) (models.User, error) {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	coll := client.Database(dbName).Collection("users")
	var user models.User
	err := coll.FindOne(ctx, bson.M{"username": username}).Decode(&user)
	if err == mongo.ErrNoDocuments {
		return models.User{}, nil
	}
	return user, err
}

func GetSequencesFromMongodb(client *mongo.Client, dbName string, userId string, startTimestamp, endTimestamp int64) ([]models.Sequences, error) {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	coll := client.Database(dbName).Collection("sequences")
	var sequences []models.Sequences
	filter := bson.M{
		"user_id": userId,
		"start":   bson.M{"$lte": endTimestamp},
		"end":     bson.M{"$gte": startTimestamp},
	}
	cur, err := coll.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	if err = cur.All(ctx, &sequences); err != nil {
		return nil, err
	}
	return sequences, nil
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
