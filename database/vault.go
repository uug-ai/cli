package database

import (
	"context"
	"log"

	"github.com/uug-ai/cli/models"
	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/mgo.v2/bson"
)

func GetAccountsFromMongodb(client *mongo.Client, DatabaseName string) []models.Account {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	db := client.Database(DatabaseName)
	accountsCollection := db.Collection("accounts")

	var accounts []models.Account

	match := bson.M{}
	cursor, err := accountsCollection.Find(ctx, match)
	if err != nil {
		log.Println(err)
	}
	defer cursor.Close(ctx)
	err = cursor.All(ctx, &accounts)
	if err != nil {
		log.Println(err)
	}

	return accounts
}

func GetMediaOfUserFromMongodb(client *mongo.Client, DatabaseName string, username string, startTimestamp int64, endTimestamp int64) []models.VaultMedia {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	db := client.Database(DatabaseName)
	mediaCollection := db.Collection("media")

	var media []models.VaultMedia

	// User is store in filename
	// For example: "cedricve/1715285345_6-967003_front_200-200-400-400_284_769_sprite.png"
	// start timestamp: 1715285345
	// end timestamp:1715385345
	match := bson.M{
		"filename":  bson.M{"$regex": "^" + username},
		"timestamp": bson.M{"$gte": startTimestamp, "$lte": endTimestamp},
	}
	cursor, err := mediaCollection.Find(ctx, match)
	if err != nil {
		log.Println(err)
	}
	defer cursor.Close(ctx)
	err = cursor.All(ctx, &media)
	if err != nil {
		log.Println(err)
	}

	return media
}

func GetActiveQueuesFromMongodb(client *mongo.Client, DatabaseName string) []models.Queue {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()

	db := client.Database(DatabaseName)
	queuesCollection := db.Collection("queues")

	var queues []models.Queue
	match := bson.M{"enabled": "true"}
	cursor, err := queuesCollection.Find(ctx, match)
	if err != nil {
		log.Println(err)
	}
	defer cursor.Close(ctx)
	err = cursor.All(ctx, &queues)
	if err != nil {
		log.Println(err)
	}

	return queues
}
