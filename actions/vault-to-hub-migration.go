package actions

import (
	"context"
	"log"
	"time"

	"github.com/uug-ai/cli/database"
	"github.com/uug-ai/cli/models"
	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/mgo.v2/bson"
)

func VaultToHubMigration(mongodbURI *string,
	mongodbHost *string,
	mongodbPort *string,
	mongodbSourceDatabase *string,
	mongodbDestinationDatabase *string,
	mongodbDatabaseCredentials *string,
	mongodbUsername *string,
	mongodbPassword *string) {

	if *mongodbURI == "" {
		// If no URI is provided, use the host/port
		if *mongodbHost == "" || *mongodbPort == "" {
			panic("Please provide a valid MongoDB URI or host/port")
		}
	}

	// Connect to MongoDB
	var db *database.DB
	if mongodbURI != nil {
		db = database.NewMongoDBURI(*mongodbURI)
	} else {
		db = database.NewMongoDBHost(*mongodbHost, *mongodbPort, *mongodbDatabaseCredentials, *mongodbUsername, *mongodbPassword)
	}
	client := db.Client
	defer client.Disconnect(context.Background())

	// Test the connection, list out the collections
	accounts := GetAccountsFromMongodb(client, *mongodbSourceDatabase)

	log.Println(accounts)
}

var TIMEOUT = 10 * time.Second

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
