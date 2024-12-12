package database

import (
	"context"
	"log"

	"github.com/uug-ai/cli/models"
	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/mgo.v2/bson"
)

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

func GetUserFromMongodb(client *mongo.Client, DatabaseName string, username string) models.User {
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
