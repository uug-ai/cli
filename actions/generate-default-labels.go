package actions

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gosuri/uiprogress"
	"github.com/uug-ai/cli/database"
	"github.com/uug-ai/cli/models"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/mgo.v2/bson"
)

func GenerateDefaultLabels(
	mode string,
	mongodbURI string,
	mongodbHost string,
	mongodbPort string,
	mongodbSourceDatabase string,
	mongodbDatabaseCredentials string,
	mongodbUsername string,
	mongodbPassword string,
	labelNames string,
	username string,
) {

	log.Println("====================================")
	log.Println("Configuration:")
	log.Println("  MongoDB URI:", mongodbURI)
	log.Println("  MongoDB Host:", mongodbHost)
	log.Println("  MongoDB Port:", mongodbPort)
	log.Println("  MongoDB Source Database:", mongodbSourceDatabase)
	log.Println("  MongoDB Database Credentials:", mongodbDatabaseCredentials)
	log.Println("  MongoDB Username:", mongodbUsername)
	log.Println("  MongoDB Password:", "************")
	log.Println("====================================")
	fmt.Println("")

	fmt.Println(" >> Please wait while we process the data. Press Ctrl+C to stop the process.")
	var steps = []string{
		"MongoDB: verify connection",
		"MongoDB: open connection",
		"Hub: get users",
		"Hub: generate labels",
		"Hub: insert labels",
		"Hub: complete",
	}
	bar := uiprogress.AddBar(len(steps)).AppendCompleted().PrependElapsed()
	bar.PrependFunc(func(b *uiprogress.Bar) string {
		return steps[b.Current()-1]
	})
	time.Sleep(time.Second * 2)
	uiprogress.Start() // start rendering

	// #####################################
	//
	// Step 1: Verify the connection to MongoDB
	// Verify the connection to MongoDB
	//
	bar.Incr()
	if mongodbURI == "" {
		// If no URI is provided, use the host/port
		if mongodbHost == "" || mongodbPort == "" {
			log.Println("Please provide a valid MongoDB URI or host/port")
			return
		}
	}
	time.Sleep(time.Second * 2)

	// #####################################
	//
	// Step 2: Connect to MongoDB
	// Connect to MongoDB
	//
	bar.Incr()
	var db *database.DB
	if mongodbURI != "" {
		db = database.NewMongoDBURI(mongodbURI)
	} else {
		db = database.NewMongoDBHost(mongodbHost, mongodbPort, mongodbDatabaseCredentials, mongodbUsername, mongodbPassword)
	}
	client := db.Client
	defer client.Disconnect(context.Background())
	time.Sleep(time.Second * 2)

	// #####################################
	//
	// Step 3: Get the users from the Hub
	// Get the owner users from the Hub
	//
	bar.Incr()
	owners := GetOwnersFromMongodb(client, mongodbSourceDatabase, username)

	time.Sleep(time.Second * 2)

	// #####################################
	//
	// Step 4: generate the default labels
	// generate the new labels to be inserted
	//
	bar.Incr()
	labelNamesArray := strings.Split(labelNames, ",")

	time.Sleep(time.Second * 2)

	// #####################################
	//
	// Step 5: Insert the labels
	// Insert the labels into the database
	//
	bar.Incr()
	labelsCollection := client.Database(mongodbSourceDatabase).Collection("labels")
	addedLabels := []models.Label{}
	for _, owner := range owners {

		labels := GenerateDefaultLabelsForNames(labelNamesArray, owner.Id.Hex())
		for _, label := range labels {
			filter := bson.M{"user_id": label.UserId, "name": label.Name}
			count, err := labelsCollection.CountDocuments(context.Background(), filter)
			if err != nil {
				log.Println("Error checking for existing label:", err)
				continue
			}
			if count == 0 {
				if mode == "dry-run" {
					// Nothing to do here..
				} else if mode == "live" {
					_, err = labelsCollection.InsertOne(context.Background(), label)
				}
				addedLabels = append(addedLabels, label)
				if err != nil {
					log.Println("Error inserting label:", err)
				}
			} else {
				log.Println("Label already exists:", label.Name, "for user:", label.UserId)
			}
		}
	}

	bar.Incr()
	uiprogress.Stop()

	// #####################################
	//
	// Printout a table with the media that was transferred
	//
	log.Println("")
	log.Println(">> Labels inserted:")
	log.Println("")
	log.Println("  +---------------------------------------------+----------------------+")
	log.Println("  | User id                                     | Label                |")
	log.Println("  +---------------------------------------------+----------------------+")
	for _, label := range addedLabels {
		log.Printf("  | %-43s | %-20s |\n", label.OwnerId, label.Name)
	}
	log.Println("  +---------------------------------------------+----------------------+")
	log.Println("")
	log.Println("Process completed.")
}

func GetOwnersFromMongodb(client *mongo.Client, DatabaseName string, username string) []models.User {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	db := client.Database(DatabaseName)
	usersCollection := db.Collection("users")

	var owners []models.User
	match := bson.M{"role": "owner"}
	if username != "" {
		match = bson.M{"role": "owner", "username": username}
	}
	cursor, err := usersCollection.Find(ctx, match)
	if err != nil {
		log.Println(err)
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var user models.User
		if err := cursor.Decode(&user); err != nil {
			log.Println(err)
		}
		owners = append(owners, user)
	}
	if err := cursor.Err(); err != nil {
		log.Println(err)
	}

	return owners
}

func GenerateDefaultLabelsForNames(names []string, userId string) []models.Label {
	// Adjust default colors here
	colors := []string{
		"#9B260B", // Red
		"#135A14", // Green
		"#214598", // Blue
		"#FFC32D", // Yellow
		"#F89A2E", // Orange
		"#5D2C6C", // Purple
	}

	// Default label names if empty
	if len(names) == 0 {
		names = []string{"Incident", "suspicious", "unauthorized"}
	}

	var labels []models.Label
	for i, name := range names {
		// Generate the label
		label := models.Label{
			Id:          primitive.NewObjectID(),
			Name:        name,
			Description: "Label for " + name,
			Color:       colors[i%len(colors)],
			UserId:      userId,
			OwnerId:     userId,
			CreatedAt:   time.Now().Unix(),
			IsPrivate:   false,
			Types:       []string{"case"},
		}
		labels = append(labels, label)
	}
	return labels
}
