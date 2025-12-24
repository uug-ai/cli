package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gosuri/uiprogress"
	"github.com/uug-ai/cli/database"
	"github.com/uug-ai/cli/models"
	"github.com/uug-ai/cli/queue"
)

func ReprocessMedia(mode string,
	mongodbURI string,
	mongodbHost string,
	mongodbPort string,
	mongodbSourceDatabase string,
	mongodbDestinationDatabase string,
	mongodbDatabaseCredentials string,
	mongodbUsername string,
	mongodbPassword string,
	queueName string,
	userId string,
	startTimestamp int64,
	endTimestamp int64,
	timezone string,
	batchSize int,
	batchDelay int,
) {
	if timezone == "" {
		timezone = "UTC"
	}

	location, _ := time.LoadLocation(timezone)
	startTime := time.Unix(startTimestamp, 0).In(location)
	endTime := time.Unix(endTimestamp, 0).In(location)

	log.Println("====================================")
	log.Println("Configuration:")
	log.Println("  MongoDB URI:", mongodbURI)
	log.Println("  MongoDB Host:", mongodbHost)
	log.Println("  MongoDB Port:", mongodbPort)
	log.Println("  MongoDB Source Database:", mongodbSourceDatabase)
	log.Println("  MongoDB Destination Database:", mongodbDestinationDatabase)
	log.Println("  MongoDB Database Credentials:", mongodbDatabaseCredentials)
	log.Println("  MongoDB Username:", mongodbUsername)
	log.Println("  MongoDB Password:", "************")
	log.Println("  Queue:", queueName)
	log.Println("  User ID:", userId)
	log.Println("  Start Time", startTime)
	log.Println("  End Time", endTime)
	log.Println("====================================")
	fmt.Println("")

	if userId == "" {
		log.Println("Please provide a valid user ID")
		return
	}
	if mongodbDestinationDatabase != "" && mongodbDestinationDatabase != mongodbSourceDatabase {
		log.Println("Warning: reprocess-media runs within one database; using source database for sequences.")
	}
	if startTimestamp == 0 || endTimestamp == 0 {
		log.Println("Please provide valid start and end timestamps")
		return
	}

	var steps = []string{
		"MongoDB: verify connection",
		"MongoDB: open connection",
		"Hub: query sequences",
		"Hub: filter media without analysis",
		"Vault: get queue connection",
		"Queue: connect",
		"Queue: send analysis events",
	}
	bar := uiprogress.AddBar(len(steps)).AppendCompleted().PrependElapsed()
	bar.PrependFunc(func(b *uiprogress.Bar) string {
		return steps[b.Current()-1]
	})
	time.Sleep(time.Second * 2)
	uiprogress.Start()

	// Step 1: Verify connection configuration
	bar.Incr()
	if mongodbURI == "" {
		if mongodbHost == "" || mongodbPort == "" {
			log.Println("Please provide a valid MongoDB URI or host/port")
			return
		}
	}
	time.Sleep(time.Second * 2)

	// Step 2: Connect to MongoDB
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

	// Step 3: Fetch sequences for the user in the hub database
	bar.Incr()
	sequences := database.GetSequencesFromMongodb(client, mongodbSourceDatabase, userId, startTimestamp, endTimestamp)
	time.Sleep(time.Second * 2)

	// Step 4: Filter media that has no analysis
	bar.Incr()
	var candidates []models.Media
	for _, sequence := range sequences {
		for _, media := range sequence.Images {
			if media.Timestamp < startTimestamp || media.Timestamp > endTimestamp {
				continue
			}
			mediaKey := media.Key
			if mediaKey == "" {
				mediaKey = media.Path
			}
			if mediaKey == "" {
				continue
			}
			if !strings.HasSuffix(mediaKey, ".mp4") {
				continue
			}
			if strings.TrimSpace(media.AnalysisID) != "" && media.TaskCreated {
				continue
			}
			candidates = append(candidates, media)
		}
	}
	time.Sleep(time.Second * 2)

	if len(candidates) == 0 {
		log.Println("No media found without analysis")
		return
	}

	log.Printf("Media to reprocess: %d\n", len(candidates))

	// Step 5: Retrieve the selected queue and the connection settings
	bar.Incr()
	queueFound := false
	var selectedQueue models.Queue
	queues := database.GetActiveQueuesFromMongodb(client, mongodbSourceDatabase)
	for _, q := range queues {
		if q.Name == queueName {
			queueFound = true
			selectedQueue = q
			break
		}
	}
	if !queueFound {
		log.Println("Aborting: specified queue was not found in the vault database")
		return
	}

	// Step 6: Connect to the queue service
	bar.Incr()
	queueService, _ := queue.CreateQueue(selectedQueue.Queue, selectedQueue)

	// Step 7: Send analysis events for the missing media
	bar.Incr()
	barProgressMedia := uiprogress.AddBar(len(candidates)).AppendCompleted().PrependElapsed()
	barProgressMedia.PrependFunc(func(b *uiprogress.Bar) string {
		return "Reprocessing media"
	})
	uiprogress.Start()

	batch := 0
	for _, media := range candidates {
		mediaKey := media.Key
		if mediaKey == "" {
			mediaKey = media.Path
		}

		instanceName := media.InstanceName
		if instanceName == "" {
			instanceName = instanceNameFromKey(mediaKey)
		}

		source := media.Provider
		if source == "" {
			source = media.Source
		}

		timestampString := fmt.Sprintf("%d", media.Timestamp)
		event := queue.Payload{
			Events:   []string{"analysis"},
			Provider: "kstorage",
			Request:  "analysis",
			Source:   source,
			Date:     media.Timestamp,
			Payload: queue.Media{
				FileName: mediaKey,
				FileSize: 1,
				MetaData: queue.MetaData{
					UploadTime:   timestampString,
					ProductId:    media.CameraId,
					InstanceName: instanceName,
					Timestamp:    timestampString,
				},
			},
		}

		if mode == "live" {
			e, err := json.Marshal(event)
			if err == nil {
				queueService.SendMessage(selectedQueue.Topic, string(e), 0)
			} else {
				log.Println("Error marshalling the event")
			}
		}

		time.Sleep(time.Millisecond * 100)
		barProgressMedia.Incr()

		batch++
		if batchSize > 0 && batch%batchSize == 0 {
			time.Sleep(time.Millisecond * time.Duration(batchDelay))
		}
	}

	queueService.Close()
	barProgressMedia.Set(100)
	bar.Set(100)

	log.Println("")
	log.Println("Reprocess completed.")
}

func instanceNameFromKey(mediaKey string) string {
	parts := strings.Split(mediaKey, "/")
	if len(parts) < 2 {
		return ""
	}
	file := parts[1]
	fileParts := strings.Split(file, "_")
	if len(fileParts) < 3 {
		return ""
	}
	return fileParts[2]
}
