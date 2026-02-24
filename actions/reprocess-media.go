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
	pipeline "github.com/uug-ai/models/pkg/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
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

	location, err := time.LoadLocation(timezone)
	if err != nil {
		log.Printf("Invalid timezone %q, defaulting to UTC: %v", timezone, err)
		location = time.UTC
	}
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
	log.Println("  Start Time:", startTime)
	log.Println("  End Time:", endTime)
	log.Println("====================================")
	fmt.Println("")

	if userId == "" {
		log.Println("Please provide a valid user ID")
		return
	}
	if mongodbDestinationDatabase != "" && mongodbDestinationDatabase != mongodbSourceDatabase {
		log.Println("Warning: reprocess-media runs within one database; using source database for sequences.")
	}
	if startTimestamp <= 0 || endTimestamp <= 0 || startTimestamp >= endTimestamp {
		log.Println("Please provide valid, positive start and end timestamps where start < end")
		return
	}

	var steps = []string{
		"MongoDB: verify connection",
		"MongoDB: open connection",
		"Hub: load user metadata",
		"Hub: query sequences",
		"Hub: filter media without analysis",
		"Hub: get queue connection",
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

	// Step 3: Load user + subscription + plan settings (analysis expects monitor stage)
	bar.Incr()
	user, subscription, plans, err := loadMonitorContext(client, mongodbSourceDatabase, userId)
	if err != nil {
		log.Printf("Error loading monitor context: %v\n", err)
		return
	}
	if user.Email == "" {
		log.Println("Warning: user has no email; analysis pipeline will skip these events")
	}
	time.Sleep(time.Second * 2)

	// Step 4: Fetch sequences for the user in the hub database
	bar.Incr()
	sequences := database.GetSequencesFromMongodb(client, mongodbSourceDatabase, userId, startTimestamp, endTimestamp)
	time.Sleep(time.Second * 2)

	// Step 5: Filter media that has no analysis
	bar.Incr()
	var candidates []models.Media
	for _, sequence := range sequences {
		for _, media := range sequence.Images {
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

	// Step 6: Retrieve the selected queue and the connection settings
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

	// Step 7: Connect to the queue service
	bar.Incr()
	queueService, err := queue.CreateQueue(selectedQueue.Queue, selectedQueue)
	if err != nil {
		log.Printf("Aborting: failed to connect to queue %s: %v\n", selectedQueue.Queue, err)
		return
	}

	// Step 8: Send analysis events for the missing media
	bar.Incr()
	barProgressMedia := uiprogress.AddBar(len(candidates)).AppendCompleted().PrependElapsed()
	barProgressMedia.PrependFunc(func(b *uiprogress.Bar) string {
		return "Reprocessing media"
	})
	uiprogress.Start()

	batch := 0
	log.Printf("Sending %d analysis events to the queue...\n", len(candidates))
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
		monitorStage := pipeline.NewMonitorStage()
		monitorStage.User = user
		monitorStage.Subscription = subscription
		monitorStage.Plans = toPipelinePlans(plans)
		monitorStage.HighUpload = user.HighUpload

		event := pipeline.PipelineEvent{
			Operation: "event",
			Stages:    []string{"analysis"},
			Storage:   "kstorage",
			Provider:  source,
			Timestamp: media.Timestamp,
			Payload: pipeline.PipelinePayload{
				Timestamp: media.Timestamp,
				FileName:  mediaKey,
				FileSize:  1,
				Metadata: pipeline.PipelineMetadata{
					Timestamp:  timestampString,
					Duration:   "",
					UploadTime: timestampString,
					DeviceId:   media.CameraId,
					DeviceName: instanceName,
				},
			},
			MonitorStage: &monitorStage,
		}

		if mode == "live" {
			e, err := json.Marshal(event)
			if err == nil {
				queueService.SendMessage(selectedQueue.Topic, string(e), 0)
			} else {
				log.Println("Error marshalling the event")
			}
		} else {
			// Dry-run mode: log the event that would be sent instead of sending it.
			e, err := json.MarshalIndent(event, "", "  ")
			if err != nil {
				log.Printf("Dry-run: error marshalling event for media %s (timestamp %d): %v", mediaKey, media.Timestamp, err)
			} else {
				log.Printf("Dry-run: would send event: %s", string(e))
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

type planSettings struct {
	Key string                 `bson:"key"`
	Map map[string]interface{} `bson:"map"`
}

func defaultPlanSettings() map[string]interface{} {
	return map[string]interface{}{
		"enterprise": map[string]interface{}{
			"analysisLimit": float64(99999999),
			"uploadLimit":   float64(99999999),
			"videoLimit":    float64(99999999),
			"usage":         float64(99999999),
			"dayLimit":      float64(30),
		},
	}
}

func numberToInt(value interface{}) int {
	switch v := value.(type) {
	case int:
		return v
	case int8:
		return int(v)
	case int16:
		return int(v)
	case int32:
		return int(v)
	case int64:
		return int(v)
	case uint:
		return int(v)
	case uint8:
		return int(v)
	case uint16:
		return int(v)
	case uint32:
		return int(v)
	case uint64:
		return int(v)
	case float32:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}

func toPipelinePlan(value interface{}) (pipeline.Plan, bool) {
	if typed, ok := value.(pipeline.Plan); ok {
		return typed, true
	}
	raw, ok := value.(map[string]interface{})
	if !ok {
		return pipeline.Plan{}, false
	}
	return pipeline.Plan{
		Level:         numberToInt(raw["level"]),
		UploadLimit:   numberToInt(raw["uploadLimit"]),
		VideoLimit:    numberToInt(raw["videoLimit"]),
		Usage:         numberToInt(raw["usage"]),
		AnalysisLimit: numberToInt(raw["analysisLimit"]),
		DayLimit:      numberToInt(raw["dayLimit"]),
	}, true
}

func toPipelinePlans(raw map[string]interface{}) map[string]pipeline.Plan {
	plans := make(map[string]pipeline.Plan, len(raw))
	for key, value := range raw {
		if plan, ok := toPipelinePlan(value); ok {
			plans[key] = plan
		}
	}
	return plans
}

func loadMonitorContext(client *mongo.Client, dbName, userId string) (pipeline.User, pipeline.Subscription, map[string]interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), database.TIMEOUT)
	defer cancel()

	db := client.Database(dbName)
	var user pipeline.User
	var subscription pipeline.Subscription
	plans := map[string]interface{}{}

	if oid, err := primitive.ObjectIDFromHex(userId); err == nil {
		err = db.Collection("users").FindOne(ctx, bson.M{"_id": oid}).Decode(&user)
		if err != nil && err != mongo.ErrNoDocuments {
			return user, subscription, plans, err
		}
	}
	if user.Id.IsZero() {
		err := db.Collection("users").FindOne(ctx, bson.M{"username": userId}).Decode(&user)
		if err != nil {
			return user, subscription, plans, err
		}
	}

	subUserID := userId
	if user.Id.IsZero() == false {
		subUserID = user.Id.Hex()
	}
	err := db.Collection("subscriptions").FindOne(ctx, bson.M{"user_id": subUserID}).Decode(&subscription)
	if err != nil && err != mongo.ErrNoDocuments {
		return user, subscription, plans, err
	}
	if subscription.StripePlan == "" {
		if user.Plan != "" {
			subscription.StripePlan = user.Plan
		} else {
			subscription.StripePlan = "enterprise"
		}
		subscription.UserId = subUserID
	}

	var settings planSettings
	err = db.Collection("settings").FindOne(ctx, bson.M{"key": "plan"}).Decode(&settings)
	if err == nil && len(settings.Map) > 0 {
		plans = settings.Map
	} else {
		plans = defaultPlanSettings()
	}

	if len(user.Activity) == 0 {
		user.Activity = []pipeline.Activity{
			{
				Day:      time.Now().Format("02-01-2006"),
				Requests: 0,
				Videos:   0,
				Images:   0,
				Usage:    0,
			},
		}
	}

	return user, subscription, plans, nil
}
