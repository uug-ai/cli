package actions

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gosuri/uiprogress"
	hubModels "github.com/uug-ai/models/pkg/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	DefaultMarkerTarget         = 1000000
	SmallMarkerTargetThreshold  = 10000
	MediumMarkerTargetThreshold = 1000000

	DefaultMarkerBatchSizeSmall  = 500
	DefaultMarkerBatchSizeMedium = 2000
	DefaultMarkerBatchSizeLarge  = 5000

	MinMarkerParallel        = 1
	MaxMarkerParallel        = 16
	AutoMarkerParallelSmall  = 2
	AutoMarkerParallelMedium = 4
	AutoMarkerParallelLarge  = 8
)

type SeedMarkersConfig struct {
	Target              int
	BatchSize           int
	Parallel            int
	MongoURI            string
	DBName              string
	DeviceColl          string
	MarkerColl          string
	MarkerOptColl       string
	TagOptColl          string
	EventOptColl        string
	MarkerOptRangesColl string
	TagOptRangesColl    string
	EventOptRangesColl  string
	ExistingUserIDHex   string
	DeviceCount         int
	Days                int
}

func (c *SeedMarkersConfig) ApplyDefaults() {
	if c.Target <= 0 {
		c.Target = DefaultMarkerTarget
	}
	if c.BatchSize <= 0 {
		switch {
		case c.Target <= SmallMarkerTargetThreshold:
			c.BatchSize = DefaultMarkerBatchSizeSmall
		case c.Target <= MediumMarkerTargetThreshold:
			c.BatchSize = DefaultMarkerBatchSizeMedium
		default:
			c.BatchSize = DefaultMarkerBatchSizeLarge
		}
	}
	if c.BatchSize > c.Target {
		c.BatchSize = c.Target
	}
	if c.Parallel <= 0 {
		switch {
		case c.Target <= SmallMarkerTargetThreshold:
			c.Parallel = AutoMarkerParallelSmall
		case c.Target <= MediumMarkerTargetThreshold:
			c.Parallel = AutoMarkerParallelMedium
		default:
			c.Parallel = AutoMarkerParallelLarge
		}
	}
	if c.Parallel < MinMarkerParallel {
		c.Parallel = MinMarkerParallel
	}
	if c.Parallel > MaxMarkerParallel {
		c.Parallel = MaxMarkerParallel
	}
	if c.MongoURI == "" {
		c.MongoURI = "mongodb://localhost:27017"
	}
	if c.DBName == "" {
		c.DBName = "kerberos-test"
	}
	if c.DeviceColl == "" {
		c.DeviceColl = "devices"
	}
	if c.MarkerColl == "" {
		c.MarkerColl = "markers"
	}
	if c.MarkerOptColl == "" {
		c.MarkerOptColl = "marker_options"
	}
	if c.TagOptColl == "" {
		c.TagOptColl = "tag_options"
	}
	if c.EventOptColl == "" {
		c.EventOptColl = "event_options"
	}
	if c.MarkerOptRangesColl == "" {
		c.MarkerOptRangesColl = "marker_option_ranges"
	}
	if c.TagOptRangesColl == "" {
		c.TagOptRangesColl = "tag_option_ranges"
	}
	if c.EventOptRangesColl == "" {
		c.EventOptRangesColl = "event_option_ranges"
	}
	if c.DeviceCount < MinDeviceCount {
		c.DeviceCount = MinDeviceCount
	}
	if c.DeviceCount > MaxDeviceCount {
		c.DeviceCount = MaxDeviceCount
	}
	if c.Days <= 0 {
		c.Days = DefaultDays
	}
	if c.Days > MaxDays {
		c.Days = MaxDays
	}
}

func RunSeedMarkers(cfg SeedMarkersConfig) error {
	HandleSignals()
	cfg.ApplyDefaults()

	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().
		ApplyURI(cfg.MongoURI).
		SetServerSelectionTimeout(ServerSelectionTimeoutSeconds*time.Second))
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}
	defer client.Disconnect(ctx)

	db := client.Database(cfg.DBName)
	CreateMarkerIndexes(ctx, db, cfg)

	var userObjectID primitive.ObjectID
	var organisationId string

	if cfg.ExistingUserIDHex != "" {
		userObjectID, err = primitive.ObjectIDFromHex(cfg.ExistingUserIDHex)
		if err != nil {
			return fmt.Errorf("invalid existing user id: %w", err)
		}
		var userDoc bson.M
		err = db.Collection("users").FindOne(ctx, bson.M{"_id": userObjectID}).Decode(&userDoc)
		if err != nil {
			return fmt.Errorf("fetch existing user: %w", err)
		}
		organisationId, _ = userDoc["organisationId"].(string)
	}

	var deviceKeys []string
	filter := bson.M{}
	if organisationId != "" {
		filter["organisationId"] = organisationId
	}
	cursor, err := db.Collection(cfg.DeviceColl).Find(ctx, filter)
	if err != nil {
		return fmt.Errorf("find devices: %w", err)
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var device bson.M
		if err := cursor.Decode(&device); err == nil {
			if key, ok := device["key"].(string); ok && key != "" {
				deviceKeys = append(deviceKeys, key)
			}
		}
	}
	if len(deviceKeys) == 0 {
		return fmt.Errorf("no existing devices found for user/organisation")
	}

	// Query existing groups for the user/organisation
	var groupIds []string
	groupCursor, err := db.Collection("groups").Find(ctx, filter)
	if err != nil {
		return fmt.Errorf("find groups: %w", err)
	}
	defer groupCursor.Close(ctx)
	for groupCursor.Next(ctx) {
		var group bson.M
		if err := groupCursor.Decode(&group); err == nil {
			if id, ok := group["_id"].(primitive.ObjectID); ok {
				groupIds = append(groupIds, id.Hex())
			}
		}
	}

	markerColl := db.Collection(cfg.MarkerColl)
	markerOptColl := db.Collection(cfg.MarkerOptColl)
	tagOptColl := db.Collection(cfg.TagOptColl)
	eventOptColl := db.Collection(cfg.EventOptColl)
	markerOptRangesColl := db.Collection(cfg.MarkerOptRangesColl)
	tagOptRangesColl := db.Collection(cfg.TagOptRangesColl)
	eventOptRangesColl := db.Collection(cfg.EventOptRangesColl)

	var (
		totalInserted int64
		startTime     = time.Now()
		batchCh       = make(chan []hubModels.Marker, cfg.Parallel*2)
		progressCh    = make(chan int, cfg.Parallel*2)
	)

	uiprogress.Start()
	bar := uiprogress.AddBar(cfg.Target).AppendCompleted().PrependElapsed()
	bar.PrependFunc(func(b *uiprogress.Bar) string {
		inserted := atomic.LoadInt64(&totalInserted)
		pct := 100 * float64(inserted) / float64(cfg.Target)
		el := time.Since(startTime).Seconds()
		rate := float64(inserted)
		if el > 0 {
			rate = float64(inserted) / el
		}
		return fmt.Sprintf("Inserted %d/%d (%.1f%%) %.0f/s", inserted, cfg.Target, pct, rate)
	})

	// create a WaitGroup for worker goroutines
	var wg sync.WaitGroup
	// Use unordered bulk writes for resiliency / speed
	bulkOpts := options.BulkWrite().SetOrdered(false)

	// spawn cfg.Parallel workers
	for w := 0; w < cfg.Parallel; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for batch := range batchCh {
				// Bulk insert markers (use InsertMany)
				var markerDocs []interface{}
				for _, marker := range batch {
					// NOTE: We use the marker as-is (typed struct) — ensure Marker.Id has a valid ObjectID.
					markerDocs = append(markerDocs, marker)
				}
				res, err := markerColl.InsertMany(ctx, markerDocs)
				if err != nil {
					// log error but continue with the rest (do not crash whole program)
					fmt.Printf("[warn] worker %d InsertMany markers failed: %v\n", workerID, err)
				} else {
					atomic.AddInt64(&totalInserted, int64(len(res.InsertedIDs)))
					progressCh <- int(atomic.LoadInt64(&totalInserted))
				}

				// dedupe maps
				nameSet := make(map[string]struct{})
				tagSet := make(map[string]struct{})
				eventSet := make(map[string]struct{})

				var markerOptUpserts []mongo.WriteModel
				var tagOptUpserts []mongo.WriteModel
				var eventOptUpserts []mongo.WriteModel

				var markerRangeDocs []interface{}
				var tagRangeDocs []interface{}
				var eventRangeDocs []interface{}

				now := time.Now().Unix()

				for _, marker := range batch {
					// marker option upsert
					if marker.Name != "" {
						if _, exists := nameSet[marker.Name]; !exists {
							nameSet[marker.Name] = struct{}{}
							createdAt := randomTimeLast30Days()
							updatedAt := randomTimeBetween(createdAt, time.Now().Unix())
							up := mongo.NewUpdateOneModel()
							up.SetFilter(bson.M{"value": marker.Name, "organisationId": marker.OrganisationId})
							up.SetUpdate(bson.M{
								"$setOnInsert": bson.M{
									"value":          marker.Name,
									"text":           marker.Name,
									"organisationId": marker.OrganisationId,
									"createdAt":      createdAt,
								},
								"$set": bson.M{
									"updatedAt": updatedAt,
								},
							})
							up.SetUpsert(true)
							markerOptUpserts = append(markerOptUpserts, up)
						}
						markerRangeDocs = append(markerRangeDocs, bson.M{
							"value":          marker.Name,
							"text":           marker.Name,
							"organisationId": marker.OrganisationId,
							"start":          marker.StartTimestamp,
							"end":            marker.EndTimestamp,
							"deviceId":       marker.DeviceId,
							"groupId":        marker.GroupId,
							"createdAt":      now,
						})
					}

					// tags
					for _, tag := range marker.Tags {
						if tag.Name == "" {
							continue
						}
						if _, exists := tagSet[tag.Name]; !exists {
							tagSet[tag.Name] = struct{}{}
							createdAt := randomTimeLast30Days()
							updatedAt := randomTimeBetween(createdAt, time.Now().Unix())
							up := mongo.NewUpdateOneModel()
							up.SetFilter(bson.M{"value": tag.Name, "organisationId": marker.OrganisationId})
							up.SetUpdate(bson.M{
								"$setOnInsert": bson.M{
									"value":          tag.Name,
									"text":           tag.Name,
									"organisationId": marker.OrganisationId,
									"createdAt":      createdAt,
								},
								"$set": bson.M{
									"updatedAt": updatedAt,
								},
							})
							up.SetUpsert(true)
							tagOptUpserts = append(tagOptUpserts, up)
						}
						tagRangeDocs = append(tagRangeDocs, bson.M{
							"value":          tag.Name,
							"text":           tag.Name,
							"organisationId": marker.OrganisationId,
							"start":          marker.StartTimestamp,
							"end":            marker.EndTimestamp,
							"deviceId":       marker.DeviceId,
							"groupId":        marker.GroupId,
							"createdAt":      now,
						})
					}

					// events
					for _, event := range marker.Events {
						if event.Name == "" {
							continue
						}
						if _, exists := eventSet[event.Name]; !exists {
							eventSet[event.Name] = struct{}{}
							createdAt := randomTimeLast30Days()
							updatedAt := randomTimeBetween(createdAt, time.Now().Unix())
							up := mongo.NewUpdateOneModel()
							up.SetFilter(bson.M{"value": event.Name, "organisationId": marker.OrganisationId})
							up.SetUpdate(bson.M{
								"$setOnInsert": bson.M{
									"value":          event.Name,
									"text":           event.Name,
									"organisationId": marker.OrganisationId,
									"createdAt":      createdAt,
								},
								"$set": bson.M{
									"updatedAt": updatedAt,
								},
							})
							up.SetUpsert(true)
							eventOptUpserts = append(eventOptUpserts, up)
						}
						evStart := event.Timestamp
						evEnd := event.Timestamp + 1
						eventRangeDocs = append(eventRangeDocs, bson.M{
							"value":          event.Name,
							"text":           event.Name,
							"organisationId": marker.OrganisationId,
							"start":          evStart,
							"end":            evEnd,
							"deviceId":       marker.DeviceId,
							"groupId":        marker.GroupId,
							"createdAt":      now,
							"updatedAt":      now,
						})
					}
				}

				// Bulk upsert options (unordered to be faster / resilient)
				if len(markerOptUpserts) > 0 {
					_, err := markerOptColl.BulkWrite(ctx, markerOptUpserts, bulkOpts)
					if err != nil {
						fmt.Printf("[warn] worker %d markerOpt BulkWrite error: %v\n", workerID, err)
					}
				}
				if len(tagOptUpserts) > 0 {
					_, err := tagOptColl.BulkWrite(ctx, tagOptUpserts, bulkOpts)
					if err != nil {
						fmt.Printf("[warn] worker %d tagOpt BulkWrite error: %v\n", workerID, err)
					}
				}
				if len(eventOptUpserts) > 0 {
					_, err := eventOptColl.BulkWrite(ctx, eventOptUpserts, bulkOpts)
					if err != nil {
						fmt.Printf("[warn] worker %d eventOpt BulkWrite error: %v\n", workerID, err)
					}
				}

				insertInChunks := func(coll *mongo.Collection, docs []interface{}) {
					if len(docs) == 0 {
						return
					}
					const chunkSize = 1000
					for s := 0; s < len(docs); s += chunkSize {
						e := s + chunkSize
						if e > len(docs) {
							e = len(docs)
						}
						if _, err := coll.InsertMany(ctx, docs[s:e]); err != nil {
							fmt.Printf("[warn] worker %d InsertMany ranges error (%s): %v\n", workerID, coll.Name(), err)
						}
					}
				}

				if len(markerRangeDocs) > 0 {
					insertInChunks(markerOptRangesColl, markerRangeDocs)
				}
				if len(tagRangeDocs) > 0 {
					insertInChunks(tagOptRangesColl, tagRangeDocs)
				}
				if len(eventRangeDocs) > 0 {
					insertInChunks(eventOptRangesColl, eventRangeDocs)
				}
			}
		}(w)
	}

	go func() {
		for total := range progressCh {
			if total > cfg.Target {
				total = cfg.Target
			}
			bar.Set(total)
		}
	}()

	var queued int64
	rand.Seed(time.Now().UnixNano())
	for atomic.LoadInt64(&queued) < int64(cfg.Target) && atomic.LoadInt32(&stopFlag) == 0 {
		remaining := int64(cfg.Target) - atomic.LoadInt64(&queued)
		current := cfg.BatchSize
		if remaining < int64(current) {
			current = int(remaining)
		}
		batch := generateBatchMarkers(current, cfg.Days, cfg.ExistingUserIDHex, deviceKeys, groupIds)
		batchCh <- batch
		atomic.AddInt64(&queued, int64(current))
	}

	close(batchCh)
	wg.Wait()
	close(progressCh)

	finalTotal := int(atomic.LoadInt64(&totalInserted))
	if finalTotal > cfg.Target {
		finalTotal = cfg.Target
	}
	bar.Set(finalTotal)
	uiprogress.Stop()

	el := time.Since(startTime).Seconds()
	rate := float64(finalTotal) / el
	fmt.Printf("[done] inserted=%d elapsed=%.2fs rate=%.0f/s stop=%v\n",
		finalTotal, el, rate, atomic.LoadInt32(&stopFlag) != 0)

	if atomic.LoadInt32(&stopFlag) != 0 {
		return fmt.Errorf("interrupted")
	}
	return nil
}

func generateBatchMarkers(batchSize int, days int, organisationId string, deviceKeys []string, groupIds []string) []hubModels.Marker {
	tagNames := []string{
		"vehicle", "license plate", "security", "entrance", "exit", "parking-lot",
		"lobby-area", "staff", "visitor", "delivery", "motion", "sound", "door", "window",
		"alarm", "camera", "gate", "restricted", "public", "emergency", "maintenance",
		"fire exit", "loading dock", "corridor", "break room", "conference room", "server room",
		"roof access", "basement", "elevator", "stairwell", "main entrance", "side entrance",
	}

	eventNames := []string{
		"Motion Detected", "Sound Detected", "Door Opened", "Glass Break", "Tamper Alarm",
		"Camera Offline", "Low Battery", "Power Restored", "Network Down", "Network Restored",
		"Intrusion Detected", "Fire Alarm", "System Check", "Manual Trigger", "Temperature Alert",
		"Humidity Alert", "Light Level Change", "Object Left Behind", "Object Removed",
		"Face Recognized", "License Plate Read", "Crowd Detected", "Loitering", "Line Crossed",
		"Access Denied", "Access Granted", "Badge Scan", "RFID Scan", "Smoke Detected",
		"Water Leak", "Vibration Detected", "Panic Button", "Emergency Call", "Maintenance Required",
		"Battery Replaced", "Firmware Updated", "Device Restarted", "Device Shutdown", "Device Started",
		"System Armed", "System Disarmed", "Alarm Silenced", "Alarm Triggered", "Zone Breach",
		"Zone Cleared", "Visitor Registered", "Delivery Arrived", "Delivery Departed", "Staff Checked In",
	}

	deviceCount := len(deviceKeys)
	groupCount := len(groupIds)

	var batch []hubModels.Marker
	now := time.Now().Unix()
	maxDays := 30
	secondsIn30Days := int64(maxDays * 86400)
	for i := 0; i < batchSize; i++ {
		start := now - int64(rand.Intn(int(secondsIn30Days)))
		duration := int64(rand.Intn(600) + 1)
		end := start + duration

		// Randomly choose device or group
		var deviceId, groupId string
		if deviceCount > 0 && groupCount > 0 {
			if rand.Intn(2) == 0 {
				deviceId = deviceKeys[rand.Intn(deviceCount)]
				groupId = ""
			} else {
				groupId = groupIds[rand.Intn(groupCount)]
				deviceId = ""
			}
		} else if deviceCount > 0 {
			deviceId = deviceKeys[rand.Intn(deviceCount)]
			groupId = ""
		} else if groupCount > 0 {
			groupId = groupIds[rand.Intn(groupCount)]
			deviceId = ""
		}

		numTags := rand.Intn(4) + 1
		tagIndexes := rand.Perm(len(tagNames))[:numTags]
		var tags []hubModels.MarkerTag
		for _, idx := range tagIndexes {
			tags = append(tags, hubModels.MarkerTag{Name: tagNames[idx]})
		}

		numEvents := rand.Intn(3) + 1
		var events []hubModels.MarkerEvent
		for j := 0; j < numEvents; j++ {
			name := eventNames[rand.Intn(len(eventNames))]
			events = append(events, hubModels.MarkerEvent{
				Timestamp:   start + int64(j*5) + int64(rand.Intn(10)),
				Name:        name,
				Description: "Random event description",
				Tags:        []string{"urgent"},
			})
		}

		marker := hubModels.Marker{
			Id:             primitive.NewObjectID(),
			DeviceId:       deviceId,
			SiteId:         randomSiteId(),
			GroupId:        groupId,
			OrganisationId: organisationId,
			StartTimestamp: start,
			EndTimestamp:   end,
			Duration:       duration,
			Name:           randomMarkerName(i),
			Events:         events,
			Description:    "Random marker description",
			Metadata:       &hubModels.MarkerMetadata{Comments: nil},
			Synchronize:    nil,
			Audit:          nil,
			Tags:           tags,
		}
		batch = append(batch, marker)
	}
	return batch
}

type MarkerName struct {
	Name string `bson:"name"`
}

func SeedMarkers(
	target int,
	batchSize int,
	parallel int,
	mongoURI string,
	dbName string,
	deviceColl string,
	groupColl string,
	markerColl string,
	markerOptColl string,
	tagOptColl string,
	eventOptColl string,
	markerOptRangesColl string,
	tagOptRangesColl string,
	eventOptRangesColl string,
	existingUserIDHex string,
	deviceCount int,
	days int,
) {
	HandleSignals()
	flag.Parse()

	cfg := SeedMarkersConfig{
		Target:              target,
		BatchSize:           batchSize,
		Parallel:            parallel,
		MongoURI:            mongoURI,
		DBName:              dbName,
		DeviceColl:          deviceColl,
		MarkerColl:          markerColl,
		MarkerOptColl:       markerOptColl,
		TagOptColl:          tagOptColl,
		EventOptColl:        eventOptColl,
		MarkerOptRangesColl: markerOptRangesColl,
		TagOptRangesColl:    tagOptRangesColl,
		EventOptRangesColl:  eventOptRangesColl,
		ExistingUserIDHex:   existingUserIDHex,
		DeviceCount:         deviceCount,
		Days:                days,
	}
	if err := RunSeedMarkers(cfg); err != nil {
		fmt.Printf("[error] seed markers: %v\n", err)
		os.Exit(1)
	}
}

// Helper functions for random data generation
func randomSiteId() string {
	return "site_" + primitive.NewObjectID().Hex()
}
func randomGroupId() string {
	return "group_" + primitive.NewObjectID().Hex()
}
func randomMarkerName(i int) string {
	return "marker-" + fmt.Sprintf("%d", int64(rand.Intn(1000000)+3000000))
}
func randomMarkerEvent(ts int64) hubModels.MarkerEvent {
	events := []string{
		"Motion Detected", "Sound Detected", "Door Opened", "Glass Break", "Tamper Alarm",
		"Camera Offline", "Low Battery", "Power Restored", "Network Down", "Network Restored",
		"Intrusion Detected", "Fire Alarm", "System Check", "Manual Trigger", "Temperature Alert",
		"Humidity Alert", "Light Level Change", "Object Left Behind", "Object Removed",
		"Face Recognized", "License Plate Read", "Crowd Detected", "Loitering", "Line Crossed",
	}
	name := events[rand.Intn(len(events))]
	return hubModels.MarkerEvent{
		Timestamp:   ts + int64(rand.Intn(10)),
		Name:        name,
		Description: "Random event description",
		Tags:        []string{"urgent"},
	}
}

func CreateMarkerIndexes(ctx context.Context, db *mongo.Database, cfg SeedMarkersConfig) {
	// Markers - index fields expected on marker documents
	markerIndexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "startTimestamp", Value: 1}}},
		{Keys: bson.D{{Key: "deviceId", Value: 1}}},
		{Keys: bson.D{{Key: "organisationId", Value: 1}}},
		// tags is an array of objects with "name" -> index tags.name for better matching
		{Keys: bson.D{{Key: "tags.name", Value: 1}}},
		{Keys: bson.D{{Key: "duration", Value: 1}}},
		{Keys: bson.D{{Key: "name", Value: 1}}},
	}
	if _, err := db.Collection(cfg.MarkerColl).Indexes().CreateMany(ctx, markerIndexes); err != nil {
		fmt.Printf("[info] marker index creation skipped: %v\n", err)
	}

	// Option collections - value/text
	optIndexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "value", Value: 1}}, Options: options.Index().SetUnique(false)},
		{Keys: bson.D{{Key: "text", Value: 1}}},
	}
	if _, err := db.Collection(cfg.MarkerOptColl).Indexes().CreateMany(ctx, optIndexes); err != nil {
		fmt.Printf("[info] marker_options index creation skipped: %v\n", err)
	}
	if _, err := db.Collection(cfg.TagOptColl).Indexes().CreateMany(ctx, optIndexes); err != nil {
		fmt.Printf("[info] tag_options index creation skipped: %v\n", err)
	}
	if _, err := db.Collection(cfg.EventOptColl).Indexes().CreateMany(ctx, optIndexes); err != nil {
		fmt.Printf("[info] event_options index creation skipped: %v\n", err)
	}

	rangeIndexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "value", Value: 1}}},
		{Keys: bson.D{{Key: "start", Value: 1}}},
		{Keys: bson.D{{Key: "end", Value: 1}}},
		{Keys: bson.D{{Key: "deviceId", Value: 1}}},
	}
	if _, err := db.Collection(cfg.MarkerOptRangesColl).Indexes().CreateMany(ctx, rangeIndexes); err != nil {
		fmt.Printf("[info] marker_option_ranges index creation skipped: %v\n", err)
	}
	if _, err := db.Collection(cfg.TagOptRangesColl).Indexes().CreateMany(ctx, rangeIndexes); err != nil {
		fmt.Printf("[info] tag_option_ranges index creation skipped: %v\n", err)
	}
	if _, err := db.Collection(cfg.EventOptRangesColl).Indexes().CreateMany(ctx, rangeIndexes); err != nil {
		fmt.Printf("[info] event_option_ranges index creation skipped: %v\n", err)
	}
}
func randomTimeLast30Days() int64 {
	now := time.Now().Unix()
	secondsIn30Days := int64(30 * 86400)
	return now - int64(rand.Intn(int(secondsIn30Days)))
}

// Helper to generate a random timestamp between two times
func randomTimeBetween(start, end int64) int64 {
	if end <= start {
		return start
	}
	return start + int64(rand.Intn(int(end-start)))
}

/*
Connect to MongoDB

Establishes a client connection to the MongoDB server.
Create Indexes

Calls CreateMarkerIndexes to ensure all necessary indexes exist for marker, option, and range collections.
User Lookup (Optional)

If ExistingUserIDHex is provided, fetches the user document from the users collection.
Device Generation and Insert

Generates device documents.
Inserts devices into the devices collection using InsertMany.
Marker Seeding Loop (Batch Processing)

For each batch:
Bulk Insert Markers
Inserts marker documents into the markers collection using InsertMany.
Deduplicate Names/Tags/Events
Collects unique marker names, tag names, and event names in the batch.
Bulk Upsert Marker/Tag/Event Options
Upserts unique marker names into marker_options using BulkWrite (with upsert).
Upserts unique tag names into tag_options using BulkWrite (with upsert).
Upserts unique event names into event_type_options using BulkWrite (with upsert).
Bulk Insert Time Ranges
Inserts marker time ranges into marker_option_ranges using InsertMany.
Inserts tag time ranges into tag_option_ranges using InsertMany.
Inserts event time ranges into event_type_option_ranges using InsertMany.
Progress Tracking

Updates progress bar after each batch.
Cleanup

Closes batch channels and waits for all workers to finish.
*/
