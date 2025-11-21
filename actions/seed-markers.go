package actions

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	"github.com/gosuri/uiprogress"
	"github.com/uug-ai/cli/database"
	hubModels "github.com/uug-ai/models/pkg/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	DefaultMarkerTarget         = 100000
	SmallMarkerTargetThreshold  = 10000
	MediumMarkerTargetThreshold = 100000

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
	Target                 int
	BatchSize              int
	Parallel               int
	MongoURI               string
	DBName                 string
	DeviceColl             string
	MarkerColl             string
	MarkerOptColl          string
	TagOptColl             string
	EventTypeOptColl       string
	MarkerOptRangesColl    string
	TagOptRangesColl       string
	EventTypeOptRangesColl string
	ExistingUserIDHex      string
	DeviceCount            int
	Days                   int
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
	if c.EventTypeOptColl == "" {
		c.EventTypeOptColl = "event_type_options"
	}
	if c.MarkerOptRangesColl == "" {
		c.MarkerOptRangesColl = "marker_option_ranges"
	}
	if c.TagOptRangesColl == "" {
		c.TagOptRangesColl = "tag_option_ranges"
	}
	if c.EventTypeOptRangesColl == "" {
		c.EventTypeOptRangesColl = "event_type_option_ranges"
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
	var amazonSecretAccessKey, amazonAccessKeyID string

	if cfg.ExistingUserIDHex != "" {
		userObjectID, err = primitive.ObjectIDFromHex(cfg.ExistingUserIDHex)
		if err != nil {
			return fmt.Errorf("invalid existing user id: %w", err)
		}
		var userDoc bson.M
		err = client.Database(cfg.DBName).Collection("users").
			FindOne(ctx, bson.M{"_id": userObjectID}).Decode(&userDoc)
		if err != nil {
			return fmt.Errorf("fetch existing user: %w", err)
		}
		amazonSecretAccessKey, _ = userDoc["amazon_secret_access_key"].(string)
		amazonAccessKeyID, _ = userDoc["amazon_access_key_id"].(string)
		if amazonSecretAccessKey == "" || amazonAccessKeyID == "" {
			return fmt.Errorf("existing user missing keys")
		}
	}

	deviceDocs, _ := database.BuildDeviceDocs(cfg.DeviceCount, userObjectID, amazonSecretAccessKey)
	if len(deviceDocs) == 0 {
		return fmt.Errorf("no devices generated")
	}
	if err := database.InsertMany(ctx, client, cfg.DBName, cfg.DeviceColl, deviceDocs); err != nil {
		return fmt.Errorf("insert devices: %w", err)
	}

	// Extract ObjectIDs from deviceDocs
	var deviceObjIDs []primitive.ObjectID
	for _, doc := range deviceDocs {
		if m, ok := doc.(bson.M); ok {
			if id, ok := m["_id"].(primitive.ObjectID); ok {
				deviceObjIDs = append(deviceObjIDs, id)
			}
		}
	}

	if len(deviceObjIDs) == 0 {
		return fmt.Errorf("no valid device ObjectIDs found")
	}

	markerColl := client.Database(cfg.DBName).Collection(cfg.MarkerColl)
	markerOptColl := client.Database(cfg.DBName).Collection(cfg.MarkerOptColl)
	tagOptColl := client.Database(cfg.DBName).Collection(cfg.TagOptColl)
	eventTypeOptColl := client.Database(cfg.DBName).Collection(cfg.EventTypeOptColl)
	markerOptRangesColl := client.Database(cfg.DBName).Collection(cfg.MarkerOptRangesColl)
	tagOptRangesColl := client.Database(cfg.DBName).Collection(cfg.TagOptRangesColl)
	eventTypeOptRangesColl := client.Database(cfg.DBName).Collection(cfg.EventTypeOptRangesColl)

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

	doneWorkers := make(chan struct{})
	go func() {
		for batch := range batchCh {
			var docs []interface{}
			for _, marker := range batch {
				docs = append(docs, marker)
			}
			res, err := markerColl.InsertMany(ctx, docs)
			if err == nil {
				atomic.AddInt64(&totalInserted, int64(len(res.InsertedIDs)))
				progressCh <- int(atomic.LoadInt64(&totalInserted))
			}
			// Upsert options and ranges for each marker in batch
			for _, marker := range batch {
				// MarkerOption
				markerOpt := hubModels.MarkerOption{
					Value: marker.Name,
					Text:  marker.Name,
					TimeRanges: []hubModels.MarkerTimeRange{
						{Start: marker.StartTimestamp, End: marker.EndTimestamp, Name: &marker.Name, DeviceId: &marker.DeviceId},
					},
				}
				_, _ = markerOptColl.UpdateOne(
					ctx,
					bson.M{"value": markerOpt.Value},
					bson.M{"$set": markerOpt},
					options.Update().SetUpsert(true),
				)
				_, _ = markerOptRangesColl.InsertOne(ctx, bson.M{
					"name":      markerOpt.Text,
					"timeRange": markerOpt.TimeRanges,
				})

				// TagOption
				for _, tag := range marker.Tags {
					tagOpt := hubModels.MarkerTagOptions{
						Value: tag,
						Text:  tag,
						TimeRanges: []hubModels.MarkerTimeRange{
							{Start: marker.StartTimestamp, End: marker.EndTimestamp, Name: &tag, DeviceId: &marker.DeviceId},
						},
					}
					_, _ = tagOptColl.UpdateOne(
						ctx,
						bson.M{"value": tagOpt.Value},
						bson.M{"$set": tagOpt},
						options.Update().SetUpsert(true),
					)
					_, _ = tagOptRangesColl.InsertOne(ctx, bson.M{
						"name":      tagOpt.Text,
						"timeRange": tagOpt.TimeRanges,
					})
				}

				// EventTypeOption
				for _, event := range marker.Events {
					eventType := string(event.Type.Name)
					eventTypeOpt := hubModels.MarkerEventTypeOption{
						Value: eventType,
						Text:  eventType,
						TimeRanges: []hubModels.MarkerTimeRange{
							{Start: event.Timestamp, End: event.Timestamp + 1, Name: &eventType, DeviceId: &marker.DeviceId},
						},
					}
					_, _ = eventTypeOptColl.UpdateOne(
						ctx,
						bson.M{"value": eventTypeOpt.Value},
						bson.M{"$set": eventTypeOpt},
						options.Update().SetUpsert(true),
					)
					_, _ = eventTypeOptRangesColl.InsertOne(ctx, bson.M{
						"name":      eventTypeOpt.Text,
						"timeRange": eventTypeOpt.TimeRanges,
					})
				}
			}
		}
		close(doneWorkers)
	}()

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
		batch := generateBatchMarkers(current, cfg.Days, cfg.ExistingUserIDHex, deviceObjIDs)
		batchCh <- batch
		atomic.AddInt64(&queued, int64(current))
	}

	close(batchCh)
	<-doneWorkers
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

func generateBatchMarkers(batchSize int, days int, organisationId string, deviceIDs []primitive.ObjectID) []hubModels.Marker {
	var batch []hubModels.Marker
	for i := 0; i < batchSize; i++ {
		start := time.Now().Unix() - int64(rand.Intn(days*86400))
		duration := int64(rand.Intn(60) + 10)
		end := start + duration
		deviceId := deviceIDs[rand.Intn(len(deviceIDs))].Hex()
		marker := hubModels.Marker{
			Id:             primitive.NewObjectID(),
			MediaIds:       []string{randomMediaId(), randomMediaId()},
			DeviceId:       deviceId,
			SiteId:         randomSiteId(),
			GroupId:        randomGroupId(),
			OrganisationId: organisationId,
			StartTimestamp: start,
			EndTimestamp:   end,
			Duration:       duration,
			Name:           randomMarkerName(i),
			Tag:            randomTag(),
			Events:         []hubModels.MarkerEvent{randomMarkerEvent(start)},
			Description:    "Random marker description",
			Metadata:       &hubModels.MarkerMetadata{Tags: []string{"vehicle", "security"}},
			Synchronize:    nil,
			Audit:          nil,
			Tags:           []string{"vehicle", "license plate"},
		}
		batch = append(batch, marker)
	}
	return batch
}

func SeedMarkersOnly(
	target int,
	batchSize int,
	parallel int,
	mongoURI string,
	dbName string,
	deviceColl string,
	markerColl string,
	markerOptColl string,
	tagOptColl string,
	eventTypeOptColl string,
	markerOptRangesColl string,
	tagOptRangesColl string,
	eventTypeOptRangesColl string,
	existingUserIDHex string,
	deviceCount int,
	days int,
) {
	HandleSignals()
	flag.Parse()

	cfg := SeedMarkersConfig{
		Target:                 target,
		BatchSize:              batchSize,
		Parallel:               parallel,
		MongoURI:               mongoURI,
		DBName:                 dbName,
		DeviceColl:             deviceColl,
		MarkerColl:             markerColl,
		MarkerOptColl:          markerOptColl,
		TagOptColl:             tagOptColl,
		EventTypeOptColl:       eventTypeOptColl,
		MarkerOptRangesColl:    markerOptRangesColl,
		TagOptRangesColl:       tagOptRangesColl,
		EventTypeOptRangesColl: eventTypeOptRangesColl,
		ExistingUserIDHex:      existingUserIDHex,
		DeviceCount:            deviceCount,
		Days:                   days,
	}
	if err := RunSeedMarkersOnly(cfg); err != nil {
		fmt.Printf("[error] seed markers only: %v\n", err)
		os.Exit(1)
	}
}

type MarkerName struct {
	Name string `bson:"name"`
}

func RunSeedMarkersOnly(cfg SeedMarkersConfig) error {
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

	var userObjectID primitive.ObjectID
	var amazonSecretAccessKey string

	if cfg.ExistingUserIDHex != "" {
		userObjectID, err = primitive.ObjectIDFromHex(cfg.ExistingUserIDHex)
		if err != nil {
			return fmt.Errorf("invalid existing user id: %w", err)
		}
		var userDoc bson.M
		err = client.Database(cfg.DBName).Collection("users").
			FindOne(ctx, bson.M{"_id": userObjectID}).Decode(&userDoc)
		if err != nil {
			return fmt.Errorf("fetch existing user: %w", err)
		}
		amazonSecretAccessKey, _ = userDoc["amazon_secret_access_key"].(string)
	}

	deviceDocs, _ := database.BuildDeviceDocs(cfg.DeviceCount, userObjectID, amazonSecretAccessKey)
	if len(deviceDocs) == 0 {
		return fmt.Errorf("no devices generated")
	}
	if err := database.InsertMany(ctx, client, cfg.DBName, cfg.DeviceColl, deviceDocs); err != nil {
		return fmt.Errorf("insert devices: %w", err)
	}

	var deviceObjIDs []primitive.ObjectID
	for _, doc := range deviceDocs {
		if m, ok := doc.(bson.M); ok {
			if id, ok := m["_id"].(primitive.ObjectID); ok {
				deviceObjIDs = append(deviceObjIDs, id)
			}
		}
	}
	if len(deviceObjIDs) == 0 {
		return fmt.Errorf("no valid device ObjectIDs found")
	}

	markerColl := client.Database(cfg.DBName).Collection(cfg.MarkerColl)
	markerOptColl := client.Database(cfg.DBName).Collection(cfg.MarkerOptColl)

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

	doneWorkers := make(chan struct{})
	go func() {
		for batch := range batchCh {
			// Insert markers in bulk
			var docs []interface{}
			for _, marker := range batch {
				docs = append(docs, marker)
			}
			res, err := markerColl.InsertMany(ctx, docs)
			if err == nil {
				atomic.AddInt64(&totalInserted, int64(len(res.InsertedIDs)))
				progressCh <- int(atomic.LoadInt64(&totalInserted))
			}

			// Upsert marker names into markerOptColl (normalized)
			seenNames := make(map[string]struct{})
			for _, marker := range batch {
				if _, exists := seenNames[marker.Name]; exists {
					continue
				}
				seenNames[marker.Name] = struct{}{}
				markerName := MarkerName{Name: marker.Name}
				_, _ = markerOptColl.UpdateOne(
					ctx,
					bson.M{"name": markerName.Name},
					bson.M{"$setOnInsert": markerName},
					options.Update().SetUpsert(true),
				)
			}
		}
		close(doneWorkers)
	}()

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
		batch := generateBatchMarkers(current, cfg.Days, cfg.ExistingUserIDHex, deviceObjIDs)
		batchCh <- batch
		atomic.AddInt64(&queued, int64(current))
	}

	close(batchCh)
	<-doneWorkers
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

func SeedMarkers(
	target int,
	batchSize int,
	parallel int,
	mongoURI string,
	dbName string,
	deviceColl string,
	markerColl string,
	markerOptColl string,
	tagOptColl string,
	eventTypeOptColl string,
	markerOptRangesColl string,
	tagOptRangesColl string,
	eventTypeOptRangesColl string,
	existingUserIDHex string,
	deviceCount int,
	days int,
) {
	HandleSignals()
	flag.Parse()

	cfg := SeedMarkersConfig{
		Target:                 target,
		BatchSize:              batchSize,
		Parallel:               parallel,
		MongoURI:               mongoURI,
		DBName:                 dbName,
		DeviceColl:             deviceColl,
		MarkerColl:             markerColl,
		MarkerOptColl:          markerOptColl,
		TagOptColl:             tagOptColl,
		EventTypeOptColl:       eventTypeOptColl,
		MarkerOptRangesColl:    markerOptRangesColl,
		TagOptRangesColl:       tagOptRangesColl,
		EventTypeOptRangesColl: eventTypeOptRangesColl,
		ExistingUserIDHex:      existingUserIDHex,
		DeviceCount:            deviceCount,
		Days:                   days,
	}
	if err := RunSeedMarkers(cfg); err != nil {
		fmt.Printf("[error] seed markers: %v\n", err)
		os.Exit(1)
	}
}

// Helper functions for random data generation
func randomMediaId() string {
	return "img_" + primitive.NewObjectID().Hex() + ".jpg"
}
func randomSiteId() string {
	return "site_" + primitive.NewObjectID().Hex()
}
func randomGroupId() string {
	return "group_" + primitive.NewObjectID().Hex()
}
func randomMarkerName(i int) string {
	return "marker-" + fmt.Sprintf("%d", i)
}
func randomTag() string {
	tags := []string{"entrance-camera-1", "lobby-area", "parking-lot"}
	return tags[rand.Intn(len(tags))]
}
func randomMarkerEvent(ts int64) hubModels.MarkerEvent {
	eventTypes := []string{"Motion Detected", "Sound Detected", "Door Opened"}
	name := eventTypes[rand.Intn(len(eventTypes))]
	return hubModels.MarkerEvent{
		Id:          primitive.NewObjectID(),
		Timestamp:   ts + int64(rand.Intn(10)),
		Type:        hubModels.MarkerEventType{Name: name},
		Name:        name,
		Description: "Random event description",
		Tags:        []string{"urgent"},
	}
}

func CreateMarkerIndexes(ctx context.Context, db *mongo.Database, cfg SeedMarkersConfig) {
	// Markers
	markerIndexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "startTimestamp", Value: 1}}},
		{Keys: bson.D{{Key: "deviceId", Value: 1}}},
		{Keys: bson.D{{Key: "organisationId", Value: 1}}},
		{Keys: bson.D{{Key: "tags", Value: 1}}},
		{Keys: bson.D{{Key: "duration", Value: 1}}},
		{Keys: bson.D{{Key: "name", Value: 1}}},
	}
	_, err := db.Collection(cfg.MarkerColl).Indexes().CreateMany(ctx, markerIndexes)
	if err != nil {
		fmt.Printf("[info] marker index creation skipped: %v\n", err)
	}

	// Marker Options
	optIndexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "value", Value: 1}}},
		{Keys: bson.D{{Key: "text", Value: 1}}},
	}
	_, err = db.Collection(cfg.MarkerOptColl).Indexes().CreateMany(ctx, optIndexes)
	if err != nil {
		fmt.Printf("[info] marker_options index creation skipped: %v\n", err)
	}

	// Tag Options
	_, err = db.Collection(cfg.TagOptColl).Indexes().CreateMany(ctx, optIndexes)
	if err != nil {
		fmt.Printf("[info] tag_options index creation skipped: %v\n", err)
	}

	// Event Type Options
	_, err = db.Collection(cfg.EventTypeOptColl).Indexes().CreateMany(ctx, optIndexes)
	if err != nil {
		fmt.Printf("[info] event_type_options index creation skipped: %v\n", err)
	}

	// Option Ranges
	rangeIndexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "name", Value: 1}}},
		{Keys: bson.D{{Key: "timeRange.start", Value: 1}}},
		{Keys: bson.D{{Key: "timeRange.end", Value: 1}}},
	}
	_, err = db.Collection(cfg.MarkerOptRangesColl).Indexes().CreateMany(ctx, rangeIndexes)
	if err != nil {
		fmt.Printf("[info] marker_option_ranges index creation skipped: %v\n", err)
	}
	_, err = db.Collection(cfg.TagOptRangesColl).Indexes().CreateMany(ctx, rangeIndexes)
	if err != nil {
		fmt.Printf("[info] tag_option_ranges index creation skipped: %v\n", err)
	}
	_, err = db.Collection(cfg.EventTypeOptRangesColl).Indexes().CreateMany(ctx, rangeIndexes)
	if err != nil {
		fmt.Printf("[info] event_type_option_ranges index creation skipped: %v\n", err)
	}
}

/*
1. Insert Marker Document
The marker document is inserted into the markers collection.
2. Upsert Marker Option
A MarkerOption document is upserted into the marker_options collection.
This contains the marker’s name and its time range.
3. Insert Marker Option Range
A corresponding range document is inserted into the marker_option_ranges collection.
This stores the name and time range for the marker.
4. Upsert Tag Option(s)
For each tag in the marker:
A MarkerTagOptions document is upserted into the tag_options collection.
Contains the tag value, text, and time range.
5. Insert Tag Option Range(s)
For each tag:
A range document is inserted into the tag_option_ranges collection.
Stores the tag name and time range.
6. Upsert Event Type Option(s)
For each event in the marker:
A MarkerEventTypeOption document is upserted into the event_type_options collection.
Contains the event type, text, and time range.
7. Insert Event Type Option Range(s)
For each event:
A range document is inserted into the event_type_option_ranges collection.
Stores the event type name and time range.
*/
