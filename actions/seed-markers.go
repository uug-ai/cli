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
	"github.com/uug-ai/markers/pkg/markers"
	"github.com/uug-ai/models/pkg/models"
	"github.com/uug-ai/trace/pkg/opentelemetry"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	DefaultMarkerTarget         = 1000000
	SmallMarkerTargetThreshold  = 10000
	MediumMarkerTargetThreshold = 1000000
	DefaultMarkerMediaFetchMax  = 10000

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
	GroupColl           string
	MediaColl           string
	MarkerColl          string
	MarkerOptColl       string
	TagOptColl          string
	EventOptColl        string
	MarkerOptRangesColl string
	CategoryOptColl     string
	TagOptRangesColl    string
	EventOptRangesColl  string
	ExistingUserIDHex   string
	LinkMedia           bool
	MediaFetchLimit     int
	DeviceCount         int
	Days                int
}

type markerInsertTask struct {
	Marker  models.Marker
	MediaID string
}

type mediaCandidate struct {
	ID             primitive.ObjectID `bson:"_id"`
	DeviceId       string             `bson:"deviceId"`
	DeviceKey      string             `bson:"deviceKey"`
	StartTimestamp int64              `bson:"startTimestamp"`
	EndTimestamp   int64              `bson:"endTimestamp"`
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
		c.DBName = "Kerberos_test"
	}
	if c.DeviceColl == "" {
		c.DeviceColl = "devices"
	}
	if c.GroupColl == "" {
		c.GroupColl = "groups"
	}
	if c.MediaColl == "" {
		c.MediaColl = "media"
	}
	if c.MarkerColl == "" {
		c.MarkerColl = "markers"
	}
	if c.MarkerOptColl == "" {
		c.MarkerOptColl = "marker_options"
	}
	if c.CategoryOptColl == "" {
		c.CategoryOptColl = "marker_category_options"
	}
	if c.TagOptColl == "" {
		c.TagOptColl = "marker_tag_options"
	}
	if c.EventOptColl == "" {
		c.EventOptColl = "marker_event_options"
	}
	if c.MarkerOptRangesColl == "" {
		c.MarkerOptRangesColl = "marker_option_ranges"
	}
	if c.TagOptRangesColl == "" {
		c.TagOptRangesColl = "marker_tag_option_ranges"
	}
	if c.EventOptRangesColl == "" {
		c.EventOptRangesColl = "marker_event_option_ranges"
	}
	if c.MediaFetchLimit <= 0 {
		c.MediaFetchLimit = DefaultMarkerMediaFetchMax
	}
	if c.MediaFetchLimit > DefaultMarkerMediaFetchMax {
		c.MediaFetchLimit = DefaultMarkerMediaFetchMax
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
	} else {
		return fmt.Errorf("no existing user id provided")
	}
	if organisationId == "" {
		organisationId = userObjectID.Hex()
	}

	// Align the marker package with user-provided database/collection names.
	markers.DatabaseName = cfg.DBName
	markers.MARKERS_COLLECTION = cfg.MarkerColl
	markers.MARKER_OPTIONS_COLLECTION = cfg.MarkerOptColl
	markers.MARKER_OPTION_RANGES_COLLECTION = cfg.MarkerOptRangesColl
	markers.MARKER_TAG_OPTIONS_COLLECTION = cfg.TagOptColl
	markers.MARKER_TAG_OPTION_RANGES_COLLECTION = cfg.TagOptRangesColl
	markers.MARKER_EVENT_OPTIONS_COLLECTION = cfg.EventOptColl
	markers.MARKER_EVENT_OPTION_RANGES_COLLECTION = cfg.EventOptRangesColl
	markers.MARKER_CATEGORY_OPTIONS_COLLECTION = cfg.CategoryOptColl

	markerTracer, err := opentelemetry.NewTracer("cli-seed-markers")
	if err != nil {
		return fmt.Errorf("create tracer: %w", err)
	}
	defer func() {
		_ = markerTracer.Shutdown(context.Background())
	}()

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
	groupCursor, err := db.Collection(cfg.GroupColl).Find(ctx, filter)
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

	var mediaItems []mediaCandidate
	if cfg.LinkMedia {
		mediaFilter := bson.M{"organisationId": organisationId}
		opts := options.Find().
			SetLimit(int64(cfg.MediaFetchLimit)).
			SetProjection(bson.M{
				"_id":            1,
				"deviceId":       1,
				"deviceKey":      1,
				"startTimestamp": 1,
				"endTimestamp":   1,
			})

		mediaCursor, err := db.Collection(cfg.MediaColl).Find(ctx, mediaFilter, opts)
		if err != nil {
			return fmt.Errorf("find media: %w", err)
		}
		defer mediaCursor.Close(ctx)
		for mediaCursor.Next(ctx) {
			var m mediaCandidate
			if err := mediaCursor.Decode(&m); err != nil {
				continue
			}
			if m.EndTimestamp <= m.StartTimestamp {
				continue
			}
			if m.DeviceKey == "" && m.DeviceId == "" {
				continue
			}
			mediaItems = append(mediaItems, m)
		}
		if len(mediaItems) == 0 {
			return fmt.Errorf("link-media enabled, but no media found for organisation")
		}
	}

	targetTotal := cfg.Target
	if cfg.LinkMedia && targetTotal > len(mediaItems) {
		fmt.Printf("[info] link-media enabled: limiting target from %d to %d (available media)\n", targetTotal, len(mediaItems))
		targetTotal = len(mediaItems)
	}

	var (
		totalInserted int64
		startTime     = time.Now()
		taskCh        = make(chan []markerInsertTask, cfg.Parallel*2)
		progressCh    = make(chan int, cfg.Parallel*2)
	)

	uiprogress.Start()
	bar := uiprogress.AddBar(targetTotal).AppendCompleted().PrependElapsed()
	bar.PrependFunc(func(b *uiprogress.Bar) string {
		inserted := atomic.LoadInt64(&totalInserted)
		pct := 100 * float64(inserted) / float64(targetTotal)
		el := time.Since(startTime).Seconds()
		rate := float64(inserted)
		if el > 0 {
			rate = float64(inserted) / el
		}
		return fmt.Sprintf("Inserted %d/%d (%.1f%%) %.0f/s", inserted, targetTotal, pct, rate)
	})

	// create a WaitGroup for worker goroutines
	var wg sync.WaitGroup
	markerService := markers.New()

	// spawn cfg.Parallel workers
	for w := 0; w < cfg.Parallel; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for batch := range taskCh {
				insertedInBatch := 0
				for _, task := range batch {
					if task.MediaID != "" {
						if _, err := markerService.Create(ctx, markerTracer, client, task.Marker, task.MediaID); err != nil {
							fmt.Printf("[warn] worker %d create marker failed: %v\n", workerID, err)
							continue
						}
					} else {
						if _, err := markerService.Create(ctx, markerTracer, client, task.Marker); err != nil {
							fmt.Printf("[warn] worker %d create marker failed: %v\n", workerID, err)
							continue
						}
					}
					insertedInBatch++
				}
				if insertedInBatch > 0 {
					total := atomic.AddInt64(&totalInserted, int64(insertedInBatch))
					progressCh <- int(total)
				}
			}
		}(w)
	}

	go func() {
		for total := range progressCh {
			if total > targetTotal {
				total = targetTotal
			}
			bar.Set(total)
		}
	}()

	var queued int64
	rand.Seed(time.Now().UnixNano())
	for atomic.LoadInt64(&queued) < int64(targetTotal) && atomic.LoadInt32(&stopFlag) == 0 {
		remaining := int64(targetTotal) - atomic.LoadInt64(&queued)
		current := cfg.BatchSize
		if remaining < int64(current) {
			current = int(remaining)
		}

		var batch []markerInsertTask
		if cfg.LinkMedia {
			batch = generateBatchMarkersFromMedia(current, organisationId, &mediaItems, groupIds)
		} else {
			base := generateBatchMarkers(current, cfg.Days, organisationId, deviceKeys, groupIds)
			batch = make([]markerInsertTask, 0, len(base))
			for _, marker := range base {
				batch = append(batch, markerInsertTask{Marker: marker})
			}
		}

		taskCh <- batch
		atomic.AddInt64(&queued, int64(current))
	}

	close(taskCh)
	wg.Wait()
	close(progressCh)

	finalTotal := int(atomic.LoadInt64(&totalInserted))
	if finalTotal > targetTotal {
		finalTotal = targetTotal
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

func generateBatchMarkersFromMedia(batchSize int, organisationId string, mediaItems *[]mediaCandidate, groupIds []string) []markerInsertTask {
	if batchSize > len(*mediaItems) {
		batchSize = len(*mediaItems)
	}

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

	categoryNames := []string{
		"security", "safety", "access control", "environmental", "maintenance",
	}

	groupCount := len(groupIds)
	batch := make([]markerInsertTask, 0, batchSize)
	for i := 0; i < batchSize; i++ {
		media := popRandomMedia(mediaItems)

		deviceId := media.DeviceKey
		if deviceId == "" {
			deviceId = media.DeviceId
		}

		start := media.StartTimestamp
		end := media.EndTimestamp
		if end <= start {
			end = start + 1
		}
		duration := end - start

		groupId := ""
		if groupCount > 0 && rand.Intn(2) == 0 {
			groupId = groupIds[rand.Intn(groupCount)]
		}

		numTags := rand.Intn(4) + 1
		tagIndexes := rand.Perm(len(tagNames))[:numTags]
		var tags []models.MarkerTag
		for _, idx := range tagIndexes {
			tags = append(tags, models.MarkerTag{Name: tagNames[idx]})
		}

		numCategories := rand.Intn(3) + 1
		categoryIndexes := rand.Perm(len(categoryNames))[:numCategories]
		var categories []models.MarkerCategory
		for _, idx := range categoryIndexes {
			categories = append(categories, models.MarkerCategory{Name: categoryNames[idx]})
		}

		numEvents := rand.Intn(3) + 1
		var events []models.MarkerEvent
		for j := 0; j < numEvents; j++ {
			name := eventNames[rand.Intn(len(eventNames))]
			evDur := int64(rand.Intn(int(max(1, int(duration/10)))) + 1)
			var evStart int64
			if end-start > evDur {
				evStart = start + int64(rand.Int63n((end-start)-evDur+1))
			} else {
				evStart = start
				evDur = max(1, end-start)
			}
			evEnd := evStart + evDur
			evStart += int64(rand.Intn(3))
			if evEnd > end {
				evEnd = end
			}
			events = append(events, models.MarkerEvent{
				StartTimestamp: evStart,
				EndTimestamp:   evEnd,
				Name:           name,
				Description:    "Random event description",
				Tags:           []string{"urgent"},
			})
		}

		marker := models.Marker{
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
			Categories:     categories,
			Description:    "Random marker description",
			Metadata:       &models.MarkerMetadata{Comments: nil},
			Synchronize:    nil,
			Audit:          nil,
			Tags:           tags,
		}
		batch = append(batch, markerInsertTask{
			Marker:  marker,
			MediaID: media.ID.Hex(),
		})
	}
	return batch
}

func popRandomMedia(mediaItems *[]mediaCandidate) mediaCandidate {
	last := len(*mediaItems) - 1
	pick := rand.Intn(len(*mediaItems))
	selected := (*mediaItems)[pick]
	(*mediaItems)[pick] = (*mediaItems)[last]
	*mediaItems = (*mediaItems)[:last]
	return selected
}

func generateBatchMarkers(batchSize int, days int, organisationId string, deviceKeys []string, groupIds []string) []models.Marker {
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

	categoryNames := []string{
		"security", "safety", "access control", "environmental", "maintenance",
	}

	deviceCount := len(deviceKeys)
	groupCount := len(groupIds)

	var batch []models.Marker
	now := time.Now().Unix()
	maxDays := days
	if maxDays <= 0 {
		maxDays = DefaultDays
	}
	if maxDays > MaxDays {
		maxDays = MaxDays
	}
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
		var tags []models.MarkerTag
		for _, idx := range tagIndexes {
			tags = append(tags, models.MarkerTag{Name: tagNames[idx]})
		}

		numCategories := rand.Intn(3) + 1
		categoryIndexes := rand.Perm(len(categoryNames))[:numCategories]
		var categories []models.MarkerCategory
		for _, idx := range categoryIndexes {
			categories = append(categories, models.MarkerCategory{Name: categoryNames[idx]})
		}

		numEvents := rand.Intn(3) + 1
		var events []models.MarkerEvent
		for j := 0; j < numEvents; j++ {
			name := eventNames[rand.Intn(len(eventNames))]
			// event duration: 1–10% of marker duration, at least 1s
			evDur := int64(rand.Intn(int(max(1, int(duration/10)))) + 1)
			// pick start within [start, end - evDur]
			var evStart int64
			if end-start > evDur {
				evStart = start + int64(rand.Int63n((end-start)-evDur+1))
			} else {
				// fallback: clamp to start
				evStart = start
				evDur = max(1, end-start)
			}
			evEnd := evStart + evDur
			// slight jitter for variety
			evStart += int64(rand.Intn(3))
			if evEnd > end {
				evEnd = end
			}
			events = append(events, models.MarkerEvent{
				StartTimestamp: evStart,
				EndTimestamp:   evEnd,
				Name:           name,
				Description:    "Random event description",
				Tags:           []string{"urgent"},
			})
		}

		marker := models.Marker{
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
			Categories:     categories,
			Description:    "Random marker description",
			Metadata:       &models.MarkerMetadata{Comments: nil},
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
	mediaColl string,
	markerColl string,
	markerOptColl string,
	tagOptColl string,
	eventOptColl string,
	markerOptRangesColl string,
	tagOptRangesColl string,
	eventOptRangesColl string,
	existingUserIDHex string,
	linkMedia bool,
	mediaFetchLimit int,
	deviceCount int,
	days int,
) {
	HandleSignals()
	flag.Parse()

	if WasFlagPassed("link-media") {
		fmt.Printf("[info] using flag -link-media=%v\n", linkMedia)
	} else {
		linkMedia = PromptBool("Link markers to existing media (-link-media, y/N): ", false)
		fmt.Printf("[info] using input -link-media=%v\n", linkMedia)
	}
	if linkMedia {
		if WasFlagPassed("media-fetch-limit") {
			fmt.Printf("[info] using flag -media-fetch-limit=%d\n", mediaFetchLimit)
		} else {
			val := PromptInt(fmt.Sprintf("Max media items to fetch (-media-fetch-limit, default %d, max %d): ", DefaultMarkerMediaFetchMax, DefaultMarkerMediaFetchMax))
			if val <= 0 {
				mediaFetchLimit = DefaultMarkerMediaFetchMax
			} else {
				if val > DefaultMarkerMediaFetchMax {
					val = DefaultMarkerMediaFetchMax
				}
				mediaFetchLimit = val
			}
			fmt.Printf("[info] using input -media-fetch-limit=%d\n", mediaFetchLimit)
		}
	}

	cfg := SeedMarkersConfig{
		Target:              target,
		BatchSize:           batchSize,
		Parallel:            parallel,
		MongoURI:            mongoURI,
		DBName:              dbName,
		DeviceColl:          deviceColl,
		GroupColl:           groupColl,
		MediaColl:           mediaColl,
		MarkerColl:          markerColl,
		MarkerOptColl:       markerOptColl,
		TagOptColl:          tagOptColl,
		EventOptColl:        eventOptColl,
		MarkerOptRangesColl: markerOptRangesColl,
		TagOptRangesColl:    tagOptRangesColl,
		EventOptRangesColl:  eventOptRangesColl,
		ExistingUserIDHex:   existingUserIDHex,
		LinkMedia:           linkMedia,
		MediaFetchLimit:     mediaFetchLimit,
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
