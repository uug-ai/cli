package actions

import (
	"context"
	"fmt"
	"time"

	"github.com/uug-ai/cli/database"
	"github.com/uug-ai/markers/pkg/markers"
	"github.com/uug-ai/models/pkg/models"
	"github.com/uug-ai/trace/pkg/opentelemetry"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	DefaultSeedDBUsers          = 1
	DefaultSeedDBDevicesPerUser = 50
	DefaultSeedDBGroupsPerUser  = 50
	DefaultSeedDBMediaPerDevice = 1000
	SeedDBMediaDurationSeconds  = 30
	SeedDBMediaSlotSeconds      = 120
)

type SeedDatabaseConfig struct {
	MongoURI            string
	DBName              string
	UserColl            string
	SubscriptionColl    string
	SettingsColl        string
	DeviceColl          string
	GroupsColl          string
	MediaColl           string
	MarkerColl          string
	MarkerOptColl       string
	CategoryOptColl     string
	TagOptColl          string
	EventOptColl        string
	MarkerOptRangesColl string
	TagOptRangesColl    string
	EventOptRangesColl  string

	Users          int
	DevicesPerUser int
	GroupsPerUser  int
	MediaPerDevice int
}

func (c *SeedDatabaseConfig) ApplyDefaults() {
	if c.MongoURI == "" {
		c.MongoURI = "mongodb://localhost:27017"
	}
	if c.DBName == "" {
		c.DBName = "Kerberos_test"
	}
	if c.UserColl == "" {
		c.UserColl = "users"
	}
	if c.SubscriptionColl == "" {
		c.SubscriptionColl = "subscriptions"
	}
	if c.SettingsColl == "" {
		c.SettingsColl = "settings"
	}
	if c.DeviceColl == "" {
		c.DeviceColl = "devices"
	}
	if c.GroupsColl == "" {
		c.GroupsColl = "groups"
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
	if c.Users <= 0 {
		c.Users = DefaultSeedDBUsers
	}
	if c.DevicesPerUser <= 0 {
		c.DevicesPerUser = DefaultSeedDBDevicesPerUser
	}
	if c.GroupsPerUser <= 0 {
		c.GroupsPerUser = DefaultSeedDBGroupsPerUser
	}
	if c.MediaPerDevice <= 0 {
		c.MediaPerDevice = DefaultSeedDBMediaPerDevice
	}
}

func deterministicObjectID(namespace string, idx int) primitive.ObjectID {
	hexInput := fmt.Sprintf("%s:%d", namespace, idx)
	sum := primitive.NewObjectIDFromTimestamp(time.Unix(1704067200+int64(idx%100000), 0))
	oidHex := fmt.Sprintf("%x", sum)
	prefix := fmt.Sprintf("%x", hexInput)
	for len(prefix) < 24 {
		prefix += prefix
	}
	finalHex := prefix[:12] + oidHex[12:]
	oid, err := primitive.ObjectIDFromHex(finalHex)
	if err != nil {
		return sum
	}
	return oid
}

func bulkUpsertByID(ctx context.Context, coll *mongo.Collection, docs []interface{}) error {
	if len(docs) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, 0, len(docs))
	for _, d := range docs {
		doc := bson.M{}
		switch typed := d.(type) {
		case bson.M:
			doc = typed
		default:
			raw, err := bson.Marshal(typed)
			if err != nil {
				return fmt.Errorf("marshal document in bulkUpsertByID (%T): %w", d, err)
			}
			if err := bson.Unmarshal(raw, &doc); err != nil {
				return fmt.Errorf("unmarshal document in bulkUpsertByID (%T): %w", d, err)
			}
		}
		id, ok := doc["_id"]
		if !ok {
			return fmt.Errorf("document missing _id in bulkUpsertByID")
		}
		model := mongo.NewReplaceOneModel().
			SetFilter(bson.M{"_id": id}).
			SetReplacement(doc).
			SetUpsert(true)
		models = append(models, model)
	}
	_, err := coll.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	return err
}

func SeedDatabase(mongoURI string, dbName string) {
	cfg := SeedDatabaseConfig{
		MongoURI: mongoURI,
		DBName:   dbName,
	}
	if err := RunSeedDatabase(cfg); err != nil {
		fmt.Printf("[error] seed database: %v\n", err)
		return
	}
}

func RunSeedDatabase(cfg SeedDatabaseConfig) error {
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

	if err := database.EnsureSettings(ctx, client, cfg.DBName, cfg.SettingsColl); err != nil {
		return fmt.Errorf("ensure settings: %w", err)
	}

	db := client.Database(cfg.DBName)
	database.CreateMediaIndexes(ctx, db.Collection(cfg.MediaColl))
	CreateMarkerIndexes(ctx, db, SeedMarkersConfig{
		MarkerColl:          cfg.MarkerColl,
		MarkerOptColl:       cfg.MarkerOptColl,
		CategoryOptColl:     cfg.CategoryOptColl,
		TagOptColl:          cfg.TagOptColl,
		EventOptColl:        cfg.EventOptColl,
		MarkerOptRangesColl: cfg.MarkerOptRangesColl,
		TagOptRangesColl:    cfg.TagOptRangesColl,
		EventOptRangesColl:  cfg.EventOptRangesColl,
	})

	type deviceRef struct {
		Key string
	}

	baseTime := time.Unix(1767225600, 0) // 2026-01-01T00:00:00Z

	users := make([]interface{}, 0, cfg.Users)
	subs := make([]interface{}, 0, cfg.Users)
	devices := make([]interface{}, 0, cfg.Users*cfg.DevicesPerUser)
	groups := make([]interface{}, 0, cfg.Users*cfg.GroupsPerUser)
	media := make([]interface{}, 0, cfg.Users*cfg.DevicesPerUser*cfg.MediaPerDevice)

	userDevices := make(map[string][]deviceRef, cfg.Users)

	mediaGlobalIdx := 0
	deviceGlobalIdx := 0
	groupGlobalIdx := 0

	for u := 0; u < cfg.Users; u++ {
		userID := deterministicObjectID("seeddb-user", u)
		userHex := userID.Hex()
		username := fmt.Sprintf("seeddb_user_%03d", u)
		createdAt := baseTime.Add(time.Duration(u) * time.Minute)

		days := make([]string, 7)
		for i := 0; i < 7; i++ {
			days[i] = baseTime.AddDate(0, 0, -i).Format("02-01-2006")
		}

		users = append(users, bson.M{
			"_id":                      userID,
			"username":                 username,
			"email":                    fmt.Sprintf("%s@example.com", username),
			"password":                 "seeddb-password-hash",
			"role":                     "owner",
			"google2fa_enabled":        false,
			"timezone":                 "UTC",
			"isActive":                 int64(1),
			"registerToken":            "",
			"updated_at":               createdAt,
			"created_at":               createdAt,
			"amazon_secret_access_key": fmt.Sprintf("SEEDDB_PRIVATE_%03d", u),
			"amazon_access_key_id":     fmt.Sprintf("SEEDDB_PUBLIC_%03d", u),
			"card_brand":               "Visa",
			"card_last_four":           "0000",
			"card_status":              "ok",
			"card_status_message":      nil,
			"days":                     days,
			"organisationId":           userHex,
		})

		subs = append(subs, bson.M{
			"_id":           deterministicObjectID("seeddb-sub", u),
			"name":          "default",
			"stripe_id":     fmt.Sprintf("seeddb_sub_%03d", u),
			"stripe_plan":   "enterprise",
			"quantity":      1,
			"trial_ends_at": nil,
			"ends_at":       nil,
			"user_id":       userHex,
			"updated_at":    createdAt,
			"created_at":    createdAt,
			"stripe_status": "active",
		})

		for d := 0; d < cfg.DevicesPerUser; d++ {
			key := fmt.Sprintf("seeddb-device-u%03d-d%03d", u, d)
			deviceID := deterministicObjectID("seeddb-device", deviceGlobalIdx)
			deviceGlobalIdx++
			userDevices[userHex] = append(userDevices[userHex], deviceRef{Key: key})
			nowMs := createdAt.UnixMilli()
			heartbeat := []models.DeprecatedHeartbeat{{
				CloudPublicKey: fmt.Sprintf("SEEDDB_PRIVATE_%03d", u),
				Key:            key,
				Timestamp:      createdAt.Unix(),
				Version:        "3.5.0",
				Release:        "seeddb",
				Encrypted:      false,
				EncryptedData:  []byte{},
				HubEncryption:  "true",
				E2EEncryption:  "false",
				Enterprise:     false,
				Hash:           "",
				MACs:           []string{fmt.Sprintf("02:42:ac:%02x:%02x:%02x", u%255, d%255, (u+d)%255)},
				IPs:            []string{"127.0.0.1/8", fmt.Sprintf("10.%d.%d.1/24", u%255, d%255)},
				CameraName:     fmt.Sprintf("seeddb-camera-%03d-%03d", u, d),
				CameraType:     "IPCamera",
				Architecture:   "x86_64",
				Hostname:       fmt.Sprintf("seeddb-host-%03d-%03d", u, d),
				FreeMemory:     "700000000",
				TotalMemory:    "16515977216",
				UsedMemory:     "15800000000",
				ProcessMemory:  "28000000",
				Kubernetes:     false,
				Docker:         true,
				Kios:           false,
				Raspberrypi:    false,
				Uptime:         "5 days",
				BootTime:       "5 days",
				ONVIF:          "false",
				ONVIFZoom:      "false",
				ONVIFPanTilt:   "false",
				ONVIFPresets:   "false",
				CameraConnected: []string{
					"true",
				}[0],
				HasBackChannel: "false",
			}}

			deviceDoc := models.Device{
				Id:                    deviceID,
				Key:                   key,
				Name:                  fmt.Sprintf("SeedDB Camera U%03d D%03d", u, d),
				Type:                  "camera",
				Repository:            "https://github.com/uug-ai/agent",
				Version:               "3.5.0",
				Deployment:            "docker",
				OrganisationId:        userHex,
				SiteIds:               []string{fmt.Sprintf("seeddb-site-%02d", u%10)},
				GroupIds:              []string{},
				UserId:                userHex,
				ConnectionStart:       nowMs,
				DeviceLastSeen:        nowMs,
				AgentLastSeen:         nowMs,
				IdleThreshold:         60000,
				DisconnectedThreshold: 300000,
				HealthyThreshold:      120000,
				Metadata: &models.DeviceMetadata{
					Architecture:  "x86_64",
					Hostname:      fmt.Sprintf("seeddb-host-%03d-%03d", u, d),
					FreeMemory:    "700000000",
					TotalMemory:   "16515977216",
					UsedMemory:    "15800000000",
					ProcessMemory: "28000000",
					Encrypted:     false,
					EncryptedData: []byte{},
					HubEncryption: "true",
					E2EEncryption: "false",
					IPAddress:     fmt.Sprintf("10.%d.%d.1", u%255, d%255),
					MacAddress:    fmt.Sprintf("02:42:ac:%02x:%02x:%02x", u%255, d%255, (u+d)%255),
					BootTime:      "5 days",
				},
				CameraMetadata: &models.CameraMetadata{
					Resolution:     "1920x1080",
					FrameRate:      15,
					Codec:          "H.264",
					HasOnvif:       false,
					HasAudio:       false,
					HasZoom:        false,
					HasBackChannel: false,
					HasPanTilt:     false,
					HasPresets:     false,
					HasIO:          false,
				},
				FeaturePermissions: &models.DeviceFeaturePermissions{
					PTZ:          models.PermissionNone,
					Liveview:     models.PermissionNone,
					RemoteConfig: models.PermissionNone,
					IO:           models.PermissionNone,
					FloorPlans:   models.PermissionNone,
				},
				AtRuntimeMetadata: &models.DeviceAtRuntimeMetadata{
					Status: "connected",
				},
				DeprecatedAnalytics: &heartbeat,
			}
			devices = append(devices, deviceDoc)
		}

		// Generate media in synchronized timeslots across all devices for this user.
		for m := 0; m < cfg.MediaPerDevice; m++ {
			start := baseTime.Unix() + int64(m*SeedDBMediaSlotSeconds)
			end := start + SeedDBMediaDurationSeconds
			for _, dev := range userDevices[userHex] {
				mediaID := deterministicObjectID("seeddb-media", mediaGlobalIdx)
				mediaGlobalIdx++
				media = append(media, bson.M{
					"_id":             mediaID,
					"startTimestamp":  start,
					"endTimestamp":    end,
					"duration":        int(end - start),
					"deviceId":        dev.Key,
					"deviceKey":       dev.Key,
					"groupId":         "",
					"organisationId":  userHex,
					"storageSolution": "kstorage",
					"videoProvider":   "azure-production",
					"videoFile":       fmt.Sprintf("seeddb/%s/%06d.mp4", dev.Key, m),
					"metadata": bson.M{
						"analysisId":     fmt.Sprintf("SEEDDB-AN-%09d", mediaGlobalIdx),
						"description":    "deterministic synthetic media",
						"detections":     []string{"person"},
						"dominantColors": []string{"blue"},
						"count":          1,
						"tags":           []string{"seeddb", "deterministic"},
						"classifications": []bson.M{
							{"key": "normal_activity", "centroids": []interface{}{}},
						},
					},
					"markerNames": []string{},
					"eventNames":  []string{},
					"tagNames":    []string{},
				})
			}
		}

		for g := 0; g < cfg.GroupsPerUser; g++ {
			groupID := deterministicObjectID("seeddb-group", groupGlobalIdx)
			groupGlobalIdx++
			var keys []string
			for i, dev := range userDevices[userHex] {
				if i%cfg.GroupsPerUser == g%cfg.GroupsPerUser {
					keys = append(keys, dev.Key)
				}
			}
			groups = append(groups, bson.M{
				"_id":             groupID,
				"name":            fmt.Sprintf("seeddb-group-u%03d-g%02d", u, g),
				"description":     "deterministic seeded group",
				"organisationId":  userHex,
				"devices":         keys,
				"groups":          []string{},
				"storageSolution": "vault",
				"vaultAccessKey":  fmt.Sprintf("seeddb-vault-access-%03d-%02d", u, g),
				"vaultSecretKey":  fmt.Sprintf("seeddb-vault-secret-%03d-%02d", u, g),
				"vaultUri":        "vault://seeddb",
			})
		}
	}

	fmt.Printf("[info] deterministic plan users=%d devices=%d groups=%d media=%d markers=%d\n",
		len(users), len(devices), len(groups), len(media), len(media))

	if err := bulkUpsertByID(ctx, db.Collection(cfg.UserColl), users); err != nil {
		return fmt.Errorf("upsert users: %w", err)
	}
	if err := bulkUpsertByID(ctx, db.Collection(cfg.SubscriptionColl), subs); err != nil {
		return fmt.Errorf("upsert subscriptions: %w", err)
	}
	if err := bulkUpsertByID(ctx, db.Collection(cfg.DeviceColl), devices); err != nil {
		return fmt.Errorf("upsert devices: %w", err)
	}
	if err := bulkUpsertByID(ctx, db.Collection(cfg.GroupsColl), groups); err != nil {
		return fmt.Errorf("upsert groups: %w", err)
	}
	if err := bulkUpsertByID(ctx, db.Collection(cfg.MediaColl), media); err != nil {
		return fmt.Errorf("upsert media: %w", err)
	}

	markers.DatabaseName = cfg.DBName
	markers.MARKERS_COLLECTION = cfg.MarkerColl
	markers.MARKER_OPTIONS_COLLECTION = cfg.MarkerOptColl
	markers.MARKER_OPTION_RANGES_COLLECTION = cfg.MarkerOptRangesColl
	markers.MARKER_TAG_OPTIONS_COLLECTION = cfg.TagOptColl
	markers.MARKER_TAG_OPTION_RANGES_COLLECTION = cfg.TagOptRangesColl
	markers.MARKER_EVENT_OPTIONS_COLLECTION = cfg.EventOptColl
	markers.MARKER_EVENT_OPTION_RANGES_COLLECTION = cfg.EventOptRangesColl
	markers.MARKER_CATEGORY_OPTIONS_COLLECTION = cfg.CategoryOptColl
	markers.MEDIA_COLLECTION = cfg.MediaColl

	markerTracer, err := opentelemetry.NewTracer("cli-seed-database")
	if err != nil {
		return fmt.Errorf("create tracer: %w", err)
	}
	defer func() {
		_ = markerTracer.Shutdown(context.Background())
	}()

	markerService := markers.New()
	insertedMarkers := 0
	for i, raw := range media {
		m := raw.(bson.M)
		mediaID := m["_id"].(primitive.ObjectID).Hex()
		userHex := m["organisationId"].(string)
		deviceID := m["deviceKey"].(string)
		start := m["startTimestamp"].(int64)
		end := m["endTimestamp"].(int64)

		markerStart := start + 1
		markerEnd := end - 1
		if markerEnd <= markerStart {
			markerEnd = markerStart + 1
		}

		markerName := fmt.Sprintf("seeddb-marker-%09d", i)
		tagName := fmt.Sprintf("seeddb-tag-%02d", i%8)
		eventName := fmt.Sprintf("seeddb-event-%02d", i%8)

		markerDoc := models.Marker{
			DeviceId:       deviceID,
			GroupId:        "",
			OrganisationId: userHex,
			StartTimestamp: markerStart,
			EndTimestamp:   markerEnd,
			Name:           markerName,
			Events: []models.MarkerEvent{{
				StartTimestamp: markerStart,
				EndTimestamp:   markerEnd,
				Name:           eventName,
				Description:    "deterministic event",
				Tags:           []string{"seeddb"},
			}},
			Categories:  []models.MarkerCategory{{Name: "security"}},
			Tags:        []models.MarkerTag{{Name: tagName}},
			Description: "deterministic marker",
			Metadata:    &models.MarkerMetadata{Comments: nil},
		}
		if _, err := markerService.Create(ctx, markerTracer, client, markerDoc, mediaID); err != nil {
			return fmt.Errorf("create marker via service (idx=%d): %w", i, err)
		}
		insertedMarkers++
	}

	fmt.Printf("[done] seed-database users=%d devices=%d groups=%d media=%d markers=%d\n",
		len(users), len(devices), len(groups), len(media), insertedMarkers)
	return nil
}
