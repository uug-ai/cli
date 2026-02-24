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
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	DefaultGroupCount = 100
	MaxGroupCount     = 100000
	MaxDeviceSample   = 10000
)

type SeedGroupsConfig struct {
	Count      int
	MongoURI   string
	DBName     string
	GroupsColl string
	DeviceColl string
	UserId     string
}

func (c *SeedGroupsConfig) ApplyDefaults() {
	if c.Count <= 0 {
		c.Count = DefaultGroupCount
	}
	if c.Count > MaxGroupCount {
		c.Count = MaxGroupCount
	}
	if c.MongoURI == "" {
		c.MongoURI = "mongodb://localhost:27017"
	}
	if c.DBName == "" {
		c.DBName = "Kerberos"
	}
	if c.GroupsColl == "" {
		c.GroupsColl = "groups"
	}
	if c.DeviceColl == "" {
		c.DeviceColl = "devices"
	}
}

func RunSeedGroups(cfg SeedGroupsConfig) error {
	HandleSignals()
	cfg.ApplyDefaults()

	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.MongoURI))
	if err != nil {
		fmt.Printf("[error] MongoDB connect failed: %v\n", err)
		return fmt.Errorf("connect mongo: %w", err)
	}
	defer client.Disconnect(ctx)

	groupsColl := client.Database(cfg.DBName).Collection(cfg.GroupsColl)
	deviceColl := client.Database(cfg.DBName).Collection(cfg.DeviceColl)

	var deviceKeys []string
	// Prefer the newer schema (`organisationId`) and fall back to legacy (`user_id`).
	deviceFilter := bson.M{"organisationId": cfg.UserId}
	cursor, err := deviceColl.Find(ctx, deviceFilter)
	if err != nil {
		fmt.Printf("[error] Device find failed (organisationId): %v\n", err)
		return fmt.Errorf("find devices by organisationId: %w", err)
	}
	devCount := 0
	for cursor.Next(ctx) {
		var device bson.M
		if err := cursor.Decode(&device); err == nil {
			if key, ok := device["key"].(string); ok && key != "" {
				deviceKeys = append(deviceKeys, key)
				devCount++
			} else {
				fmt.Printf("[warn] Device missing key field: %+v\n", device)
			}
		} else {
			fmt.Printf("[warn] Device decode failed: %v\n", err)
		}
	}
	cursor.Close(ctx)

	if len(deviceKeys) == 0 {
		deviceFilter = bson.M{"user_id": cfg.UserId}
		cursor, err = deviceColl.Find(ctx, deviceFilter)
		if err != nil {
			fmt.Printf("[error] Device find failed (user_id): %v\n", err)
			return fmt.Errorf("find devices by user_id: %w", err)
		}
		defer cursor.Close(ctx)

		for cursor.Next(ctx) {
			var device bson.M
			if err := cursor.Decode(&device); err == nil {
				if key, ok := device["key"].(string); ok && key != "" {
					deviceKeys = append(deviceKeys, key)
					devCount++
				} else {
					fmt.Printf("[warn] Device missing key field: %+v\n", device)
				}
			} else {
				fmt.Printf("[warn] Device decode failed: %v\n", err)
			}
		}
	}

	if len(deviceKeys) == 0 {
		fmt.Printf("[error] No devices found for user/organisation %s\n", cfg.UserId)
		return fmt.Errorf("no devices found for user/organisation %s", cfg.UserId)
	}

	var inserted int64
	uiprogress.Start()
	bar := uiprogress.AddBar(cfg.Count).AppendCompleted().PrependElapsed()
	bar.PrependFunc(func(b *uiprogress.Bar) string {
		cur := atomic.LoadInt64(&inserted)
		pct := 100 * float64(cur) / float64(cfg.Count)
		return fmt.Sprintf("Groups %d/%d (%.1f%%)", cur, cfg.Count, pct)
	})

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < cfg.Count; i++ {
		if atomic.LoadInt32(&stopFlag) != 0 {
			fmt.Printf("[debug] Stop flag detected, breaking loop\n")
			break
		}
		numDevices := rand.Intn(13) + 3
		devices := randomSample(deviceKeys, numDevices)
		group := bson.M{
			"_id":             primitive.NewObjectID(),
			"name":            fmt.Sprintf("group-%d", rand.Intn(1000000)),
			"description":     randomGroupDescription(),
			"organisationId":  cfg.UserId,
			"devices":         devices,
			"groups":          []string{},
			"storageSolution": randomStorageSolution(),
			"vaultAccessKey":  randomVaultKey(),
			"vaultSecretKey":  randomVaultKey(),
			"vaultUri":        fmt.Sprintf("vault://%d", rand.Intn(1000000)),
			"metadata":        nil,
			"audit":           nil,
		}
		_, err := groupsColl.InsertOne(ctx, group)
		if err != nil {
			fmt.Printf("[warn] Insert group %d failed: %v\n", i, err)
			continue
		}
		atomic.AddInt64(&inserted, 1)
		bar.Set(int(inserted))
	}

	uiprogress.Stop()
	fmt.Printf("[done] groups_inserted=%d stop=%v\n", inserted, atomic.LoadInt32(&stopFlag) != 0)

	if atomic.LoadInt32(&stopFlag) != 0 {
		return fmt.Errorf("interrupted")
	}
	return nil
}

// randomSample returns n random elements from src (without replacement)
func randomSample(src []string, n int) []string {
	if n >= len(src) {
		return append([]string{}, src...)
	}
	perm := rand.Perm(len(src))
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = src[perm[i]]
	}
	return out
}

func randomGroupDescription() string {
	descs := []string{
		"Main office group", "Security team", "Floor 2", "Parking area", "Lobby group",
		"Night shift", "Maintenance", "VIP area", "Conference zone", "Restricted access",
		"Staff only", "Public zone", "Emergency response", "Operations", "Tech support",
	}
	return descs[rand.Intn(len(descs))]
}

func randomStorageSolution() string {
	solutions := []string{"vault", "azure", "aws", "gcp"}
	return solutions[rand.Intn(len(solutions))]
}

func randomVaultKey() string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 20)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func SeedGroups(
	count int,
	mongodbURI string,
	dbName string,
	groupsCollName string,
	deviceCollName string,
	userId string,
) {
	HandleSignals()
	flag.Parse()

	// No prompts for db / collection names (consistent)
	if WasFlagPassed("db") {
		fmt.Printf("[info] using flag -db=%s\n", dbName)
	} else {
		fmt.Printf("[info] using default -db=%s (no flag)\n", dbName)
	}
	if WasFlagPassed("device-collection") {
		fmt.Printf("[info] using flag -device-collection=%s\n", deviceCollName)
	} else {
		fmt.Printf("[info] using default -device-collection=%s\n", deviceCollName)
	}
	if WasFlagPassed("groups-collection") {
		fmt.Printf("[info] using flag -groups-collection=%s\n", groupsCollName)
	} else {
		fmt.Printf("[info] using default -groups-collection=%s\n", groupsCollName)
	}

	// mongodb-uri
	if WasFlagPassed("mongodb-uri") {
		if mongodbURI == "" {
			mongodbURI = "mongodb://localhost:27017"
		}
		fmt.Printf("[info] using flag -mongodb-uri=%s\n", mongodbURI)
	} else {
		in := PromptString("MongoDB URI (-mongodb-uri, default mongodb://localhost:27017): ")
		if in == "" {
			mongodbURI = "mongodb://localhost:27017"
			fmt.Printf("[info] using default -mongodb-uri=%s\n", mongodbURI)
		} else {
			mongodbURI = in
			fmt.Printf("[info] using input -mongodb-uri=%s\n", mongodbURI)
		}
	}

	if WasFlagPassed("user-id") {
		if userId == "" {
			fmt.Println("[error] -user-id flag provided but empty")
			os.Exit(1)
		}
		fmt.Printf("[info] using flag -user-id=%s\n", userId)
	} else {
		in := PromptString("User ID to link groups to (-user-id): ")
		if in == "" {
			fmt.Println("[error] -user-id is required")
			os.Exit(1)
		} else {
			userId = in
			fmt.Printf("[info] using input -user-id=%s\n", userId)
		}
	}

	cfg := SeedGroupsConfig{
		Count:      count,
		MongoURI:   mongodbURI,
		DBName:     dbName,
		GroupsColl: groupsCollName,
		DeviceColl: deviceCollName,
		UserId:     userId,
	}
	if err := RunSeedGroups(cfg); err != nil {
		fmt.Printf("[error] seed groups: %v\n", err)
		os.Exit(1)
	}
}
