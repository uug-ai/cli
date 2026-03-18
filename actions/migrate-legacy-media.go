package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/uug-ai/cli/database"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type migrateLegacyMediaReport struct {
	Mode          string            `json:"mode"`
	Database      string            `json:"database"`
	Organisation  string            `json:"organisationId,omitempty"`
	Username      string            `json:"username,omitempty"`
	StartTS       int64             `json:"startTimestamp,omitempty"`
	EndTS         int64             `json:"endTimestamp,omitempty"`
	Scanned       int64             `json:"scanned"`
	MatchedFilter int64             `json:"matchedFilter"`
	Cases         map[string]int64  `json:"cases"`
	Needs         map[string]int64  `json:"needs"`
	UnknownShapes int64             `json:"unknownShapes"`
	Examples      map[string]string `json:"examples,omitempty"`
}

type analysisSnapshot struct {
	ID                 string
	Key                string
	HasThumby          bool
	HasSprite          bool
	HasDominantColor   bool
	ThumbyFilename     string
	ThumbyProvider     string
	SpriteFilename     string
	SpriteProvider     string
	SpriteInterval     int64
	DominantColorCount int64
}

func MigrateLegacyMedia(mode string,
	mongodbURI string,
	mongodbHost string,
	mongodbPort string,
	mongodbSourceDatabase string,
	mongodbDestinationDatabase string,
	mongodbDatabaseCredentials string,
	mongodbUsername string,
	mongodbPassword string,
	username string,
	organisationId string,
	startTimestamp int64,
	endTimestamp int64,
) {
	dbName := strings.TrimSpace(mongodbDestinationDatabase)
	if dbName == "" {
		dbName = strings.TrimSpace(mongodbSourceDatabase)
	}
	if dbName == "" {
		log.Println("Please provide a database name through -mongodb-destination-database or -mongodb-source-database")
		return
	}

	if mode == "" {
		mode = "dry-run"
	}

	if mode != "dry-run" {
		log.Printf("Action migrate-legacy-media currently supports dry-run only; got mode=%q\n", mode)
		return
	}

	var db *database.DB
	if mongodbURI != "" {
		db = database.NewMongoDBURI(mongodbURI)
	} else {
		db = database.NewMongoDBHost(mongodbHost, mongodbPort, mongodbDatabaseCredentials, mongodbUsername, mongodbPassword)
	}
	client := db.Client
	defer client.Disconnect(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	hubDB := client.Database(dbName)
	mediaCollection := hubDB.Collection("media")
	analysisCollection := hubDB.Collection("analysis")

	orgID := strings.TrimSpace(organisationId)
	if orgID == "" && strings.TrimSpace(username) != "" {
		var userDoc struct {
			ID any `bson:"_id"`
		}
		err := hubDB.Collection("users").FindOne(ctx, bson.M{"username": strings.TrimSpace(username)}).Decode(&userDoc)
		if err == nil {
			orgID = objectIDToHex(userDoc.ID)
		}
	}

	match := bson.M{}
	if orgID != "" {
		match["organisationId"] = orgID
	}

	if startTimestamp > 0 && endTimestamp > 0 && startTimestamp <= endTimestamp {
		// Cover both media-shaped and analysis-shaped timestamp fields.
		match["$or"] = []bson.M{
			{
				"$and": []bson.M{
					{"startTimestamp": bson.M{"$lte": endTimestamp}},
					{"endTimestamp": bson.M{"$gte": startTimestamp}},
				},
			},
			{"startTimestamp": bson.M{"$gte": startTimestamp, "$lte": endTimestamp}},
			{"timestamp": bson.M{"$gte": startTimestamp, "$lte": endTimestamp}},
			{
				"$and": []bson.M{
					{"start": bson.M{"$lte": endTimestamp}},
					{"end": bson.M{"$gte": startTimestamp}},
				},
			},
		}
	}

	report := migrateLegacyMediaReport{
		Mode:         mode,
		Database:     dbName,
		Organisation: orgID,
		Username:     strings.TrimSpace(username),
		StartTS:      startTimestamp,
		EndTS:        endTimestamp,
		Cases: map[string]int64{
			"new_shape_compliant":          0,
			"legacy_media_missing_fields":  0,
			"analysis_shaped_in_media":     0,
			"invalid_media_missing_key":    0,
			"candidate_insert_from_analysis": 0,
			"candidate_patch_existing":     0,
			"already_enriched_with_analysis": 0,
		},
		Needs: map[string]int64{
			"deviceKey":            0,
			"organisationId":       0,
			"startTimestamp":       0,
			"endTimestamp":         0,
			"duration":             0,
			"thumbnailFile":        0,
			"thumbnailProvider":    0,
			"spriteFile":           0,
			"spriteProvider":       0,
			"metadata.spriteInterval": 0,
			"metadata.analysisId":  0,
			"metadata.dominantColors": 0,
		},
		Examples: map[string]string{},
	}

	projection := bson.M{
		"_id": 1,
		"key": 1,
		"provider": 1,
		"source": 1,
		"timestamp": 1,
		"start": 1,
		"end": 1,
		"userid": 1,
		"user_id": 1,
		"deviceid": 1,
		"data": 1,
		"deviceId": 1,
		"deviceKey": 1,
		"organisationId": 1,
		"startTimestamp": 1,
		"endTimestamp": 1,
		"duration": 1,
		"videoFile": 1,
		"videoProvider": 1,
		"storageSolution": 1,
		"thumbnailFile": 1,
		"thumbnailProvider": 1,
		"spriteFile": 1,
		"spriteProvider": 1,
		"metadata": 1,
	}

	count, err := mediaCollection.CountDocuments(ctx, match)
	if err == nil {
		report.MatchedFilter = count
	}

	cursor, err := mediaCollection.Find(ctx, match, options.Find().SetProjection(projection))
	if err != nil {
		log.Printf("Error querying media collection: %v\n", err)
		return
	}
	defer cursor.Close(ctx)

	type mediaSnapshot struct {
		doc bson.M
	}

	items := make([]mediaSnapshot, 0, 4096)
	keysSet := make(map[string]struct{})

	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			log.Printf("Warning: failed to decode media doc: %v\n", err)
			continue
		}
		report.Scanned++
		items = append(items, mediaSnapshot{doc: doc})

		videoFile := asString(doc["videoFile"])
		if videoFile != "" {
			keysSet[videoFile] = struct{}{}
		}
		key := asString(doc["key"])
		if key != "" {
			keysSet[key] = struct{}{}
		}

		if report.Scanned%5000 == 0 {
			log.Printf("Scanned %d media docs...\n", report.Scanned)
		}
	}
	if err := cursor.Err(); err != nil {
		log.Printf("Cursor error while reading media: %v\n", err)
		return
	}

	analysisByKey := make(map[string]analysisSnapshot)
	if len(keysSet) > 0 {
		keys := make([]string, 0, len(keysSet))
		for k := range keysSet {
			keys = append(keys, k)
		}

		analysisMatch := bson.M{"key": bson.M{"$in": keys}}
		if orgID != "" {
			analysisMatch["$or"] = []bson.M{{"userid": orgID}, {"user_id": orgID}}
		}

		analysisProjection := bson.M{
			"_id": 1,
			"key": 1,
			"data.thumby.filename": 1,
			"data.thumby.provider": 1,
			"data.sprite.filename": 1,
			"data.sprite.provider": 1,
			"data.sprite.interval": 1,
			"data.dominantcolor.hexs": 1,
		}
		ac, err := analysisCollection.Find(ctx, analysisMatch, options.Find().SetProjection(analysisProjection))
		if err == nil {
			defer ac.Close(ctx)
			for ac.Next(ctx) {
				var doc bson.M
				if err := ac.Decode(&doc); err != nil {
					continue
				}
				key := asString(doc["key"])
				if key == "" {
					continue
				}
				dataMap := asMap(doc["data"])
				th := asMap(dataMap["thumby"])
				sp := asMap(dataMap["sprite"])
				dc := asMap(dataMap["dominantcolor"])
				hexs := asSlice(dc["hexs"])
				analysisByKey[key] = analysisSnapshot{
					ID:                 objectIDToHex(doc["_id"]),
					Key:                key,
					HasThumby:          asString(th["filename"]) != "",
					HasSprite:          asString(sp["filename"]) != "",
					HasDominantColor:   len(hexs) > 0,
					ThumbyFilename:     asString(th["filename"]),
					ThumbyProvider:     asString(th["provider"]),
					SpriteFilename:     asString(sp["filename"]),
					SpriteProvider:     asString(sp["provider"]),
					SpriteInterval:     asInt64(sp["interval"]),
					DominantColorCount: int64(len(hexs)),
				}
			}
		}
	}

	for _, item := range items {
		doc := item.doc

		videoFile := asString(doc["videoFile"])
		key := asString(doc["key"])
		organisation := asString(doc["organisationId"])
		deviceID := asString(doc["deviceId"])
		deviceKey := asString(doc["deviceKey"])
		startTS := asInt64(doc["startTimestamp"])
		endTS := asInt64(doc["endTimestamp"])
		duration := asInt64(doc["duration"])

		metadata := asMap(doc["metadata"])
		metaAnalysisID := asString(metadata["analysisId"])
		metaSpriteInterval := asInt64(metadata["spriteInterval"])
		metaDominantColorsLen := int64(len(asSlice(metadata["dominantColors"])))

		thumbnailFile := asString(doc["thumbnailFile"])
		thumbnailProvider := asString(doc["thumbnailProvider"])
		spriteFile := asString(doc["spriteFile"])
		spriteProvider := asString(doc["spriteProvider"])

		isAnalysisShaped := key != "" && videoFile == "" && asMap(doc["data"]) != nil
		if isAnalysisShaped {
			report.Cases["analysis_shaped_in_media"]++
			if _, ok := report.Examples["analysis_shaped_in_media"]; !ok {
				report.Examples["analysis_shaped_in_media"] = key
			}
			if key != "" {
				report.Cases["candidate_insert_from_analysis"]++
			}
			continue
		}

		if videoFile == "" {
			report.Cases["invalid_media_missing_key"]++
			report.UnknownShapes++
			if _, ok := report.Examples["invalid_media_missing_key"]; !ok {
				report.Examples["invalid_media_missing_key"] = objectIDToHex(doc["_id"])
			}
			continue
		}

		analysis := analysisByKey[videoFile]

		needPatch := false
		if organisation == "" {
			report.Needs["organisationId"]++
			needPatch = true
		}
		if deviceKey == "" && deviceID != "" {
			report.Needs["deviceKey"]++
			needPatch = true
		}
		if startTS <= 0 {
			report.Needs["startTimestamp"]++
			needPatch = true
		}
		if endTS <= 0 {
			report.Needs["endTimestamp"]++
			needPatch = true
		}
		if duration <= 0 {
			report.Needs["duration"]++
			needPatch = true
		}

		if thumbnailFile == "" && analysis.HasThumby {
			report.Needs["thumbnailFile"]++
			needPatch = true
		}
		if thumbnailProvider == "" && analysis.ThumbyProvider != "" {
			report.Needs["thumbnailProvider"]++
			needPatch = true
		}
		if spriteFile == "" && analysis.HasSprite {
			report.Needs["spriteFile"]++
			needPatch = true
		}
		if spriteProvider == "" && analysis.SpriteProvider != "" {
			report.Needs["spriteProvider"]++
			needPatch = true
		}
		if metaSpriteInterval <= 0 && analysis.SpriteInterval > 0 {
			report.Needs["metadata.spriteInterval"]++
			needPatch = true
		}
		if metaAnalysisID == "" && analysis.ID != "" {
			report.Needs["metadata.analysisId"]++
			needPatch = true
		}
		if metaDominantColorsLen == 0 && analysis.DominantColorCount > 0 {
			report.Needs["metadata.dominantColors"]++
			needPatch = true
		}

		isCompliant := organisation != "" && deviceKey != "" && startTS > 0 && endTS > 0 && duration > 0
		if isCompliant && !needPatch {
			report.Cases["new_shape_compliant"]++
		} else {
			report.Cases["legacy_media_missing_fields"]++
			report.Cases["candidate_patch_existing"]++
			if _, ok := report.Examples["legacy_media_missing_fields"]; !ok {
				report.Examples["legacy_media_missing_fields"] = videoFile
			}
		}

		if metaAnalysisID != "" && metaSpriteInterval > 0 {
			report.Cases["already_enriched_with_analysis"]++
		}
	}

	out, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		log.Printf("Failed to marshal migration report: %v\n", err)
		return
	}

	fmt.Println("====================================")
	fmt.Println("Legacy Media Migration Dry-Run Report")
	fmt.Println("====================================")
	fmt.Println(string(out))
}

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	default:
		return ""
	}
}

func asInt64(v any) int64 {
	switch t := v.(type) {
	case int:
		return int64(t)
	case int32:
		return int64(t)
	case int64:
		return t
	case float64:
		return int64(t)
	case float32:
		return int64(t)
	default:
		return 0
	}
}

func asMap(v any) bson.M {
	switch t := v.(type) {
	case bson.M:
		return t
	case map[string]any:
		m := bson.M{}
		for k, val := range t {
			m[k] = val
		}
		return m
	default:
		return nil
	}
}

func asSlice(v any) []any {
	switch t := v.(type) {
	case []any:
		return t
	case []string:
		res := make([]any, 0, len(t))
		for _, s := range t {
			res = append(res, s)
		}
		return res
	default:
		return nil
	}
}

func objectIDToHex(v any) string {
	if v == nil {
		return ""
	}
	switch id := v.(type) {
	case interface{ Hex() string }:
		return id.Hex()
	default:
		return ""
	}
}
