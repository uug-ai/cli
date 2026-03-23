package actions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/uug-ai/cli/database"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type migrateLegacyMediaReport struct {
	Mode          string            `json:"mode"`
	Version       int               `json:"version"`
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
	Writes        map[string]int64  `json:"writes,omitempty"`
	MarkerOptions map[string]int64  `json:"markerOptions,omitempty"`
	Indexes       map[string]int64  `json:"indexes,omitempty"`
}

type analysisSnapshot struct {
	ID                 string
	Key                string
	HasThumby          bool
	HasSprite          bool
	Width              int64
	Height             int64
	HasDominantColor   bool
	ThumbyFilename     string
	ThumbyProvider     string
	SpriteFilename     string
	SpriteProvider     string
	SpriteInterval     int64
	DominantColorCount int64
	DominantColors     []string
	Classifications    []bson.M
	ClassificationSummary []bson.M
	MarkerNames        []string
	CountingSummary    []bson.M
	CountTotal         int64
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
	migrationTimeoutMinutes int,
	skipMatchedCount bool,
	migrationVersion int,
	checkMigrationIndexes bool,
	applyMigrationIndexes bool,
	generateDefaultMarkerOptions bool,
) {
	const latestMigrationVersion = 1
	dbName := strings.TrimSpace(mongodbDestinationDatabase)
	if dbName == "" {
		dbName = strings.TrimSpace(mongodbSourceDatabase)
	}
	if dbName == "" {
		log.Println("Please provide a database name through -mongodb-destination-database or -mongodb-source-database")
		return
	}
	if strings.HasPrefix(dbName, "-") {
		log.Printf("Invalid database name %q. This usually means a missing value after -mongodb-destination-database or -mongodb-source-database.\n", dbName)
		return
	}

	if mode == "" {
		mode = "dry-run"
	}

	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode != "dry-run" && mode != "live" {
		log.Printf("Action migrate-legacy-media expects mode dry-run or live; got mode=%q\n", mode)
		return
	}
	if migrationVersion <= 0 {
		migrationVersion = latestMigrationVersion
	}
	if migrationVersion > latestMigrationVersion {
		log.Printf("Unsupported migration version %d (latest supported is %d)\n", migrationVersion, latestMigrationVersion)
		return
	}
	liveMode := mode == "live"

	var db *database.DB
	if mongodbURI != "" {
		db = database.NewMongoDBURI(mongodbURI)
	} else {
		db = database.NewMongoDBHost(mongodbHost, mongodbPort, mongodbDatabaseCredentials, mongodbUsername, mongodbPassword)
	}
	client := db.Client
	defer client.Disconnect(context.Background())

	var ctx context.Context
	var cancel context.CancelFunc
	if migrationTimeoutMinutes <= 0 {
		ctx, cancel = context.WithCancel(context.Background())
		log.Println("Running migrate-legacy-media with no context timeout")
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(migrationTimeoutMinutes)*time.Minute)
	}
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
		Version:      migrationVersion,
		Database:     dbName,
		Organisation: orgID,
		Username:     strings.TrimSpace(username),
		StartTS:      startTimestamp,
		EndTS:        endTimestamp,
		Cases: map[string]int64{
			"new_shape_compliant":             0,
			"legacy_media_missing_fields":     0,
			"analysis_shaped_in_media":        0,
			"invalid_media_missing_key":       0,
			"missing_matching_analysis":       0,
			"candidate_insert_from_analysis": 0,
			"candidate_patch_existing":        0,
			"already_enriched_with_analysis": 0,
		},
		Needs: map[string]int64{
			"deviceKey":               0,
			"organisationId":          0,
			"startTimestamp":          0,
			"endTimestamp":            0,
			"duration":                0,
			"thumbnailFile":           0,
			"thumbnailProvider":       0,
			"spriteFile":              0,
			"spriteProvider":          0,
			"metadata.spriteInterval": 0,
			"metadata.analysisId":     0,
			"metadata.dominantColors": 0,
			"metadata.classifications": 0,
			"metadata.width":          0,
			"metadata.height":         0,
			"metadata.resolution":     0,
			"classificationSummary":   0,
			"markerNames":             0,
			"countingSummary":         0,
			"metadata.count":          0,
		},
		Examples: map[string]string{},
		Writes: map[string]int64{
			"patch_attempted":  0,
			"patch_applied":    0,
			"insert_attempted": 0,
			"insert_applied":   0,
			"errors":           0,
		},
		MarkerOptions: map[string]int64{
			"enabled":         0,
			"classification_count": 0,
			"users_scanned":   0,
			"users_targeted":  0,
			"options_attempted": 0,
			"options_upserted": 0,
			"options_modified": 0,
			"errors":          0,
			"skipped":         0,
		},
		Indexes: map[string]int64{
			"checked":  0,
			"existing": 0,
			"missing":  0,
			"created":  0,
			"errors":   0,
		},
	}
	if generateDefaultMarkerOptions {
		report.MarkerOptions["enabled"] = 1
	}
	if checkMigrationIndexes || applyMigrationIndexes {
		stats, examples := checkOrApplyLegacyMigrationIndexes(ctx, hubDB, applyMigrationIndexes)
		for k, v := range stats {
			report.Indexes[k] += v
		}
		for k, v := range examples {
			if _, ok := report.Examples[k]; !ok {
				report.Examples[k] = v
			}
		}
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
		"classificationSummary": 1,
		"countingSummary": 1,
		"markerNames": 1,
		"metadata": 1,
	}

	if !skipMatchedCount {
		count, err := mediaCollection.CountDocuments(ctx, match)
		if err == nil {
			report.MatchedFilter = count
		} else {
			log.Printf("Warning: CountDocuments failed: %v\n", err)
		}
	} else {
		report.MatchedFilter = -1
	}

	cursor, err := mediaCollection.Find(ctx, match, options.Find().SetProjection(projection).SetBatchSize(2000))
	if err != nil {
		log.Printf("Error querying media collection: %v\n", err)
		return
	}
	defer cursor.Close(ctx)

	const mediaBatchSize = 5000
	classificationOptionSet := make(map[string]string)
	batchItems := make([]bson.M, 0, mediaBatchSize)
	batchKeysSet := make(map[string]struct{}, mediaBatchSize)
	batchAnalysisCandidates := make([]any, 0, mediaBatchSize)

		flushBatch := func() {
		if len(batchItems) == 0 {
			return
		}
		analysisByKey := fetchAnalysisSnapshotsByKeys(ctx, analysisCollection, orgID, batchKeysSet)
		analysisShapedIDs := fetchAnalysisShapedIDsForBatch(ctx, mediaCollection, batchAnalysisCandidates)
		patchModels := make([]mongo.WriteModel, 0, len(batchItems))
		insertModels := make([]mongo.WriteModel, 0, len(batchItems))
		for _, doc := range batchItems {
			processLegacyMediaDocByVersion(migrationVersion, doc, analysisByKey, analysisShapedIDs, liveMode, orgID, &report, classificationOptionSet, &patchModels, &insertModels)
		}
		if liveMode {
			if len(insertModels) > 0 {
				result, err := mediaCollection.BulkWrite(ctx, insertModels, options.BulkWrite().SetOrdered(false))
				if err != nil {
					if result != nil {
						report.Writes["insert_applied"] += result.UpsertedCount
					}
					report.Writes["errors"] += bulkWriteErrorCount(err)
					log.Printf("Warning: insert bulk write error: %v\n", err)
				} else if result != nil {
					report.Writes["insert_applied"] += result.UpsertedCount
				}
			}
			if len(patchModels) > 0 {
				result, err := mediaCollection.BulkWrite(ctx, patchModels, options.BulkWrite().SetOrdered(false))
				if err != nil {
					if result != nil {
						report.Writes["patch_applied"] += result.ModifiedCount
					}
					report.Writes["errors"] += bulkWriteErrorCount(err)
					log.Printf("Warning: patch bulk write error: %v\n", err)
				} else if result != nil {
					report.Writes["patch_applied"] += result.ModifiedCount
				}
			}
		}
		batchItems = batchItems[:0]
		batchKeysSet = make(map[string]struct{}, mediaBatchSize)
		batchAnalysisCandidates = batchAnalysisCandidates[:0]
	}

	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			log.Printf("Warning: failed to decode media doc: %v\n", err)
			continue
		}
		report.Scanned++
		batchItems = append(batchItems, doc)

		videoFile := asString(doc["videoFile"])
		if videoFile != "" {
			batchKeysSet[videoFile] = struct{}{}
		}
		key := asString(doc["key"])
		if key != "" {
			batchKeysSet[key] = struct{}{}
		}
		if key != "" && videoFile == "" && doc["_id"] != nil {
			batchAnalysisCandidates = append(batchAnalysisCandidates, doc["_id"])
		}

		if report.Scanned%5000 == 0 {
			log.Printf("Scanned %d media docs...\n", report.Scanned)
		}
		if len(batchItems) >= mediaBatchSize {
			flushBatch()
		}
	}
	if err := cursor.Err(); err != nil {
		log.Printf("Cursor error while reading media: %v\n", err)
		return
	}
	flushBatch()

	if generateDefaultMarkerOptions {
		classificationNames := sortedClassificationNames(classificationOptionSet)
		stats := generateDefaultClassificationMarkerOptions(ctx, hubDB, orgID, classificationNames, liveMode)
		for k, v := range stats {
			report.MarkerOptions[k] += v
		}
	}

	out, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		log.Printf("Failed to marshal migration report: %v\n", err)
		return
	}

	fmt.Println("====================================")
	if liveMode {
		fmt.Println("Legacy Media Migration Live Report")
	} else {
		fmt.Println("Legacy Media Migration Dry-Run Report")
	}
	fmt.Println("====================================")
	fmt.Println(string(out))
}

func fetchAnalysisSnapshotsByKeys(
	ctx context.Context,
	analysisCollection *mongo.Collection,
	orgID string,
	keysSet map[string]struct{},
) map[string]analysisSnapshot {
	analysisByKey := make(map[string]analysisSnapshot)
	if len(keysSet) == 0 {
		return analysisByKey
	}

	keys := make([]string, 0, len(keysSet))
	for key := range keysSet {
		keys = append(keys, key)
	}

	analysisMatch := bson.M{"key": bson.M{"$in": keys}}
	if orgID != "" {
		analysisMatch["$or"] = []bson.M{{"userid": orgID}, {"user_id": orgID}}
	}

	analysisProjection := bson.M{
		"_id":                               1,
		"key":                               1,
		"data.thumby.filename":              1,
		"data.thumby.provider":              1,
		"data.sprite.filename":              1,
		"data.sprite.provider":              1,
		"data.sprite.interval":              1,
		"data.dominantcolor.hexs":           1,
		"data.classify.details.classified":  1,
		"data.classify.details.trajectCentroids": 1,
		"data.classify.details.traject":          1,
		"data.classify.details.x":                1,
		"data.classify.details.y":                1,
		"data.classify.details.frameWidth":       1,
		"data.classify.details.frameHeight":      1,
		"data.classify.properties":               1,
		"data.details.classified":                1,
		"data.details.objectName":                1,
		"data.details.trajectCentroids":          1,
		"data.details.traject":                   1,
		"data.details.x":                         1,
		"data.details.y":                         1,
		"data.details.frameWidth":                1,
		"data.details.frameHeight":               1,
		"data.properties":                        1,
		"data.counting.detail.videoWidth":        1,
		"data.counting.detail.videoHeight":       1,
		"data.thumby.width":                      1,
		"data.thumby.height":                     1,
		"data.sprite.width":                      1,
		"data.sprite.height":                     1,
		"data.counting.records.type":             1,
		"data.counting.records.count":            1,
		"data.counting.records.duration":         1,
	}

	cursor, err := analysisCollection.Find(ctx, analysisMatch, options.Find().SetProjection(analysisProjection).SetBatchSize(2000))
	if err != nil {
		log.Printf("Warning: failed querying analysis batch: %v\n", err)
		return analysisByKey
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
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
		classifications := deriveClassificationsFromAnalysis(dataMap)
		classificationSummary := deriveClassificationSummary(classifications)
		markerNames := markerNamesFromSummary(classificationSummary)
		countingSummary, countTotal := deriveCountingSummaryFromAnalysis(dataMap)
		width, height := deriveMediaDimensionsFromAnalysis(dataMap)
		analysisByKey[key] = analysisSnapshot{
			ID:                    objectIDToHex(doc["_id"]),
			Key:                   key,
			HasThumby:             asString(th["filename"]) != "",
			HasSprite:             asString(sp["filename"]) != "",
			Width:                 width,
			Height:                height,
			HasDominantColor:      len(hexs) > 0,
			ThumbyFilename:        asString(th["filename"]),
			ThumbyProvider:        asString(th["provider"]),
			SpriteFilename:        asString(sp["filename"]),
			SpriteProvider:        asString(sp["provider"]),
			SpriteInterval:        asInt64(sp["interval"]),
			DominantColorCount:    int64(len(hexs)),
			DominantColors:        toStringSlice(hexs),
			Classifications:       classifications,
			ClassificationSummary: classificationSummary,
			MarkerNames:           markerNames,
			CountingSummary:       countingSummary,
			CountTotal:            countTotal,
		}
	}
	if err := cursor.Err(); err != nil {
		log.Printf("Warning: analysis batch cursor error: %v\n", err)
	}
	return analysisByKey
}

func fetchAnalysisShapedIDsForBatch(
	ctx context.Context,
	mediaCollection *mongo.Collection,
	candidateIDs []any,
) map[string]struct{} {
	shaped := make(map[string]struct{}, len(candidateIDs))
	if len(candidateIDs) == 0 {
		return shaped
	}

	filter := bson.M{
		"_id":  bson.M{"$in": candidateIDs},
		"data": bson.M{"$exists": true},
	}
	projection := bson.M{"_id": 1, "data": 1}
	cursor, err := mediaCollection.Find(ctx, filter, options.Find().SetProjection(projection).SetBatchSize(1000))
	if err != nil {
		log.Printf("Warning: failed querying analysis-shaped candidates: %v\n", err)
		return shaped
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		if asMap(doc["data"]) == nil {
			continue
		}
		id := objectIDToHex(doc["_id"])
		if id == "" {
			continue
		}
		shaped[id] = struct{}{}
	}
	if err := cursor.Err(); err != nil {
		log.Printf("Warning: analysis-shaped candidate cursor error: %v\n", err)
	}
	return shaped
}

func processLegacyMediaDoc(
	doc bson.M,
	analysisByKey map[string]analysisSnapshot,
	analysisShapedIDs map[string]struct{},
	liveMode bool,
	orgID string,
	report *migrateLegacyMediaReport,
	classificationOptionSet map[string]string,
	patchModels *[]mongo.WriteModel,
	insertModels *[]mongo.WriteModel,
) {
	videoFile := asString(doc["videoFile"])
	key := asString(doc["key"])
	organisation := asString(doc["organisationId"])
	deviceID := asString(doc["deviceId"])
	deviceKey := asString(doc["deviceKey"])
	startTS := asInt64(doc["startTimestamp"])
	endTS := asInt64(doc["endTimestamp"])
	duration := asInt64(doc["duration"])

	metadata := asMap(doc["metadata"])
	collectClassificationOptionNames(classificationOptionSet, asSlice(doc["classificationSummary"]))
	collectClassificationOptionNames(classificationOptionSet, asSlice(metadata["classifications"]))
	metaAnalysisID := asString(metadata["analysisId"])
	metaSpriteInterval := asInt64(metadata["spriteInterval"])
	metaDominantColorsLen := int64(len(asSlice(metadata["dominantColors"])))
	metaClassifications := asSlice(metadata["classifications"])
	metaClassificationsLen := int64(len(metaClassifications))
	metaWidth := asInt64(metadata["width"])
	metaHeight := asInt64(metadata["height"])
	metaResolution := asString(metadata["resolution"])
	metaCount := asInt64(metadata["count"])

	thumbnailFile := asString(doc["thumbnailFile"])
	thumbnailProvider := asString(doc["thumbnailProvider"])
	spriteFile := asString(doc["spriteFile"])
	spriteProvider := asString(doc["spriteProvider"])
	classificationSummaryLen := int64(len(asSlice(doc["classificationSummary"])))
	markerNamesLen := int64(len(asSlice(doc["markerNames"])))
	countingSummaryLen := int64(len(asSlice(doc["countingSummary"])))

	_, isAnalysisShaped := analysisShapedIDs[objectIDToHex(doc["_id"])]
	isAnalysisShaped = key != "" && videoFile == "" && isAnalysisShaped
	if isAnalysisShaped {
		report.Cases["analysis_shaped_in_media"]++
		if _, ok := report.Examples["analysis_shaped_in_media"]; !ok {
			report.Examples["analysis_shaped_in_media"] = key
		}
		if key != "" {
			report.Cases["candidate_insert_from_analysis"]++
			if liveMode {
				report.Writes["insert_attempted"]++
				insertOrganisation := resolveOrganisationForDoc(doc, orgID)
				insertStart, insertEnd, insertDuration := deriveTimesFromAnalysisShapedDoc(doc)
				insertDevice := asString(doc["deviceid"])
				insertSetOnInsert := bson.M{
					"organisationId":  insertOrganisation,
					"videoFile":       key,
					"storageSolution": asString(doc["provider"]),
					"videoProvider":   asString(doc["source"]),
					"deviceId":        insertDevice,
					"deviceKey":       insertDevice,
					"startTimestamp":  insertStart,
					"endTimestamp":    insertEnd,
					"duration":        insertDuration,
				}

				if analysis, ok := analysisByKey[key]; ok {
					if analysis.ThumbyFilename != "" {
						insertSetOnInsert["thumbnailFile"] = analysis.ThumbyFilename
					}
					if analysis.ThumbyProvider != "" {
						insertSetOnInsert["thumbnailProvider"] = analysis.ThumbyProvider
					}
					if analysis.SpriteFilename != "" {
						insertSetOnInsert["spriteFile"] = analysis.SpriteFilename
					}
					if analysis.SpriteProvider != "" {
						insertSetOnInsert["spriteProvider"] = analysis.SpriteProvider
					}

					metadataFields := bson.M{}
					if analysis.ID != "" {
						metadataFields["analysisId"] = analysis.ID
					}
					if analysis.SpriteInterval > 0 {
						metadataFields["spriteInterval"] = analysis.SpriteInterval
					}
					if len(analysis.DominantColors) > 0 {
						metadataFields["dominantColors"] = analysis.DominantColors
					}
					if len(analysis.Classifications) > 0 {
						metadataFields["classifications"] = analysis.Classifications
					}
					if analysis.Width > 0 {
						metadataFields["width"] = analysis.Width
					}
					if analysis.Height > 0 {
						metadataFields["height"] = analysis.Height
					}
					if analysis.Width > 0 && analysis.Height > 0 {
						metadataFields["resolution"] = fmt.Sprintf("%dx%d", analysis.Width, analysis.Height)
					}
					if analysis.CountTotal != 0 {
						metadataFields["count"] = analysis.CountTotal
					}
					if len(metadataFields) > 0 {
						insertSetOnInsert["metadata"] = metadataFields
					}
					if len(analysis.ClassificationSummary) > 0 {
						insertSetOnInsert["classificationSummary"] = analysis.ClassificationSummary
					}
					if len(analysis.MarkerNames) > 0 {
						insertSetOnInsert["markerNames"] = analysis.MarkerNames
					}
					if len(analysis.CountingSummary) > 0 {
						insertSetOnInsert["countingSummary"] = analysis.CountingSummary
					}
				}

				filter := bson.M{"videoFile": key}
				if insertOrganisation != "" {
					filter["organisationId"] = insertOrganisation
				}
				model := mongo.NewUpdateOneModel().
					SetFilter(filter).
					SetUpdate(bson.M{"$setOnInsert": insertSetOnInsert}).
					SetUpsert(true)
				*insertModels = append(*insertModels, model)
			}
		}
		return
	}

	if videoFile == "" {
		report.Cases["invalid_media_missing_key"]++
		report.UnknownShapes++
		if _, ok := report.Examples["invalid_media_missing_key"]; !ok {
			report.Examples["invalid_media_missing_key"] = objectIDToHex(doc["_id"])
		}
		return
	}

	analysis, hasAnalysis := analysisByKey[videoFile]
	if !hasAnalysis {
		report.Cases["missing_matching_analysis"]++
		if _, ok := report.Examples["missing_matching_analysis"]; !ok {
			report.Examples["missing_matching_analysis"] = videoFile
		}
	}
	collectClassificationOptionNames(classificationOptionSet, toAnySliceFromBsonMaps(analysis.ClassificationSummary))
	collectClassificationOptionNames(classificationOptionSet, toAnySliceFromBsonMaps(analysis.Classifications))

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
	if (metaClassificationsLen == 0 || !hasAnyNonEmptyCentroids(metaClassifications) || hasCentroidsOutsideNormalizedRange(metaClassifications)) && len(analysis.Classifications) > 0 {
		report.Needs["metadata.classifications"]++
		needPatch = true
	}
	if metaWidth <= 0 && analysis.Width > 0 {
		report.Needs["metadata.width"]++
		needPatch = true
	}
	if metaHeight <= 0 && analysis.Height > 0 {
		report.Needs["metadata.height"]++
		needPatch = true
	}
	if metaResolution == "" && analysis.Width > 0 && analysis.Height > 0 {
		report.Needs["metadata.resolution"]++
		needPatch = true
	}
	if classificationSummaryLen == 0 && len(analysis.ClassificationSummary) > 0 {
		report.Needs["classificationSummary"]++
		needPatch = true
	}
	if markerNamesLen == 0 && len(analysis.MarkerNames) > 0 {
		report.Needs["markerNames"]++
		needPatch = true
	}
	if countingSummaryLen == 0 && len(analysis.CountingSummary) > 0 {
		report.Needs["countingSummary"]++
		needPatch = true
	}
	if metaCount == 0 && analysis.CountTotal != 0 {
		report.Needs["metadata.count"]++
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
		if liveMode && needPatch {
			setFields := buildPatchSetFields(doc, analysis, orgID)
			if len(setFields) > 0 {
				report.Writes["patch_attempted"]++
				model := mongo.NewUpdateOneModel().
					SetFilter(bson.M{"_id": doc["_id"]}).
					SetUpdate(bson.M{"$set": setFields})
				*patchModels = append(*patchModels, model)
			}
		}
	}

	if metaAnalysisID != "" && metaSpriteInterval > 0 {
		report.Cases["already_enriched_with_analysis"]++
	}
}

func processLegacyMediaDocByVersion(
	version int,
	doc bson.M,
	analysisByKey map[string]analysisSnapshot,
	analysisShapedIDs map[string]struct{},
	liveMode bool,
	orgID string,
	report *migrateLegacyMediaReport,
	classificationOptionSet map[string]string,
	patchModels *[]mongo.WriteModel,
	insertModels *[]mongo.WriteModel,
) {
	switch version {
	case 1:
		processLegacyMediaDoc(doc, analysisByKey, analysisShapedIDs, liveMode, orgID, report, classificationOptionSet, patchModels, insertModels)
	default:
		// Guardrail for future versions: migration version is validated at action entry.
		processLegacyMediaDoc(doc, analysisByKey, analysisShapedIDs, liveMode, orgID, report, classificationOptionSet, patchModels, insertModels)
	}
}

func bulkWriteErrorCount(err error) int64 {
	if err == nil {
		return 0
	}
	var bwe mongo.BulkWriteException
	if errors.As(err, &bwe) && len(bwe.WriteErrors) > 0 {
		return int64(len(bwe.WriteErrors))
	}
	return 1
}

func checkOrApplyLegacyMigrationIndexes(
	ctx context.Context,
	hubDB *mongo.Database,
	apply bool,
) (map[string]int64, map[string]string) {
	stats := map[string]int64{
		"checked":  0,
		"existing": 0,
		"missing":  0,
		"created":  0,
		"errors":   0,
	}
	examples := map[string]string{}

	type indexSpec struct {
		collection string
		name       string
		model      mongo.IndexModel
	}

	specs := []indexSpec{
		{
			collection: "media",
			name:       "organisationId_1_startTimestamp_1_endTimestamp_1",
			model: mongo.IndexModel{
				Keys:    bson.D{{Key: "organisationId", Value: 1}, {Key: "startTimestamp", Value: 1}, {Key: "endTimestamp", Value: 1}},
				Options: options.Index().SetName("organisationId_1_startTimestamp_1_endTimestamp_1"),
			},
		},
		{
			collection: "analysis",
			name:       "key_1_userid_1",
			model: mongo.IndexModel{
				Keys:    bson.D{{Key: "key", Value: 1}, {Key: "userid", Value: 1}},
				Options: options.Index().SetName("key_1_userid_1"),
			},
		},
		{
			collection: "analysis",
			name:       "key_1_user_id_1",
			model: mongo.IndexModel{
				Keys:    bson.D{{Key: "key", Value: 1}, {Key: "user_id", Value: 1}},
				Options: options.Index().SetName("key_1_user_id_1"),
			},
		},
		{
			collection: "marker_options",
			name:       "organisationId_1_value_1",
			model: mongo.IndexModel{
				Keys:    bson.D{{Key: "organisationId", Value: 1}, {Key: "value", Value: 1}},
				Options: options.Index().SetName("organisationId_1_value_1").SetUnique(true),
			},
		},
		{
			collection: "marker_options",
			name:       "organisationId_1_updatedAt_-1__id_-1",
			model: mongo.IndexModel{
				Keys:    bson.D{{Key: "organisationId", Value: 1}, {Key: "updatedAt", Value: -1}, {Key: "_id", Value: -1}},
				Options: options.Index().SetName("organisationId_1_updatedAt_-1__id_-1"),
			},
		},
	}

	cache := make(map[string]map[string]struct{})
	for _, spec := range specs {
		stats["checked"]++
		names, ok := cache[spec.collection]
		if !ok {
			indexNames, err := listCollectionIndexNames(ctx, hubDB.Collection(spec.collection))
			if err != nil {
				stats["errors"]++
				if _, exists := examples["index_list_error"]; !exists {
					examples["index_list_error"] = spec.collection
				}
				continue
			}
			names = indexNames
			cache[spec.collection] = names
		}

		if _, exists := names[spec.name]; exists {
			stats["existing"]++
			continue
		}
		stats["missing"]++
		if !apply {
			continue
		}

		_, err := hubDB.Collection(spec.collection).Indexes().CreateOne(ctx, spec.model)
		if err != nil {
			stats["errors"]++
			if _, exists := examples["index_create_error"]; !exists {
				examples["index_create_error"] = spec.collection + ":" + spec.name
			}
			continue
		}
		stats["created"]++
		names[spec.name] = struct{}{}
	}

	return stats, examples
}

func listCollectionIndexNames(ctx context.Context, coll *mongo.Collection) (map[string]struct{}, error) {
	names := map[string]struct{}{}
	cursor, err := coll.Indexes().List(ctx)
	if err != nil {
		return names, err
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var idx bson.M
		if err := cursor.Decode(&idx); err != nil {
			continue
		}
		name := asString(idx["name"])
		if name != "" {
			names[name] = struct{}{}
		}
	}
	if err := cursor.Err(); err != nil {
		return names, err
	}
	return names, nil
}

func generateDefaultClassificationMarkerOptions(
	ctx context.Context,
	hubDB *mongo.Database,
	orgID string,
	classificationNames []string,
	liveMode bool,
) map[string]int64 {
	allClassificationOptions := mergeClassificationOptions(classificationNames)
	stats := map[string]int64{
		"classification_count": int64(len(allClassificationOptions)),
		"users_scanned":        0,
		"users_targeted":       0,
		"options_attempted":    0,
		"options_upserted":     0,
		"options_modified":     0,
		"category_options_attempted": 0,
		"category_options_upserted":  0,
		"category_options_modified":  0,
		"errors":               0,
	}

	targetUserIDs := make([]string, 0, 1024)
	seenUserIDs := make(map[string]struct{})
	if strings.TrimSpace(orgID) != "" {
		resolved := strings.TrimSpace(orgID)
		targetUserIDs = append(targetUserIDs, resolved)
		seenUserIDs[resolved] = struct{}{}
		stats["users_scanned"] = 1
		stats["users_targeted"] = 1
	} else {
		usersColl := hubDB.Collection("users")
		userProjection := bson.M{"_id": 1}
		cursor, err := usersColl.Find(ctx, bson.M{}, options.Find().SetProjection(userProjection))
		if err != nil {
			log.Printf("Failed to query users for marker_options generation: %v\n", err)
			stats["errors"]++
			return stats
		}
		defer cursor.Close(ctx)

		for cursor.Next(ctx) {
			var user bson.M
			if err := cursor.Decode(&user); err != nil {
				stats["errors"]++
				continue
			}
			stats["users_scanned"]++

			userID := objectIDToHex(user["_id"])
			if userID == "" {
				continue
			}
			if _, exists := seenUserIDs[userID]; exists {
				continue
			}
			seenUserIDs[userID] = struct{}{}
			targetUserIDs = append(targetUserIDs, userID)
		}
		if err := cursor.Err(); err != nil {
			log.Printf("Cursor error while scanning users for marker_options generation: %v\n", err)
			stats["errors"]++
		}
		stats["users_targeted"] = int64(len(targetUserIDs))
	}

	markerOptionsColl := hubDB.Collection("marker_options")
	markerCategoryOptionsColl := hubDB.Collection("marker_category_options")
	if !liveMode {
		stats["options_attempted"] = int64(len(allClassificationOptions) * len(targetUserIDs))
		stats["category_options_attempted"] = int64(len(targetUserIDs))
		return stats
	}

	now := time.Now().Unix()
	for _, userID := range targetUserIDs {
		categoryFilter := bson.M{
			"organisationId": userID,
			"value":          "classification",
		}
		categoryUpdate := bson.M{
			"$setOnInsert": bson.M{
				"organisationId": userID,
				"text":           "classification",
				"value":          "classification",
				"createdAt":      now,
				"updatedAt":      now,
			},
		}
		stats["category_options_attempted"]++
		categoryResult, categoryErr := markerCategoryOptionsColl.UpdateOne(ctx, categoryFilter, categoryUpdate, options.Update().SetUpsert(true))
		if categoryErr != nil {
			stats["errors"]++
			continue
		}
		if categoryResult.UpsertedCount > 0 {
			stats["category_options_upserted"]++
		} else if categoryResult.ModifiedCount > 0 {
			stats["category_options_modified"]++
		}

		for _, option := range allClassificationOptions {
			filter := bson.M{
				"organisationId": userID,
				"value":          option.Value,
			}
			update := bson.M{
				"$setOnInsert": bson.M{
					"organisationId": userID,
					"text":           option.Text,
					"value":          option.Value,
					"createdAt":      now,
					"updatedAt":      now,
				},
				"$addToSet": bson.M{
					"categories": "classification",
				},
			}

			stats["options_attempted"]++
			result, err := markerOptionsColl.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
			if err != nil {
				stats["errors"]++
				continue
			}
			if result.UpsertedCount > 0 {
				stats["options_upserted"]++
			} else if result.ModifiedCount > 0 {
				stats["options_modified"]++
			}
		}
	}

	return stats
}

func collectClassificationOptionNames(target map[string]string, rawEntries []any) {
	for _, raw := range rawEntries {
		entry := asMap(raw)
		if entry == nil {
			continue
		}
		name := asString(entry["key"])
		if name == "" {
			name = asString(entry["name"])
		}
		if name == "" {
			continue
		}
		normalized := strings.ToLower(strings.TrimSpace(name))
		if normalized == "" {
			continue
		}
		if _, exists := target[normalized]; !exists {
			target[normalized] = name
		}
	}
}

func sortedClassificationNames(entries map[string]string) []string {
	names := make([]string, 0, len(entries))
	for _, name := range entries {
		if strings.TrimSpace(name) != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

type markerClassificationOption struct {
	Value string
	Text  string
}

func mergeClassificationOptions(discovered []string) []markerClassificationOption {
	defaults := []markerClassificationOption{
		{Value: "animal", Text: "animal"},
		{Value: "pedestrian", Text: "pedestrian"},
		{Value: "cyclist", Text: "cyclist"},
		{Value: "motorbike", Text: "motorbike"},
		{Value: "lorry", Text: "lorry"},
		{Value: "car", Text: "car"},
		{Value: "handbag", Text: "handbag"},
		{Value: "suitecase", Text: "suitecase"},
		{Value: "cell phone", Text: "cell phone"},
		{Value: "backpack", Text: "backpack"},
	}

	merged := make([]markerClassificationOption, 0, len(defaults)+len(discovered))
	seen := make(map[string]struct{}, len(defaults)+len(discovered))

	for _, option := range defaults {
		normalized := strings.ToLower(strings.TrimSpace(option.Value))
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		merged = append(merged, option)
	}

	for _, name := range discovered {
		value := strings.TrimSpace(name)
		if value == "" {
			continue
		}
		normalized := strings.ToLower(value)
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		merged = append(merged, markerClassificationOption{
			Value: value,
			Text:  value,
		})
	}

	return merged
}

func toAnySliceFromBsonMaps(items []bson.M) []any {
	if len(items) == 0 {
		return nil
	}
	res := make([]any, 0, len(items))
	for _, item := range items {
		res = append(res, item)
	}
	return res
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
	case string:
		value := strings.TrimSpace(t)
		if value == "" {
			return 0
		}
		if parsedInt, err := strconv.ParseInt(value, 10, 64); err == nil {
			return parsedInt
		}
		if parsedFloat, err := strconv.ParseFloat(value, 64); err == nil {
			return int64(parsedFloat)
		}
		return 0
	default:
		return 0
	}
}

func deriveMediaDimensionsFromAnalysis(dataMap bson.M) (int64, int64) {
	classify := asMap(dataMap["classify"])
	details := asSlice(classify["details"])
	if len(details) == 0 {
		details = asSlice(dataMap["details"])
	}
	for _, raw := range details {
		detail := asMap(raw)
		width := asInt64(detail["frameWidth"])
		height := asInt64(detail["frameHeight"])
		if width > 0 && height > 0 {
			return width, height
		}
	}

	counting := asMap(dataMap["counting"])
	countingDetails := asSlice(counting["detail"])
	if len(countingDetails) == 0 {
		countingDetails = asSlice(counting["details"])
	}
	for _, raw := range countingDetails {
		detail := asMap(raw)
		width := asInt64(detail["videoWidth"])
		height := asInt64(detail["videoHeight"])
		if width > 0 && height > 0 {
			return width, height
		}
	}

	thumby := asMap(dataMap["thumby"])
	width := asInt64(thumby["width"])
	height := asInt64(thumby["height"])
	if width > 0 && height > 0 {
		return width, height
	}

	sprite := asMap(dataMap["sprite"])
	width = asInt64(sprite["width"])
	height = asInt64(sprite["height"])
	if width > 0 && height > 0 {
		return width, height
	}

	return 0, 0
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
	case primitive.D:
		m := bson.M{}
		for _, elem := range t {
			m[elem.Key] = elem.Value
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
	case primitive.A:
		res := make([]any, 0, len(t))
		for _, item := range t {
			res = append(res, item)
		}
		return res
	case []string:
		res := make([]any, 0, len(t))
		for _, s := range t {
			res = append(res, s)
		}
		return res
	case []bson.M:
		res := make([]any, 0, len(t))
		for _, item := range t {
			res = append(res, item)
		}
		return res
	case []primitive.D:
		res := make([]any, 0, len(t))
		for _, item := range t {
			res = append(res, item)
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

func resolveOrganisationForDoc(doc bson.M, fallback string) string {
	if v := asString(doc["organisationId"]); v != "" {
		return v
	}
	if v := asString(doc["userid"]); v != "" {
		return v
	}
	if v := asString(doc["user_id"]); v != "" {
		return v
	}
	return fallback
}

func deriveTimesFromAnalysisShapedDoc(doc bson.M) (int64, int64, int64) {
	start := asInt64(doc["start"])
	if start <= 0 {
		start = asInt64(doc["timestamp"])
	}

	end := asInt64(doc["end"])
	if end < 0 {
		end = 0
	}

	duration := asInt64(doc["duration"])
	if duration <= 0 && start > 0 && end > start {
		duration = (end - start) * 1000
	}
	if end <= 0 && duration > 0 && start > 0 {
		end = start + duration/1000
	}

	return start, end, duration
}

func buildPatchSetFields(doc bson.M, analysis analysisSnapshot, fallbackOrganisation string) bson.M {
	setFields := bson.M{}

	organisation := asString(doc["organisationId"])
	deviceID := asString(doc["deviceId"])
	deviceKey := asString(doc["deviceKey"])
	startTS := asInt64(doc["startTimestamp"])
	endTS := asInt64(doc["endTimestamp"])
	duration := asInt64(doc["duration"])
	videoFile := asString(doc["videoFile"])

	if organisation == "" {
		if resolved := resolveOrganisationForDoc(doc, fallbackOrganisation); resolved != "" {
			setFields["organisationId"] = resolved
		}
	}
	if deviceKey == "" && deviceID != "" {
		setFields["deviceKey"] = deviceID
	}

	if startTS <= 0 {
		if derived := asInt64(doc["timestamp"]); derived > 0 {
			setFields["startTimestamp"] = derived
			startTS = derived
		} else if derived := asInt64(doc["start"]); derived > 0 {
			setFields["startTimestamp"] = derived
			startTS = derived
		}
	}
	if endTS <= 0 {
		if derived := asInt64(doc["end"]); derived > 0 {
			setFields["endTimestamp"] = derived
			endTS = derived
		}
	}
	if duration <= 0 && startTS > 0 && endTS > startTS {
		duration = (endTS - startTS) * 1000
		setFields["duration"] = duration
	}
	if endTS <= 0 && duration > 0 && startTS > 0 {
		setFields["endTimestamp"] = startTS + duration/1000
	}

	thumbnailFile := asString(doc["thumbnailFile"])
	thumbnailProvider := asString(doc["thumbnailProvider"])
	spriteFile := asString(doc["spriteFile"])
	spriteProvider := asString(doc["spriteProvider"])
	classificationSummaryLen := len(asSlice(doc["classificationSummary"]))
	markerNamesLen := len(asSlice(doc["markerNames"]))
	countingSummaryLen := len(asSlice(doc["countingSummary"]))
	metadata := asMap(doc["metadata"])
	metaAnalysisID := asString(metadata["analysisId"])
	metaSpriteInterval := asInt64(metadata["spriteInterval"])
	metaDominantColorsLen := len(asSlice(metadata["dominantColors"]))
	metaClassifications := asSlice(metadata["classifications"])
	metaClassificationsLen := len(metaClassifications)
	metaWidth := asInt64(metadata["width"])
	metaHeight := asInt64(metadata["height"])
	metaResolution := asString(metadata["resolution"])
	metaCount := asInt64(metadata["count"])

	if thumbnailFile == "" {
		if analysis.ThumbyFilename != "" {
			setFields["thumbnailFile"] = analysis.ThumbyFilename
		} else if videoFile != "" && strings.HasSuffix(videoFile, ".mp4") {
			setFields["thumbnailFile"] = strings.TrimSuffix(videoFile, ".mp4") + "_thumbnail.jpg"
		}
	}
	if thumbnailProvider == "" && analysis.ThumbyProvider != "" {
		setFields["thumbnailProvider"] = analysis.ThumbyProvider
	}
	if spriteFile == "" && analysis.SpriteFilename != "" {
		setFields["spriteFile"] = analysis.SpriteFilename
	}
	if spriteProvider == "" && analysis.SpriteProvider != "" {
		setFields["spriteProvider"] = analysis.SpriteProvider
	}
	if metaAnalysisID == "" && analysis.ID != "" {
		setFields["metadata.analysisId"] = analysis.ID
	}
	if metaSpriteInterval <= 0 && analysis.SpriteInterval > 0 {
		setFields["metadata.spriteInterval"] = analysis.SpriteInterval
	}
	if metaDominantColorsLen == 0 && len(analysis.DominantColors) > 0 {
		setFields["metadata.dominantColors"] = analysis.DominantColors
	}
	if (metaClassificationsLen == 0 || !hasAnyNonEmptyCentroids(metaClassifications) || hasCentroidsOutsideNormalizedRange(metaClassifications)) && len(analysis.Classifications) > 0 {
		setFields["metadata.classifications"] = analysis.Classifications
	}
	if metaWidth <= 0 && analysis.Width > 0 {
		setFields["metadata.width"] = analysis.Width
	}
	if metaHeight <= 0 && analysis.Height > 0 {
		setFields["metadata.height"] = analysis.Height
	}
	if metaResolution == "" && analysis.Width > 0 && analysis.Height > 0 {
		setFields["metadata.resolution"] = fmt.Sprintf("%dx%d", analysis.Width, analysis.Height)
	}
	if classificationSummaryLen == 0 && len(analysis.ClassificationSummary) > 0 {
		setFields["classificationSummary"] = analysis.ClassificationSummary
	}
	if markerNamesLen == 0 && len(analysis.MarkerNames) > 0 {
		setFields["markerNames"] = analysis.MarkerNames
	}
	if countingSummaryLen == 0 && len(analysis.CountingSummary) > 0 {
		setFields["countingSummary"] = analysis.CountingSummary
	}
	if metaCount == 0 && analysis.CountTotal != 0 {
		setFields["metadata.count"] = analysis.CountTotal
	}

	return setFields
}

func deriveClassificationsFromAnalysis(dataMap bson.M) []bson.M {
	classify := asMap(dataMap["classify"])
	details := asSlice(classify["details"])
	if len(details) == 0 {
		details = asSlice(dataMap["details"])
	}
	properties := toStringSlice(asSlice(classify["properties"]))
	if len(properties) == 0 {
		properties = toStringSlice(asSlice(dataMap["properties"]))
	}
	if len(properties) == 0 {
		if classified := asString(dataMap["classified"]); classified != "" {
			properties = append(properties, classified)
		}
	}

	classifications := make([]bson.M, 0, len(details))
	for _, item := range details {
		detail := asMap(item)
		key := asString(detail["classified"])
		if key == "" {
			key = asString(detail["objectName"])
		}
		if key == "" {
			continue
		}

		frameWidth := asInt64(detail["frameWidth"])
		frameHeight := asInt64(detail["frameHeight"])

		centroids := toCentroids(detail["trajectCentroids"])
		centroids = normalizeCentroidsIfNeeded(centroids, frameWidth, frameHeight)
		if len(centroids) == 0 {
			centroids = toCentroidsFromTraject(detail["traject"], frameWidth, frameHeight)
		}
		if len(centroids) == 0 {
			x, okX := toFloat64(detail["x"])
			y, okY := toFloat64(detail["y"])
			if okX && okY {
				x, y = normalizePointIfNeeded(x, y, frameWidth, frameHeight)
				centroids = [][]float64{{x, y}}
			}
		}
		classifications = append(classifications, bson.M{
			"key":       key,
			"centroids": centroids,
		})
	}

	if len(classifications) == 0 && len(properties) > 0 {
		for _, property := range properties {
			classifications = append(classifications, bson.M{
				"key":       property,
				"centroids": [][]float64{},
			})
		}
	}

	return classifications
}

func deriveClassificationSummary(classifications []bson.M) []bson.M {
	if len(classifications) == 0 {
		return nil
	}

	countByKey := map[string]int64{}
	for _, classification := range classifications {
		key := asString(classification["key"])
		if key == "" {
			continue
		}
		countByKey[key]++
	}

	keys := make([]string, 0, len(countByKey))
	for key := range countByKey {
		keys = append(keys, key)
	}
	sortStrings(keys)

	summary := make([]bson.M, 0, len(keys))
	for _, key := range keys {
		summary = append(summary, bson.M{
			"key":   key,
			"count": countByKey[key],
		})
	}

	return summary
}

func markerNamesFromSummary(summary []bson.M) []string {
	names := make([]string, 0, len(summary))
	seen := map[string]struct{}{}
	for _, item := range summary {
		key := asString(item["key"])
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		names = append(names, key)
	}
	return names
}

func deriveCountingSummaryFromAnalysis(dataMap bson.M) ([]bson.M, int64) {
	counting := asMap(dataMap["counting"])
	records := asSlice(counting["records"])
	if len(records) == 0 {
		return nil, 0
	}

	countByType := map[string]int64{}
	durationByType := map[string]float64{}
	var total int64
	for _, raw := range records {
		record := asMap(raw)
		recordType := asString(record["type"])
		if recordType == "" {
			continue
		}
		rawValue := asInt64(record["count"])
		total += rawValue

		normalizedType := strings.ToLower(strings.TrimSpace(recordType))
		duration := 0.0
		if d, ok := toFloat64(record["duration"]); ok {
			duration = d
		}

		switch normalizedType {
		case "counting_line":
			if rawValue == 0 {
				continue
			}
			mappedKey := "countingIn"
			mappedCount := rawValue
			if rawValue < 0 {
				mappedKey = "countingOut"
				mappedCount = -rawValue
			}
			countByType[mappedKey] += mappedCount
			durationByType[mappedKey] += duration
		case "counting_region":
			countByType["countingRegion"] += rawValue
			durationByType["countingRegion"] += duration
		default:
			countByType[recordType] += rawValue
			durationByType[recordType] += duration
		}
	}

	keys := make([]string, 0, len(countByType))
	for key := range countByType {
		keys = append(keys, key)
	}
	sortStrings(keys)

	summary := make([]bson.M, 0, len(keys))
	for _, key := range keys {
		entry := bson.M{
			"key":   key,
			"count": countByType[key],
		}
		if durationByType[key] > 0 {
			entry["duration"] = durationByType[key]
		}
		summary = append(summary, entry)
	}

	return summary, total
}

func toCentroids(v any) [][]float64 {
	rawCentroids := asSlice(v)
	if len(rawCentroids) == 0 {
		return nil
	}

	centroids := make([][]float64, 0, len(rawCentroids))
	for _, raw := range rawCentroids {
		point := asSlice(raw)
		if len(point) < 2 {
			continue
		}
		x, okX := toFloat64(point[0])
		y, okY := toFloat64(point[1])
		if !okX || !okY {
			continue
		}
		centroids = append(centroids, []float64{x, y})
	}
	return centroids
}

func toCentroidsFromTraject(v any, frameWidth int64, frameHeight int64) [][]float64 {
	rawTraject := asSlice(v)
	if len(rawTraject) == 0 {
		return nil
	}

	centroids := make([][]float64, 0, len(rawTraject))
	for _, raw := range rawTraject {
		point := asSlice(raw)
		if len(point) >= 4 {
			x1, ok1 := toFloat64(point[0])
			y1, ok2 := toFloat64(point[1])
			x2, ok3 := toFloat64(point[2])
			y2, ok4 := toFloat64(point[3])
			if ok1 && ok2 && ok3 && ok4 {
				x, y := normalizePointIfNeeded((x1+x2)/2, (y1+y2)/2, frameWidth, frameHeight)
				centroids = append(centroids, []float64{x, y})
				continue
			}
		}

		if len(point) >= 2 {
			x, okX := toFloat64(point[0])
			y, okY := toFloat64(point[1])
			if okX && okY {
				x, y = normalizePointIfNeeded(x, y, frameWidth, frameHeight)
				centroids = append(centroids, []float64{x, y})
			}
		}
	}

	return centroids
}

func normalizeCentroidsIfNeeded(centroids [][]float64, frameWidth int64, frameHeight int64) [][]float64 {
	if len(centroids) == 0 {
		return centroids
	}

	out := make([][]float64, 0, len(centroids))
	for _, point := range centroids {
		if len(point) < 2 {
			continue
		}
		x, y := normalizePointIfNeeded(point[0], point[1], frameWidth, frameHeight)
		out = append(out, []float64{x, y})
	}
	return out
}

func normalizePointIfNeeded(x float64, y float64, frameWidth int64, frameHeight int64) (float64, float64) {
	if frameWidth <= 0 || frameHeight <= 0 {
		return x, y
	}

	// Region filters use 0..100 coordinate space.
	if (x >= 0 && x <= 100) && (y >= 0 && y <= 100) {
		return x, y
	}

	nx := (x * 100.0) / float64(frameWidth)
	ny := (y * 100.0) / float64(frameHeight)
	return clamp(nx, 0, 100), clamp(ny, 0, 100)
}

func clamp(v float64, min float64, max float64) float64 {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func toFloat64(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	case int32:
		return float64(t), true
	case int64:
		return float64(t), true
	case string:
		value := strings.TrimSpace(t)
		if value == "" {
			return 0, false
		}
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

func sortStrings(values []string) {
	if len(values) < 2 {
		return
	}
	for i := 0; i < len(values)-1; i++ {
		for j := i + 1; j < len(values); j++ {
			if values[j] < values[i] {
				values[i], values[j] = values[j], values[i]
			}
		}
	}
}

func toStringSlice(values []any) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if s, ok := value.(string); ok && strings.TrimSpace(s) != "" {
			out = append(out, strings.TrimSpace(s))
		}
	}
	return out
}

func hasAnyNonEmptyCentroids(classifications []any) bool {
	for _, raw := range classifications {
		classification := asMap(raw)
		if classification == nil {
			continue
		}
		centroids := asSlice(classification["centroids"])
		if len(centroids) > 0 {
			return true
		}
	}
	return false
}

func hasCentroidsOutsideNormalizedRange(classifications []any) bool {
	for _, raw := range classifications {
		classification := asMap(raw)
		if classification == nil {
			continue
		}
		centroids := asSlice(classification["centroids"])
		for _, rawPoint := range centroids {
			point := asSlice(rawPoint)
			if len(point) < 2 {
				continue
			}
			x, okX := toFloat64(point[0])
			y, okY := toFloat64(point[1])
			if !okX || !okY {
				continue
			}
			if x < 0 || x > 100 || y < 0 || y > 100 {
				return true
			}
		}
	}
	return false
}
