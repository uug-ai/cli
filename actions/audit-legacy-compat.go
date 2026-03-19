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
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type compatibilityDomainReport struct {
	Collection         string           `json:"collection"`
	Scanned            int64            `json:"scanned"`
	MatchedFilter      int64            `json:"matchedFilter"`
	MissingRequired    map[string]int64 `json:"missingRequired"`
	MissingRecommended map[string]int64 `json:"missingRecommended"`
	FallbackCandidates map[string]int64 `json:"fallbackCandidates"`
	Examples           map[string]string `json:"examples,omitempty"`
}

type compatibilityAuditReport struct {
	Mode         string                              `json:"mode"`
	Database     string                              `json:"database"`
	Organisation string                              `json:"organisationId,omitempty"`
	Username     string                              `json:"username,omitempty"`
	StartTS      int64                               `json:"startTimestamp,omitempty"`
	EndTS        int64                               `json:"endTimestamp,omitempty"`
	Domains      map[string]compatibilityDomainReport `json:"domains"`
}

type compatibilityDomainDefinition struct {
	Name       string
	Collection string
	Projection bson.M
	BuildMatch func(orgID string, startTS int64, endTS int64) bson.M
	Evaluate   func(doc bson.M, report *compatibilityDomainReport)
}

func AuditLegacyCompatibility(
	mode string,
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
	domainsCSV string,
) {
	dbName := strings.TrimSpace(mongodbDestinationDatabase)
	if dbName == "" {
		dbName = strings.TrimSpace(mongodbSourceDatabase)
	}
	if dbName == "" {
		log.Println("Please provide a database name through -mongodb-destination-database or -mongodb-source-database")
		return
	}

	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		mode = "dry-run"
	}

	var db *database.DB
	if mongodbURI != "" {
		db = database.NewMongoDBURI(mongodbURI)
	} else {
		db = database.NewMongoDBHost(mongodbHost, mongodbPort, mongodbDatabaseCredentials, mongodbUsername, mongodbPassword)
	}

	client := db.Client
	defer client.Disconnect(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	hubDB := client.Database(dbName)
	orgID := resolveAuditOrganisationID(ctx, hubDB, strings.TrimSpace(username), strings.TrimSpace(organisationId))

	definitions := compatibilityDomainDefinitions()
	selected := selectCompatibilityDomains(definitions, domainsCSV)
	if len(selected) == 0 {
		log.Println("No compatible domains selected. Available domains: media,analysis,users,devices,groups,sites,settings")
		return
	}

	report := compatibilityAuditReport{
		Mode:         mode,
		Database:     dbName,
		Organisation: orgID,
		Username:     strings.TrimSpace(username),
		StartTS:      startTimestamp,
		EndTS:        endTimestamp,
		Domains:      map[string]compatibilityDomainReport{},
	}

	for _, domain := range selected {
		domainReport := compatibilityDomainReport{
			Collection:         domain.Collection,
			MissingRequired:    map[string]int64{},
			MissingRecommended: map[string]int64{},
			FallbackCandidates: map[string]int64{},
			Examples:           map[string]string{},
		}

		coll := hubDB.Collection(domain.Collection)
		match := domain.BuildMatch(orgID, startTimestamp, endTimestamp)
		if match == nil {
			match = bson.M{}
		}

		count, err := coll.CountDocuments(ctx, match)
		if err == nil {
			domainReport.MatchedFilter = count
		}

		cursor, err := coll.Find(ctx, match, options.Find().SetProjection(domain.Projection))
		if err != nil {
			domainReport.Examples["query_error"] = err.Error()
			report.Domains[domain.Name] = domainReport
			continue
		}

		for cursor.Next(ctx) {
			var doc bson.M
			if err := cursor.Decode(&doc); err != nil {
				continue
			}
			domainReport.Scanned++
			domain.Evaluate(doc, &domainReport)
		}
		_ = cursor.Close(ctx)
		report.Domains[domain.Name] = domainReport
	}

	out, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		log.Printf("Failed to marshal compatibility report: %v\n", err)
		return
	}

	fmt.Println("====================================")
	fmt.Println("Legacy Compatibility Audit Report")
	fmt.Println("====================================")
	fmt.Println(string(out))
}

func compatibilityDomainDefinitions() map[string]compatibilityDomainDefinition {
	return map[string]compatibilityDomainDefinition{
		"media": {
			Name:       "media",
			Collection: "media",
			Projection: bson.M{
				"_id":                    1,
				"key":                    1,
				"videoFile":              1,
				"organisationId":         1,
				"deviceId":               1,
				"deviceKey":              1,
				"startTimestamp":         1,
				"endTimestamp":           1,
				"duration":               1,
				"timestamp":              1,
				"start":                  1,
				"end":                    1,
				"thumbnailFile":          1,
				"spriteFile":             1,
				"classificationSummary":  1,
				"markerNames":            1,
				"countingSummary":        1,
				"metadata.analysisId":    1,
				"metadata.spriteInterval": 1,
				"metadata.classifications": 1,
			},
			BuildMatch: buildMatchMedia,
			Evaluate:   evaluateMediaCompatibility,
		},
		"analysis": {
			Name:       "analysis",
			Collection: "analysis",
			Projection: bson.M{
				"_id":                    1,
				"key":                    1,
				"userid":                 1,
				"user_id":                1,
				"timestamp":              1,
				"start":                  1,
				"end":                    1,
				"data.thumby.filename":   1,
				"data.sprite.filename":   1,
				"data.classify.details":  1,
				"data.counting.records":  1,
			},
			BuildMatch: buildMatchAnalysis,
			Evaluate:   evaluateAnalysisCompatibility,
		},
		"users": {
			Name:       "users",
			Collection: "users",
			Projection: bson.M{
				"_id":                  1,
				"username":             1,
				"email":                1,
				"timezone":             1,
				"notification_settings": 1,
				"notificationSettings": 1,
			},
			BuildMatch: buildMatchUsers,
			Evaluate:   evaluateUsersCompatibility,
		},
		"devices": {
			Name:       "devices",
			Collection: "devices",
			Projection: bson.M{
				"_id":      1,
				"user_id":  1,
				"key":      1,
				"name":     1,
				"timezone": 1,
				"site":     1,
				"sites":    1,
			},
			BuildMatch: buildMatchUserScopedCollection,
			Evaluate:   evaluateDevicesCompatibility,
		},
		"groups": {
			Name:       "groups",
			Collection: "groups",
			Projection: bson.M{
				"_id":      1,
				"user_id":  1,
				"name":     1,
				"devices":  1,
				"groups":   1,
				"sites":    1,
			},
			BuildMatch: buildMatchUserScopedCollection,
			Evaluate:   evaluateGroupsCompatibility,
		},
		"sites": {
			Name:       "sites",
			Collection: "sites",
			Projection: bson.M{
				"_id":      1,
				"user_id":  1,
				"name":     1,
				"devices":  1,
				"groups":   1,
			},
			BuildMatch: buildMatchUserScopedCollection,
			Evaluate:   evaluateSitesCompatibility,
		},
		"settings": {
			Name:       "settings",
			Collection: "settings",
			Projection: bson.M{
				"_id":                 1,
				"user_id":             1,
				"timezone":            1,
				"notificationSettings": 1,
				"notification_settings": 1,
			},
			BuildMatch: buildMatchUserScopedCollection,
			Evaluate:   evaluateSettingsCompatibility,
		},
	}
}

func selectCompatibilityDomains(definitions map[string]compatibilityDomainDefinition, csv string) []compatibilityDomainDefinition {
	defaultOrder := []string{"media", "analysis", "users", "devices", "groups", "sites", "settings"}
	selectedNames := defaultOrder
	if strings.TrimSpace(csv) != "" {
		selectedNames = parseCSV(csv)
	}
	out := make([]compatibilityDomainDefinition, 0, len(selectedNames))
	for _, name := range selectedNames {
		if def, ok := definitions[strings.TrimSpace(name)]; ok {
			out = append(out, def)
		}
	}
	return out
}

func resolveAuditOrganisationID(ctx context.Context, db *mongo.Database, username string, organisationID string) string {
	orgID := strings.TrimSpace(organisationID)
	if orgID != "" || strings.TrimSpace(username) == "" {
		return orgID
	}

	var userDoc struct {
		ID any `bson:"_id"`
	}
	err := db.Collection("users").FindOne(ctx, bson.M{"username": strings.TrimSpace(username)}).Decode(&userDoc)
	if err != nil {
		return ""
	}
	return objectIDToHex(userDoc.ID)
}

func buildMatchMedia(orgID string, startTS int64, endTS int64) bson.M {
	match := bson.M{}
	if orgID != "" {
		match["organisationId"] = orgID
	}
	if startTS > 0 && endTS > 0 && startTS <= endTS {
		match["$or"] = []bson.M{
			{
				"$and": []bson.M{
					{"startTimestamp": bson.M{"$lte": endTS}},
					{"endTimestamp": bson.M{"$gte": startTS}},
				},
			},
			{"startTimestamp": bson.M{"$gte": startTS, "$lte": endTS}},
			{"timestamp": bson.M{"$gte": startTS, "$lte": endTS}},
			{
				"$and": []bson.M{
					{"start": bson.M{"$lte": endTS}},
					{"end": bson.M{"$gte": startTS}},
				},
			},
		}
	}
	return match
}

func buildMatchAnalysis(orgID string, startTS int64, endTS int64) bson.M {
	match := bson.M{}
	if orgID != "" {
		match["$or"] = []bson.M{
			{"userid": orgID},
			{"user_id": orgID},
		}
	}
	if startTS > 0 && endTS > 0 && startTS <= endTS {
		timeCond := bson.M{
			"$or": []bson.M{
				{"timestamp": bson.M{"$gte": startTS, "$lte": endTS}},
				{
					"$and": []bson.M{
						{"start": bson.M{"$lte": endTS}},
						{"end": bson.M{"$gte": startTS}},
					},
				},
				{"start": bson.M{"$gte": startTS, "$lte": endTS}},
			},
		}
		if existingOr, ok := match["$or"]; ok {
			match = bson.M{"$and": []bson.M{{"$or": existingOr}, timeCond}}
		} else {
			match = timeCond
		}
	}
	return match
}

func buildMatchUsers(orgID string, _ int64, _ int64) bson.M {
	if orgID == "" {
		return bson.M{}
	}
	match := bson.M{}
	ors := []bson.M{
		{"organisationId": orgID},
		{"userid": orgID},
		{"user_id": orgID},
	}
	if oid, err := primitive.ObjectIDFromHex(orgID); err == nil {
		ors = append(ors, bson.M{"_id": oid})
	}
	match["$or"] = ors
	return match
}

func buildMatchUserScopedCollection(orgID string, _ int64, _ int64) bson.M {
	if orgID == "" {
		return bson.M{}
	}
	return bson.M{"user_id": orgID}
}

func evaluateMediaCompatibility(doc bson.M, report *compatibilityDomainReport) {
	docID := compatibilityDocIdentifier(doc)

	videoFile := asString(doc["videoFile"])
	key := asString(doc["key"])
	organisationID := asString(doc["organisationId"])
	deviceID := asString(doc["deviceId"])
	deviceKey := asString(doc["deviceKey"])
	startTS := asInt64(doc["startTimestamp"])
	endTS := asInt64(doc["endTimestamp"])
	duration := asInt64(doc["duration"])
	metadata := asMap(doc["metadata"])

	if videoFile == "" && key == "" {
		compatAddFinding(report.MissingRequired, report.Examples, "videoFileOrKey", docID)
	}
	if organisationID == "" {
		compatAddFinding(report.MissingRequired, report.Examples, "organisationId", docID)
	}
	if deviceKey == "" && deviceID == "" {
		compatAddFinding(report.MissingRequired, report.Examples, "deviceRef", docID)
	}
	if startTS <= 0 {
		compatAddFinding(report.MissingRequired, report.Examples, "startTimestamp", docID)
		if asInt64(doc["timestamp"]) > 0 || asInt64(doc["start"]) > 0 {
			report.FallbackCandidates["startTimestamp.fromLegacy"]++
		}
	}
	if endTS <= 0 {
		compatAddFinding(report.MissingRequired, report.Examples, "endTimestamp", docID)
		if asInt64(doc["end"]) > 0 || (startTS > 0 && duration > 0) {
			report.FallbackCandidates["endTimestamp.fromLegacy"]++
		}
	}
	if duration <= 0 {
		compatAddFinding(report.MissingRequired, report.Examples, "duration", docID)
		if startTS > 0 && endTS > startTS {
			report.FallbackCandidates["duration.fromRange"]++
		}
	}
	if deviceKey == "" && deviceID != "" {
		report.FallbackCandidates["deviceKey.fromDeviceId"]++
	}

	if asString(doc["thumbnailFile"]) == "" {
		compatAddFinding(report.MissingRecommended, report.Examples, "thumbnailFile", docID)
	}
	if asString(doc["spriteFile"]) == "" {
		compatAddFinding(report.MissingRecommended, report.Examples, "spriteFile", docID)
	}
	if asInt64(metadata["spriteInterval"]) <= 0 {
		compatAddFinding(report.MissingRecommended, report.Examples, "metadata.spriteInterval", docID)
	}
	if len(asSlice(metadata["classifications"])) == 0 {
		compatAddFinding(report.MissingRecommended, report.Examples, "metadata.classifications", docID)
	}
	if len(asSlice(doc["classificationSummary"])) == 0 {
		compatAddFinding(report.MissingRecommended, report.Examples, "classificationSummary", docID)
	}
	if len(asSlice(doc["markerNames"])) == 0 {
		compatAddFinding(report.MissingRecommended, report.Examples, "markerNames", docID)
	}
	if len(asSlice(doc["countingSummary"])) == 0 {
		compatAddFinding(report.MissingRecommended, report.Examples, "countingSummary", docID)
	}
}

func evaluateAnalysisCompatibility(doc bson.M, report *compatibilityDomainReport) {
	docID := compatibilityDocIdentifier(doc)

	if asString(doc["key"]) == "" {
		compatAddFinding(report.MissingRequired, report.Examples, "key", docID)
	}
	if asString(doc["userid"]) == "" && asString(doc["user_id"]) == "" {
		compatAddFinding(report.MissingRequired, report.Examples, "userRef", docID)
	}
	if asMap(doc["data"]) == nil {
		compatAddFinding(report.MissingRequired, report.Examples, "data", docID)
	}
	if asInt64(doc["timestamp"]) <= 0 && asInt64(doc["start"]) <= 0 {
		compatAddFinding(report.MissingRequired, report.Examples, "time", docID)
	}

	if !compatHasNestedNonEmpty(doc, "data.thumby.filename") {
		compatAddFinding(report.MissingRecommended, report.Examples, "data.thumby.filename", docID)
	}
	if !compatHasNestedNonEmpty(doc, "data.sprite.filename") {
		compatAddFinding(report.MissingRecommended, report.Examples, "data.sprite.filename", docID)
	}
	if !compatHasNestedArray(doc, "data.classify.details") {
		compatAddFinding(report.MissingRecommended, report.Examples, "data.classify.details", docID)
	}
	if !compatHasNestedArray(doc, "data.counting.records") {
		compatAddFinding(report.MissingRecommended, report.Examples, "data.counting.records", docID)
	}
}

func evaluateUsersCompatibility(doc bson.M, report *compatibilityDomainReport) {
	docID := compatibilityDocIdentifier(doc)

	if asString(doc["username"]) == "" {
		compatAddFinding(report.MissingRequired, report.Examples, "username", docID)
	}
	if asString(doc["email"]) == "" {
		compatAddFinding(report.MissingRequired, report.Examples, "email", docID)
	}

	if asString(doc["timezone"]) == "" {
		compatAddFinding(report.MissingRecommended, report.Examples, "timezone", docID)
	}
	if asMap(doc["notificationSettings"]) == nil && asMap(doc["notification_settings"]) == nil {
		compatAddFinding(report.MissingRecommended, report.Examples, "notificationSettings", docID)
	}
}

func evaluateDevicesCompatibility(doc bson.M, report *compatibilityDomainReport) {
	docID := compatibilityDocIdentifier(doc)

	if asString(doc["user_id"]) == "" {
		compatAddFinding(report.MissingRequired, report.Examples, "user_id", docID)
	}
	if asString(doc["key"]) == "" {
		compatAddFinding(report.MissingRequired, report.Examples, "key", docID)
	}

	if asString(doc["name"]) == "" {
		compatAddFinding(report.MissingRecommended, report.Examples, "name", docID)
	}
	if asString(doc["timezone"]) == "" {
		compatAddFinding(report.MissingRecommended, report.Examples, "timezone", docID)
	}
}

func evaluateGroupsCompatibility(doc bson.M, report *compatibilityDomainReport) {
	docID := compatibilityDocIdentifier(doc)

	if asString(doc["user_id"]) == "" {
		compatAddFinding(report.MissingRequired, report.Examples, "user_id", docID)
	}
	if asString(doc["name"]) == "" {
		compatAddFinding(report.MissingRequired, report.Examples, "name", docID)
	}

	if len(asSlice(doc["devices"])) == 0 {
		compatAddFinding(report.MissingRecommended, report.Examples, "devices", docID)
	}
	if len(asSlice(doc["sites"])) == 0 {
		compatAddFinding(report.MissingRecommended, report.Examples, "sites", docID)
	}
}

func evaluateSitesCompatibility(doc bson.M, report *compatibilityDomainReport) {
	docID := compatibilityDocIdentifier(doc)

	if asString(doc["user_id"]) == "" {
		compatAddFinding(report.MissingRequired, report.Examples, "user_id", docID)
	}
	if asString(doc["name"]) == "" {
		compatAddFinding(report.MissingRequired, report.Examples, "name", docID)
	}

	if len(asSlice(doc["devices"])) == 0 && len(asSlice(doc["groups"])) == 0 {
		compatAddFinding(report.MissingRecommended, report.Examples, "devicesOrGroups", docID)
	}
}

func evaluateSettingsCompatibility(doc bson.M, report *compatibilityDomainReport) {
	docID := compatibilityDocIdentifier(doc)

	if asString(doc["user_id"]) == "" {
		compatAddFinding(report.MissingRequired, report.Examples, "user_id", docID)
	}

	if asString(doc["timezone"]) == "" {
		compatAddFinding(report.MissingRecommended, report.Examples, "timezone", docID)
	}
	if asMap(doc["notificationSettings"]) == nil && asMap(doc["notification_settings"]) == nil {
		compatAddFinding(report.MissingRecommended, report.Examples, "notificationSettings", docID)
	}
}

func compatAddFinding(counter map[string]int64, examples map[string]string, key string, docID string) {
	counter[key]++
	exampleKey := "example." + key
	if _, ok := examples[exampleKey]; !ok && docID != "" {
		examples[exampleKey] = docID
	}
}

func compatibilityDocIdentifier(doc bson.M) string {
	if value := asString(doc["videoFile"]); value != "" {
		return value
	}
	if value := asString(doc["key"]); value != "" {
		return value
	}
	return objectIDToHex(doc["_id"])
}

func compatHasNestedNonEmpty(doc bson.M, path string) bool {
	value, ok := compatGetNested(doc, path)
	if !ok || value == nil {
		return false
	}
	switch t := value.(type) {
	case string:
		return strings.TrimSpace(t) != ""
	case []any:
		return len(t) > 0
	case primitive.A:
		return len(t) > 0
	case bson.M:
		return len(t) > 0
	case map[string]any:
		return len(t) > 0
	default:
		return true
	}
}

func compatHasNestedArray(doc bson.M, path string) bool {
	value, ok := compatGetNested(doc, path)
	if !ok || value == nil {
		return false
	}
	switch t := value.(type) {
	case []any:
		return len(t) > 0
	case primitive.A:
		return len(t) > 0
	default:
		return false
	}
}

func compatGetNested(doc bson.M, path string) (any, bool) {
	current := any(doc)
	parts := strings.Split(path, ".")
	for _, part := range parts {
		switch typed := current.(type) {
		case bson.M:
			next, ok := typed[part]
			if !ok {
				return nil, false
			}
			current = next
		case map[string]any:
			next, ok := typed[part]
			if !ok {
				return nil, false
			}
			current = next
		default:
			return nil, false
		}
	}
	return current, true
}
