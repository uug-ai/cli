package actions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/uug-ai/cli/database"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	organisationsBackfillAdapterVersion  = "v1"
	organisationsBackfillExitOperational = 1
	organisationsBackfillExitData        = 2
	organisationsBackfillExitUsage       = 64
)

type OrganisationsBackfillConfig struct {
	Mode                       string
	MongoDBURI                 string
	MongoDBHost                string
	MongoDBPort                string
	MongoDBSourceDatabase      string
	MongoDBDestinationDatabase string
	MongoDBDatabaseCredentials string
	MongoDBUsername            string
	MongoDBPassword            string
	MigrationVersion           int
	AdapterVersion             string
	Collection                 string
	AllCollections             bool
	BatchSize                  int
	MigrationTimeoutMinutes    int
	OrganisationID             string
	DocumentID                 string
	Resume                     bool
	Restart                    bool
	CheckMigrationIndexes      bool
	ApplyMigrationIndexes      bool
	StopOnConflict             bool
	ReportFile                 string
}

type organisationsBackfillAdapter struct {
	Collection       string
	TargetField      string
	TargetBSONType   string
	LegacyCandidates []string
	PreservedFields  []string
}

type organisationsBackfillReport struct {
	Mode             string                                     `json:"mode"`
	Database         string                                     `json:"database"`
	MigrationVersion int                                        `json:"migrationVersion"`
	AdapterVersion   string                                     `json:"adapterVersion"`
	BatchSize        int                                        `json:"batchSize"`
	StartedAt        string                                     `json:"startedAt"`
	CompletedAt      string                                     `json:"completedAt"`
	PreflightStatus  string                                     `json:"preflightStatus"`
	Collections      map[string]organisationsBackfillCollection `json:"collections"`
}

type organisationsBackfillCollection struct {
	TargetField          string           `json:"targetField"`
	TargetBSONType       string           `json:"targetBsonType"`
	LegacyCandidates     []string         `json:"legacyCandidates"`
	PreservedFields      []string         `json:"preservedFields"`
	Total                int64            `json:"total"`
	CanonicalPresent     int64            `json:"canonicalPresent"`
	CanonicalMissing     int64            `json:"canonicalMissing"`
	CanonicalWrongType   int64            `json:"canonicalWrongType"`
	CanonicalInvalidHex  int64            `json:"canonicalInvalidHex"`
	LegacyCandidateCount map[string]int64 `json:"legacyCandidateCount"`
	Indexes              []string         `json:"indexes"`
	TargetIndexCovered   bool             `json:"targetIndexCovered"`
}

type organisationsBackfillError struct {
	code int
	err  error
}

func (e *organisationsBackfillError) Error() string {
	return e.err.Error()
}

func (e *organisationsBackfillError) Unwrap() error {
	return e.err
}

func OrganisationsBackfillExitCode(err error) int {
	if err == nil {
		return 0
	}
	var backfillError *organisationsBackfillError
	if errors.As(err, &backfillError) {
		return backfillError.code
	}
	return organisationsBackfillExitOperational
}

func OrganisationsBackfill(config OrganisationsBackfillConfig) error {
	config = normalizeOrganisationsBackfillConfig(config)
	adapters, err := validateOrganisationsBackfillConfig(config)
	if err != nil {
		return &organisationsBackfillError{code: organisationsBackfillExitUsage, err: err}
	}

	ctx := context.Background()
	cancel := func() {}
	if config.MigrationTimeoutMinutes > 0 {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(config.MigrationTimeoutMinutes)*time.Minute)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	var db *database.DB
	if config.MongoDBURI != "" {
		db = database.NewMongoDBURI(config.MongoDBURI)
	} else {
		db = database.NewMongoDBHost(
			config.MongoDBHost,
			config.MongoDBPort,
			config.MongoDBDatabaseCredentials,
			config.MongoDBUsername,
			config.MongoDBPassword,
		)
	}
	client := db.Client
	defer client.Disconnect(context.Background())

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return &organisationsBackfillError{code: organisationsBackfillExitOperational, err: fmt.Errorf("MongoDB preflight failed: %w", err)}
	}

	hubDB := client.Database(config.MongoDBDestinationDatabase)
	for _, collection := range []string{"users", "organisation"} {
		if _, err := hubDB.Collection(collection).EstimatedDocumentCount(ctx); err != nil {
			return &organisationsBackfillError{code: organisationsBackfillExitOperational, err: fmt.Errorf("cannot read %s: %w", collection, err)}
		}
	}

	startedAt := time.Now().UTC()
	report := organisationsBackfillReport{
		Mode:             config.Mode,
		Database:         config.MongoDBDestinationDatabase,
		MigrationVersion: config.MigrationVersion,
		AdapterVersion:   config.AdapterVersion,
		BatchSize:        config.BatchSize,
		StartedAt:        startedAt.Format(time.RFC3339),
		PreflightStatus:  "passed",
		Collections:      make(map[string]organisationsBackfillCollection, len(adapters)),
	}

	hasDataConflict := false
	for _, adapter := range adapters {
		collectionReport, err := inspectOrganisationsBackfillCollection(ctx, hubDB.Collection(adapter.Collection), adapter, config.DocumentID)
		if err != nil {
			return &organisationsBackfillError{code: organisationsBackfillExitOperational, err: fmt.Errorf("inspect %s: %w", adapter.Collection, err)}
		}
		if collectionReport.CanonicalWrongType > 0 || collectionReport.CanonicalInvalidHex > 0 {
			hasDataConflict = true
			report.PreflightStatus = "blocked"
		}
		report.Collections[adapter.Collection] = collectionReport
		if hasDataConflict && config.StopOnConflict {
			break
		}
	}

	report.CompletedAt = time.Now().UTC().Format(time.RFC3339)
	if err := writeOrganisationsBackfillReport(report, config.ReportFile); err != nil {
		return &organisationsBackfillError{code: organisationsBackfillExitOperational, err: err}
	}
	if hasDataConflict {
		return &organisationsBackfillError{code: organisationsBackfillExitData, err: errors.New("preflight found canonical tenant values with invalid BSON type or ObjectID hex")}
	}
	return nil
}

func normalizeOrganisationsBackfillConfig(config OrganisationsBackfillConfig) OrganisationsBackfillConfig {
	config.Mode = strings.ToLower(strings.TrimSpace(config.Mode))
	if config.Mode == "" {
		config.Mode = "dry-run"
	}
	config.Collection = strings.ToLower(strings.TrimSpace(config.Collection))
	config.AdapterVersion = strings.TrimSpace(config.AdapterVersion)
	if config.AdapterVersion == "" {
		config.AdapterVersion = organisationsBackfillAdapterVersion
	}
	config.OrganisationID = strings.TrimSpace(config.OrganisationID)
	config.DocumentID = strings.TrimSpace(config.DocumentID)
	config.ReportFile = strings.TrimSpace(config.ReportFile)
	config.MongoDBURI = strings.TrimSpace(config.MongoDBURI)
	config.MongoDBDestinationDatabase = strings.TrimSpace(config.MongoDBDestinationDatabase)
	if config.MongoDBDestinationDatabase == "" {
		config.MongoDBDestinationDatabase = strings.TrimSpace(config.MongoDBSourceDatabase)
	}
	if config.MigrationVersion == 0 {
		config.MigrationVersion = 1
	}
	if config.BatchSize == 0 {
		config.BatchSize = 500
	}
	return config
}

func validateOrganisationsBackfillConfig(config OrganisationsBackfillConfig) ([]organisationsBackfillAdapter, error) {
	if config.Mode != "dry-run" && config.Mode != "live" {
		return nil, fmt.Errorf("mode must be dry-run or live, got %q", config.Mode)
	}
	if config.Mode == "live" {
		return nil, errors.New("live mode is disabled until checkpointed compare-and-set writes are implemented")
	}
	if config.MongoDBDestinationDatabase == "" || strings.HasPrefix(config.MongoDBDestinationDatabase, "-") {
		return nil, errors.New("an explicit MongoDB source or destination database is required")
	}
	if config.MongoDBURI == "" && strings.TrimSpace(config.MongoDBHost) == "" {
		return nil, errors.New("provide -mongodb-uri or -mongodb-host")
	}
	if config.MigrationVersion != 1 {
		return nil, fmt.Errorf("unsupported migration version %d", config.MigrationVersion)
	}
	if config.AdapterVersion != organisationsBackfillAdapterVersion {
		return nil, fmt.Errorf("unsupported adapter version %q", config.AdapterVersion)
	}
	if config.BatchSize <= 0 {
		return nil, errors.New("batch-size must be greater than zero")
	}
	if config.Collection == "" && !config.AllCollections {
		return nil, errors.New("provide exactly one of -collection or -all")
	}
	if config.Collection != "" && config.AllCollections {
		return nil, errors.New("-collection and -all are mutually exclusive")
	}
	if config.Resume && config.Restart {
		return nil, errors.New("-resume and -restart are mutually exclusive")
	}
	if config.Resume || config.Restart {
		return nil, errors.New("checkpoint resume/restart is disabled until live mode is implemented")
	}
	if config.ApplyMigrationIndexes {
		return nil, errors.New("index creation is disabled until adapter-specific index contracts are registered")
	}
	if config.OrganisationID != "" {
		if _, err := primitive.ObjectIDFromHex(config.OrganisationID); err != nil {
			return nil, fmt.Errorf("invalid organisation-id: %w", err)
		}
		return nil, errors.New("organisation-scoped resolution is disabled until collection resolvers are implemented")
	}
	if config.DocumentID != "" {
		if config.AllCollections || config.Collection == "" {
			return nil, errors.New("document-id requires exactly one -collection")
		}
		if _, err := primitive.ObjectIDFromHex(config.DocumentID); err != nil {
			return nil, fmt.Errorf("invalid document-id: %w", err)
		}
	}
	return selectOrganisationsBackfillAdapters(config.Collection, config.AllCollections)
}

func selectOrganisationsBackfillAdapters(collection string, all bool) ([]organisationsBackfillAdapter, error) {
	registry := organisationsBackfillAdapters()
	if all {
		collections := make([]string, 0, len(registry))
		for name := range registry {
			collections = append(collections, name)
		}
		sort.Strings(collections)
		adapters := make([]organisationsBackfillAdapter, 0, len(collections))
		for _, name := range collections {
			adapters = append(adapters, registry[name])
		}
		return adapters, nil
	}
	if adapter, ok := registry[collection]; ok {
		return []organisationsBackfillAdapter{adapter}, nil
	}
	if reason, blocked := organisationsBackfillBlockedAdapters()[collection]; blocked {
		return nil, fmt.Errorf("collection %q is blocked: %s", collection, reason)
	}
	return nil, fmt.Errorf("collection %q has no registered adapter", collection)
}

func organisationsBackfillAdapters() map[string]organisationsBackfillAdapter {
	return map[string]organisationsBackfillAdapter{
		"alerts": {
			Collection:       "alerts",
			TargetField:      "organisationId",
			TargetBSONType:   "string",
			LegacyCandidates: []string{"master_user_id"},
			PreservedFields:  []string{"user_id"},
		},
		"devices": {
			Collection:       "devices",
			TargetField:      "organisationId",
			TargetBSONType:   "string",
			LegacyCandidates: []string{"user_id"},
			PreservedFields:  []string{"user_id"},
		},
		"groups": {
			Collection:       "groups",
			TargetField:      "organisationId",
			TargetBSONType:   "string",
			LegacyCandidates: []string{"user_id"},
			PreservedFields:  []string{"created_by", "updated_by"},
		},
		"sites": {
			Collection:       "sites",
			TargetField:      "organisationId",
			TargetBSONType:   "string",
			LegacyCandidates: []string{"user_id"},
			PreservedFields:  []string{"created_by", "updated_by"},
		},
	}
}

func organisationsBackfillBlockedAdapters() map[string]string {
	return map[string]string{
		"analysis":      "shared model and canonical BSON type are not declared",
		"channels":      "persistence and ownership contracts are unverified",
		"counting":      "persisted shapes require a production audit",
		"heatmap":       "persisted shapes require a production audit",
		"io":            "product ownership remains undecided",
		"labels":        "shared model does not declare organisationId",
		"notifications": "shape-specific canonical models and writers are missing",
		"sequences":     "shared model and canonical BSON type are not declared",
		"settings":      "shared model does not declare a canonical tenant field",
		"tasks":         "shared model and canonical BSON type are not declared",
		"videowalls":    "shared model does not declare organisationId",
		"workflow_runs": "persisted canonical BSON type is not verified",
	}
}

func inspectOrganisationsBackfillCollection(
	ctx context.Context,
	collection *mongo.Collection,
	adapter organisationsBackfillAdapter,
	documentID string,
) (organisationsBackfillCollection, error) {
	baseFilter := bson.M{}
	if documentID != "" {
		id, _ := primitive.ObjectIDFromHex(documentID)
		baseFilter["_id"] = id
	}

	report := organisationsBackfillCollection{
		TargetField:          adapter.TargetField,
		TargetBSONType:       adapter.TargetBSONType,
		LegacyCandidates:     append([]string(nil), adapter.LegacyCandidates...),
		PreservedFields:      append([]string(nil), adapter.PreservedFields...),
		LegacyCandidateCount: make(map[string]int64, len(adapter.LegacyCandidates)),
	}

	counts := []struct {
		target *int64
		filter bson.M
	}{
		{&report.Total, baseFilter},
		{&report.CanonicalPresent, combineOrganisationsBackfillFilters(baseFilter, bson.M{adapter.TargetField: bson.M{"$type": adapter.TargetBSONType, "$ne": ""}})},
		{&report.CanonicalMissing, combineOrganisationsBackfillFilters(baseFilter, bson.M{"$or": bson.A{
			bson.M{adapter.TargetField: bson.M{"$exists": false}},
			bson.M{adapter.TargetField: nil},
			bson.M{adapter.TargetField: ""},
		}})},
		{&report.CanonicalWrongType, combineOrganisationsBackfillFilters(baseFilter, bson.M{
			adapter.TargetField: bson.M{"$exists": true, "$ne": nil},
			"$expr":             bson.M{"$ne": bson.A{bson.M{"$type": "$" + adapter.TargetField}, adapter.TargetBSONType}},
		})},
		{&report.CanonicalInvalidHex, combineOrganisationsBackfillFilters(baseFilter, bson.M{adapter.TargetField: bson.M{
			"$type": adapter.TargetBSONType,
			"$ne":   "",
			"$not":  primitive.Regex{Pattern: "^[0-9a-fA-F]{24}$"},
		}})},
	}
	for _, count := range counts {
		value, err := collection.CountDocuments(ctx, count.filter)
		if err != nil {
			return report, err
		}
		*count.target = value
	}
	for _, candidate := range adapter.LegacyCandidates {
		count, err := collection.CountDocuments(ctx, combineOrganisationsBackfillFilters(baseFilter, bson.M{candidate: bson.M{"$exists": true, "$nin": bson.A{nil, ""}}}))
		if err != nil {
			return report, err
		}
		report.LegacyCandidateCount[candidate] = count
	}

	indexCursor, err := collection.Indexes().List(ctx)
	if err != nil {
		return report, err
	}
	defer indexCursor.Close(ctx)
	for indexCursor.Next(ctx) {
		var index bson.M
		if err := indexCursor.Decode(&index); err != nil {
			return report, err
		}
		if name, ok := index["name"].(string); ok {
			report.Indexes = append(report.Indexes, name)
		}
		if organisationsBackfillIndexCoversField(index["key"], adapter.TargetField) {
			report.TargetIndexCovered = true
		}
	}
	if err := indexCursor.Err(); err != nil {
		return report, err
	}
	sort.Strings(report.Indexes)
	return report, nil
}

func combineOrganisationsBackfillFilters(filters ...bson.M) bson.M {
	nonEmpty := make([]bson.M, 0, len(filters))
	for _, filter := range filters {
		if len(filter) > 0 {
			nonEmpty = append(nonEmpty, filter)
		}
	}
	if len(nonEmpty) == 0 {
		return bson.M{}
	}
	if len(nonEmpty) == 1 {
		return nonEmpty[0]
	}
	clauses := make(bson.A, len(nonEmpty))
	for index, filter := range nonEmpty {
		clauses[index] = filter
	}
	return bson.M{"$and": clauses}
}

func organisationsBackfillIndexCoversField(key any, field string) bool {
	switch value := key.(type) {
	case bson.D:
		for _, element := range value {
			if element.Key == field {
				return true
			}
		}
	case bson.M:
		_, ok := value[field]
		return ok
	}
	return false
}

func writeOrganisationsBackfillReport(report organisationsBackfillReport, reportFile string) error {
	output, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	fmt.Println(string(output))
	if reportFile == "" {
		return nil
	}
	if err := os.WriteFile(reportFile, append(output, '\n'), 0o600); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	return nil
}
