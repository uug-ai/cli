package actions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/uug-ai/cli/database"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// backfill-device-names gives every v2 device document a top-level display
// name and removes the pre-release agent.cameraName field.
//
// The display name is user-editable in Hub, so a non-empty name is never
// touched. Every write carries its precondition in the update filter, which
// makes a concurrent rename win over the backfill and makes re-runs no-ops.

const (
	deviceNamesBackfillExitOperational  = 1
	deviceNamesBackfillExitUsage        = 64
	deviceNamesBackfillDefaultBatchSize = 500
	deviceNamesBackfillDefaultColl      = "devices"
	deviceNamesBackfillKeyPrefixLength  = 6
	// deviceNamesBackfillBlankPattern matches the empty string and whitespace-only strings.
	// $regex only matches string values, so other BSON types never match.
	deviceNamesBackfillBlankPattern = `^\s*$`
)

type DeviceNamesBackfillConfig struct {
	Mode                       string
	MongoDBURI                 string
	MongoDBHost                string
	MongoDBPort                string
	MongoDBSourceDatabase      string
	MongoDBDestinationDatabase string
	MongoDBDatabaseCredentials string
	MongoDBUsername            string
	MongoDBPassword            string
	DeviceCollection           string
	BatchSize                  int
	MigrationTimeoutMinutes    int
	ReportFile                 string
}

// deviceNamesBackfillReport only ever contains counts: no names, keys or
// document contents.
type deviceNamesBackfillReport struct {
	Mode               string                     `json:"mode"`
	Database           string                     `json:"database"`
	Collection         string                     `json:"collection"`
	BatchSize          int                        `json:"batchSize"`
	StartedAt          string                     `json:"startedAt"`
	CompletedAt        string                     `json:"completedAt"`
	Scanned            int64                      `json:"scanned"`
	DecodeErrors       int64                      `json:"decodeErrors"`
	NamesToSet         int64                      `json:"namesToSet"`
	CameraNameToRemove int64                      `json:"cameraNameToRemove"`
	AgentNameToCopy    int64                      `json:"agentNameToCopy"`
	SkippedNoAgentName int64                      `json:"skippedNoAgentName"`
	Written            *deviceNamesBackfillWrites `json:"written,omitempty"`
}

type deviceNamesBackfillWrites struct {
	NamesSet          int64 `json:"namesSet"`
	CameraNameRemoved int64 `json:"cameraNameRemoved"`
	AgentNameCopied   int64 `json:"agentNameCopied"`
	// SkippedConcurrentChange counts planned writes whose filter no longer
	// matched, e.g. because the device was renamed after it was scanned.
	SkippedConcurrentChange int64 `json:"skippedConcurrentChange"`
	Errors                  int64 `json:"errors"`
}

type deviceNamesBackfillError struct {
	code int
	err  error
}

func (e *deviceNamesBackfillError) Error() string { return e.err.Error() }
func (e *deviceNamesBackfillError) Unwrap() error { return e.err }

func DeviceNamesBackfillExitCode(err error) int {
	if err == nil {
		return 0
	}
	var backfillError *deviceNamesBackfillError
	if errors.As(err, &backfillError) {
		return backfillError.code
	}
	return deviceNamesBackfillExitOperational
}

func normalizeDeviceNamesBackfillConfig(config DeviceNamesBackfillConfig) DeviceNamesBackfillConfig {
	config.Mode = strings.ToLower(strings.TrimSpace(config.Mode))
	if config.Mode == "" {
		config.Mode = "dry-run"
	}
	config.MongoDBURI = strings.TrimSpace(config.MongoDBURI)
	config.MongoDBDestinationDatabase = strings.TrimSpace(config.MongoDBDestinationDatabase)
	if config.MongoDBDestinationDatabase == "" {
		config.MongoDBDestinationDatabase = strings.TrimSpace(config.MongoDBSourceDatabase)
	}
	config.DeviceCollection = strings.TrimSpace(config.DeviceCollection)
	if config.DeviceCollection == "" {
		config.DeviceCollection = deviceNamesBackfillDefaultColl
	}
	config.ReportFile = strings.TrimSpace(config.ReportFile)
	if config.BatchSize == 0 {
		config.BatchSize = deviceNamesBackfillDefaultBatchSize
	}
	return config
}

func validateDeviceNamesBackfillConfig(config DeviceNamesBackfillConfig) error {
	if config.Mode != "dry-run" && config.Mode != "live" {
		return fmt.Errorf("mode must be dry-run or live, got %q", config.Mode)
	}
	if config.MongoDBDestinationDatabase == "" || strings.HasPrefix(config.MongoDBDestinationDatabase, "-") {
		return errors.New("an explicit MongoDB source or destination database is required")
	}
	if config.MongoDBURI == "" && strings.TrimSpace(config.MongoDBHost) == "" {
		return errors.New("provide -mongodb-uri or -mongodb-host")
	}
	if config.BatchSize <= 0 {
		return errors.New("batch-size must be greater than zero")
	}
	if strings.HasPrefix(config.DeviceCollection, "-") {
		return errors.New("invalid device collection name")
	}
	return nil
}

// BackfillDeviceNames runs the backfill-device-names action.
func BackfillDeviceNames(config DeviceNamesBackfillConfig) error {
	config = normalizeDeviceNamesBackfillConfig(config)
	if err := validateDeviceNamesBackfillConfig(config); err != nil {
		return &deviceNamesBackfillError{code: deviceNamesBackfillExitUsage, err: err}
	}

	ctx := context.Background()
	var cancel context.CancelFunc
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
		return &deviceNamesBackfillError{code: deviceNamesBackfillExitOperational, err: fmt.Errorf("MongoDB preflight failed: %w", err)}
	}

	collection := client.Database(config.MongoDBDestinationDatabase).Collection(config.DeviceCollection)
	report, runErr := runDeviceNamesBackfill(ctx, collection, config)
	report.CompletedAt = time.Now().UTC().Format(time.RFC3339)
	if err := writeDeviceNamesBackfillReport(report, config.ReportFile); err != nil {
		return &deviceNamesBackfillError{code: deviceNamesBackfillExitOperational, err: err}
	}
	if runErr != nil {
		return &deviceNamesBackfillError{code: deviceNamesBackfillExitOperational, err: runErr}
	}
	return nil
}

// deviceNamesBackfillPlan is the outcome for one device document.
type deviceNamesBackfillPlan struct {
	// SetName is the value for the top-level name; empty means no name write.
	SetName string
	// SkippedNoAgentName is true when the name is blank but no agent name exists.
	SkippedNoAgentName bool
	// UnsetCameraName is true when agent.cameraName exists and must be removed.
	UnsetCameraName bool
	// CopyAgentName is the agent.cameraName value to copy into a missing
	// agent.name in the same update; empty means no copy.
	CopyAgentName string
}

func (p deviceNamesBackfillPlan) isNoop() bool {
	return p.SetName == "" && !p.UnsetCameraName
}

// planDeviceNamesBackfill decides what to write for one device document,
// decoded with the projection from deviceNamesBackfillProjection.
func planDeviceNamesBackfill(doc bson.M) deviceNamesBackfillPlan {
	var plan deviceNamesBackfillPlan

	agent, _ := doc["agent"].(bson.M)
	agentName, _ := nonBlankString(agent, "name")
	cameraNameValue, hasCameraName := agent["cameraName"]
	cameraName, _ := cameraNameValue.(string)

	if hasCameraName {
		plan.UnsetCameraName = true
		if isBlankOrMissing(agent, "name") && strings.TrimSpace(cameraName) != "" {
			plan.CopyAgentName = cameraName
		}
	}

	if isBlankOrMissing(doc, "name") {
		// agent.name after this run: the stored value, or the cameraName
		// value copied into it by the cleanup write.
		effectiveAgentName := agentName
		if effectiveAgentName == "" {
			effectiveAgentName = plan.CopyAgentName
		}
		switch {
		case strings.TrimSpace(effectiveAgentName) != "":
			plan.SetName = strings.TrimSpace(effectiveAgentName)
		case strings.TrimSpace(lastAnalyticsCameraName(doc)) != "":
			plan.SetName = strings.TrimSpace(lastAnalyticsCameraName(doc))
		default:
			plan.SkippedNoAgentName = true
		}
	}
	return plan
}

// isBlankOrMissing mirrors deviceNamesBackfillBlankFilter: the field is absent,
// null, or a whitespace-only string. Other BSON types are not blank.
func isBlankOrMissing(doc bson.M, field string) bool {
	if doc == nil {
		return true
	}
	value, ok := doc[field]
	if !ok || value == nil {
		return true
	}
	text, isString := value.(string)
	return isString && strings.TrimSpace(text) == ""
}

func nonBlankString(doc bson.M, field string) (string, bool) {
	if doc == nil {
		return "", false
	}
	text, ok := doc[field].(string)
	if !ok || strings.TrimSpace(text) == "" {
		return "", false
	}
	return text, true
}

func lastAnalyticsCameraName(doc bson.M) string {
	analytics, ok := doc["analytics"].(bson.A)
	if !ok || len(analytics) == 0 {
		return ""
	}
	last, ok := analytics[len(analytics)-1].(bson.M)
	if !ok {
		return ""
	}
	name, _ := nonBlankString(last, "cameraname")
	return name
}

// deviceNamesBackfillBlankFilter matches documents whose field is missing,
// null, or a whitespace-only string ({field: null} matches missing and null).
func deviceNamesBackfillBlankFilter(field string) bson.A {
	return bson.A{
		bson.M{field: nil},
		bson.M{field: bson.M{"$regex": deviceNamesBackfillBlankPattern}},
	}
}

func deviceNamesBackfillSelector() bson.M {
	selector := deviceNamesBackfillBlankFilter("name")
	selector = append(selector, bson.M{"agent.cameraName": bson.M{"$exists": true}})
	return bson.M{"$or": selector}
}

func deviceNamesBackfillProjection() bson.M {
	return bson.M{
		"_id":                  1,
		"key":                  1,
		"name":                 1,
		"agent.name":           1,
		"agent.cameraName":     1,
		"analytics.cameraname": 1,
	}
}

// deviceNamesBackfillNameModel sets the top-level name only while it is still
// missing, null or blank, so a rename after the scan is never overwritten.
func deviceNamesBackfillNameModel(id any, name string) mongo.WriteModel {
	return mongo.NewUpdateOneModel().
		SetFilter(bson.M{"_id": id, "$or": deviceNamesBackfillBlankFilter("name")}).
		SetUpdate(bson.M{"$set": bson.M{"name": name}})
}

// deviceNamesBackfillCopyModel moves agent.cameraName into agent.name only
// while agent.name is still missing and cameraName still holds the scanned value.
func deviceNamesBackfillCopyModel(id any, cameraName string) mongo.WriteModel {
	return mongo.NewUpdateOneModel().
		SetFilter(bson.M{
			"_id":              id,
			"agent.cameraName": cameraName,
			"$or":              deviceNamesBackfillBlankFilter("agent.name"),
		}).
		SetUpdate(bson.M{
			"$set":   bson.M{"agent.name": cameraName},
			"$unset": bson.M{"agent.cameraName": ""},
		})
}

func deviceNamesBackfillUnsetModel(id any) mongo.WriteModel {
	return mongo.NewUpdateOneModel().
		SetFilter(bson.M{"_id": id, "agent.cameraName": bson.M{"$exists": true}}).
		SetUpdate(bson.M{"$unset": bson.M{"agent.cameraName": ""}})
}

type deviceNamesBackfillWriteGroup struct {
	label  string
	models []mongo.WriteModel
	keys   []string
}

func (g *deviceNamesBackfillWriteGroup) add(model mongo.WriteModel, key string) {
	g.models = append(g.models, model)
	g.keys = append(g.keys, key)
}

func (g *deviceNamesBackfillWriteGroup) reset() {
	g.models = g.models[:0]
	g.keys = g.keys[:0]
}

func runDeviceNamesBackfill(ctx context.Context, collection *mongo.Collection, config DeviceNamesBackfillConfig) (deviceNamesBackfillReport, error) {
	live := config.Mode == "live"
	report := deviceNamesBackfillReport{
		Mode:       config.Mode,
		Database:   collection.Database().Name(),
		Collection: collection.Name(),
		BatchSize:  config.BatchSize,
		StartedAt:  time.Now().UTC().Format(time.RFC3339),
	}
	if live {
		report.Written = &deviceNamesBackfillWrites{}
	}

	cursor, err := collection.Find(ctx, deviceNamesBackfillSelector(),
		options.Find().SetProjection(deviceNamesBackfillProjection()).SetBatchSize(int32(config.BatchSize)))
	if err != nil {
		return report, fmt.Errorf("query %s: %w", collection.Name(), err)
	}
	defer cursor.Close(ctx)

	names := &deviceNamesBackfillWriteGroup{label: "name"}
	copies := &deviceNamesBackfillWriteGroup{label: "agent-name-copy"}
	unsets := &deviceNamesBackfillWriteGroup{label: "camera-name-unset"}
	pending := 0

	var flushErr error
	flush := func() {
		if live {
			if result, ok := applyDeviceNamesBackfillGroup(ctx, collection, names, report.Written); ok {
				report.Written.NamesSet += result.ModifiedCount
			}
			if result, ok := applyDeviceNamesBackfillGroup(ctx, collection, copies, report.Written); ok {
				report.Written.AgentNameCopied += result.ModifiedCount
				report.Written.CameraNameRemoved += result.ModifiedCount
			}
			if result, ok := applyDeviceNamesBackfillGroup(ctx, collection, unsets, report.Written); ok {
				report.Written.CameraNameRemoved += result.ModifiedCount
			}
			if ctx.Err() != nil && flushErr == nil {
				flushErr = ctx.Err()
			}
		}
		names.reset()
		copies.reset()
		unsets.reset()
		pending = 0
	}

	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			report.DecodeErrors++
			continue
		}
		report.Scanned++

		plan := planDeviceNamesBackfill(doc)
		if plan.SkippedNoAgentName {
			report.SkippedNoAgentName++
		}
		if plan.isNoop() {
			continue
		}
		key, _ := doc["key"].(string)
		key = truncateDeviceKey(key)
		if plan.SetName != "" {
			report.NamesToSet++
			names.add(deviceNamesBackfillNameModel(doc["_id"], plan.SetName), key)
		}
		if plan.UnsetCameraName {
			report.CameraNameToRemove++
			if plan.CopyAgentName != "" {
				report.AgentNameToCopy++
				copies.add(deviceNamesBackfillCopyModel(doc["_id"], plan.CopyAgentName), key)
			} else {
				unsets.add(deviceNamesBackfillUnsetModel(doc["_id"]), key)
			}
		}
		pending++
		if pending >= config.BatchSize {
			flush()
			if flushErr != nil {
				return report, flushErr
			}
			log.Printf("backfill-device-names: scanned %d device documents...\n", report.Scanned)
		}
	}
	if err := cursor.Err(); err != nil {
		return report, fmt.Errorf("cursor error while reading %s: %w", collection.Name(), err)
	}
	flush()
	if flushErr != nil {
		return report, flushErr
	}
	if live && report.Written.Errors > 0 {
		return report, fmt.Errorf("%d device write(s) failed; re-run to retry", report.Written.Errors)
	}
	return report, nil
}

// applyDeviceNamesBackfillGroup writes one group unordered. Write errors are
// logged by code and truncated device key only: driver error messages can
// embed document values (e.g. duplicate key errors).
func applyDeviceNamesBackfillGroup(
	ctx context.Context,
	collection *mongo.Collection,
	group *deviceNamesBackfillWriteGroup,
	writes *deviceNamesBackfillWrites,
) (*mongo.BulkWriteResult, bool) {
	if len(group.models) == 0 {
		return nil, false
	}
	planned := int64(len(group.models))
	result, err := collection.BulkWrite(ctx, group.models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		var bulkErr mongo.BulkWriteException
		if errors.As(err, &bulkErr) && len(bulkErr.WriteErrors) > 0 {
			writes.Errors += int64(len(bulkErr.WriteErrors))
			for _, writeErr := range bulkErr.WriteErrors {
				key := ""
				if writeErr.Index >= 0 && writeErr.Index < len(group.keys) {
					key = group.keys[writeErr.Index]
				}
				log.Printf("backfill-device-names: %s write failed for device %q (code %d)\n", group.label, key, writeErr.Code)
			}
			if result != nil {
				writes.SkippedConcurrentChange += planned - int64(len(bulkErr.WriteErrors)) - result.MatchedCount
			}
			return result, result != nil
		}
		writes.Errors += planned
		// Server errors are reported by code only; their messages may echo values.
		var serverErr mongo.ServerError
		if errors.As(err, &serverErr) {
			log.Printf("backfill-device-names: %s batch of %d writes failed (server error)\n", group.label, planned)
		} else {
			log.Printf("backfill-device-names: %s batch of %d writes failed: %v\n", group.label, planned, err)
		}
		return nil, false
	}
	writes.SkippedConcurrentChange += planned - result.MatchedCount
	return result, true
}

func truncateDeviceKey(key string) string {
	runes := []rune(key)
	if len(runes) <= deviceNamesBackfillKeyPrefixLength {
		return key
	}
	return string(runes[:deviceNamesBackfillKeyPrefixLength])
}

func writeDeviceNamesBackfillReport(report deviceNamesBackfillReport, reportFile string) error {
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
