package loadtest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

var mongoTestDatabase = regexp.MustCompile(`^loadtest_[a-zA-Z0-9_]+$`)

const (
	sentinelCollection = "loadtest_guard"
	manifestCollection = "loadtest_runs"
)

type mongoStore struct {
	client *mongo.Client
	db     *mongo.Database
	cfg    Config
}

// NewMongoStore owns one reusable driver client; pool sizing remains at driver defaults.
func NewMongoStore(ctx context.Context, cfg Config) (Store, error) {
	if err := validateMongoTarget(cfg); err != nil {
		return nil, err
	}
	op, cancel := context.WithTimeout(ctx, cfg.OperationTimeout)
	defer cancel()
	// Locks must be durably acknowledged and read from the primary, regardless
	// of a URI's general-purpose read/write preference.
	client, err := mongo.Connect(op, loadTestMongoOptions(cfg.MongoURI))
	if err != nil {
		// Driver URI parse errors can contain credentials.
		return nil, errors.New("could not configure load-test MongoDB client")
	}
	return &mongoStore{client: client, db: client.Database(cfg.Database), cfg: cfg}, nil
}

func loadTestMongoOptions(uri string) *options.ClientOptions {
	clientOptions := options.Client().ApplyURI(uri).SetReadPreference(readpref.Primary())
	concern := writeconcern.Majority()
	if clientOptions.WriteConcern != nil {
		concern.Journal = clientOptions.WriteConcern.Journal
		concern.WTimeout = clientOptions.WriteConcern.WTimeout
	}
	return clientOptions.SetWriteConcern(concern)
}

func validateMongoTarget(cfg Config) error {
	legacyDefault := cfg.AllowDefaultNames && cfg.ConfirmTestTarget && cfg.Database == "Kerberos"
	if (!mongoTestDatabase.MatchString(cfg.Database) || len(cfg.Database) > 63) && !legacyDefault {
		return errors.New("MongoDB requires loadtest_<name>; isolated legacy Kerberos requires allow-default-names and test-target confirmation")
	}
	if cfg.OperationTimeout <= 0 {
		return errors.New("MongoDB operation timeout must be positive")
	}
	if !cfg.Execute || !cfg.ConfirmTestTarget {
		return errors.New("execution and explicit test-target confirmation are required")
	}
	return nil
}

func (s *mongoStore) operation(ctx context.Context, fn func(context.Context) error) error {
	op, cancel := context.WithTimeout(ctx, s.cfg.OperationTimeout)
	defer cancel()
	return fn(op)
}

func (s *mongoStore) findOne(ctx context.Context, collection string, filter any, result any) error {
	return s.operation(ctx, func(op context.Context) error {
		return s.db.Collection(collection).FindOne(op, filter).Decode(result)
	})
}

func (s *mongoStore) insert(ctx context.Context, collection string, documents []any) error {
	if len(documents) == 0 {
		return nil
	}
	return s.operation(ctx, func(op context.Context) error {
		_, err := s.db.Collection(collection).InsertMany(op, documents)
		return err
	})
}

func (s *mongoStore) transition(ctx context.Context, runID string, states []string, values bson.M) error {
	return s.operation(ctx, func(op context.Context) error {
		result, err := s.db.Collection(manifestCollection).UpdateOne(op,
			bson.M{"_id": runID, "schema": Schema, "state": bson.M{"$in": states}},
			bson.M{"$set": values})
		if err != nil {
			return err
		}
		if result.MatchedCount != 1 {
			return errors.New("run state changed or operation is not permitted in its current state")
		}
		return nil
	})
}

func (s *mongoStore) guard(ctx context.Context, initialize bool) error {
	if err := validateMongoTarget(s.cfg); err != nil {
		return err
	}
	if s.db == nil || s.db.Name() != s.cfg.Database {
		return errors.New("MongoDB target does not match the configured test database")
	}
	var sentinel struct {
		Schema   string `bson:"schema"`
		Database string `bson:"database"`
	}
	err := s.findOne(ctx, sentinelCollection, bson.M{"_id": "database"}, &sentinel)
	if err == nil {
		if sentinel.Schema != Schema || sentinel.Database != s.cfg.Database {
			return errors.New("database harness sentinel is invalid")
		}
		return nil
	}
	if !errors.Is(err, mongo.ErrNoDocuments) {
		return fmt.Errorf("read database harness sentinel: %w", err)
	}
	if !initialize {
		return errors.New("database has no harness sentinel; prepare an empty dedicated database first")
	}
	var names []string
	if err := s.operation(ctx, func(op context.Context) error {
		var err error
		names, err = s.db.ListCollectionNames(op, bson.D{})
		return err
	}); err != nil {
		return fmt.Errorf("inspect empty test database: %w", err)
	}
	if len(names) != 0 {
		return errors.New("refusing to initialize a nonempty database without a harness sentinel")
	}
	err = s.insert(ctx, sentinelCollection, []any{bson.M{
		"_id": "database", "schema": Schema, "database": s.cfg.Database,
	}})
	if mongo.IsDuplicateKeyError(err) {
		// A concurrent preparer may have initialized the same empty database.
		return s.guard(ctx, false)
	}
	return err
}

func (s *mongoStore) checkRunID(runID string) error {
	if !fixtureRunID.MatchString(runID) || (s.cfg.RunID != "" && s.cfg.RunID != runID) {
		return errors.New("invalid or mismatched run ID")
	}
	return nil
}

func (s *mongoStore) ensurePlan(ctx context.Context) error {
	var existing bson.M
	err := s.findOne(ctx, "settings", bson.M{"key": "plan"}, &existing)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return s.insert(ctx, "settings", []any{planFixture()})
	}
	if err != nil {
		return err
	}
	// Compare BSON-normalized documents, so numeric widths do not depend on Go int.
	encoded, err := bson.Marshal(planFixture())
	if err != nil {
		return err
	}
	var expected bson.M
	if err := bson.Unmarshal(encoded, &expected); err != nil {
		return err
	}
	if !reflect.DeepEqual(existing, expected) {
		return errors.New("existing plan settings are not the unchanged harness fixture; refusing to overwrite them")
	}
	return nil
}

// These are harness lookup indexes, not a production index rollout or parity claim.
func (s *mongoStore) ensureIndexes(ctx context.Context) error {
	specs := []struct {
		collection string
		keys       bson.D
	}{
		{"users", bson.D{{Key: "username", Value: 1}}},
		{"devices", bson.D{{Key: "key", Value: 1}}},
		{"settings", bson.D{{Key: "key", Value: 1}}},
		{"subscriptions", bson.D{{Key: "organisation_id", Value: 1}, {Key: "updated_at", Value: -1}, {Key: "_id", Value: -1}}},
		{"subscriptions", bson.D{{Key: "user_id", Value: 1}, {Key: "updated_at", Value: -1}, {Key: "_id", Value: -1}}},
		{"media", bson.D{{Key: "videoFile", Value: 1}}},
		{"media", bson.D{{Key: "organisationId", Value: 1}, {Key: "projectId", Value: 1}, {Key: "deviceId", Value: 1}, {Key: "startTimestamp", Value: 1}}},
		{"markers", bson.D{{Key: "organisationId", Value: 1}, {Key: "projectId", Value: 1}, {Key: "deviceId", Value: 1}, {Key: "startTimestamp", Value: 1}}},
	}
	for _, spec := range specs {
		if err := s.operation(ctx, func(op context.Context) error {
			_, err := s.db.Collection(spec.collection).Indexes().CreateOne(op, mongo.IndexModel{Keys: spec.keys})
			return err
		}); err != nil {
			return fmt.Errorf("create %s harness index: %w", spec.collection, err)
		}
	}
	return nil
}

func (s *mongoStore) recordFailure(ctx context.Context, runID, from, state, field string, cause error) error {
	// Preserve a diagnostic even when the caller cancelled the main operation.
	err := s.transition(context.WithoutCancel(ctx), runID, []string{from}, bson.M{
		"state": state, field: cause.Error(),
	})
	if err != nil {
		return errors.Join(cause, fmt.Errorf("retain failure on manifest: %w", err))
	}
	return cause
}

func (s *mongoStore) Prepare(ctx context.Context, m Manifest) error {
	if err := validateFixtureManifest(m); err != nil {
		return err
	}
	if err := s.checkRunID(m.RunID); err != nil {
		return err
	}
	if err := s.guard(ctx, true); err != nil {
		return err
	}
	if err := s.acquireRunLock(ctx, m.RunID, "preparing"); err != nil {
		return err
	}
	m.State, m.Report = "preparing", nil
	if err := s.insert(ctx, manifestCollection, []any{m}); err != nil {
		releaseErr := s.releaseRunLock(context.WithoutCancel(ctx), m.RunID, true)
		return errors.Join(fmt.Errorf("create new run manifest (run IDs cannot be reused): %w", err), releaseErr)
	}
	prepare := func() error {
		if err := s.ensurePlan(ctx); err != nil {
			return fmt.Errorf("prepare plan fixture: %w", err)
		}
		if err := s.ensureIndexes(ctx); err != nil {
			return err
		}
		buffers := map[string][]any{}
		err := walkFixtures(m, func(collection string, doc bson.M) error {
			buffers[collection] = append(buffers[collection], doc)
			if len(buffers[collection]) < fixtureBatchSize {
				return nil
			}
			if err := s.insert(ctx, collection, buffers[collection]); err != nil {
				return fmt.Errorf("insert %s fixtures: %w", collection, err)
			}
			buffers[collection] = nil
			return nil
		})
		if err != nil {
			return err
		}
		for collection, documents := range buffers {
			if err := s.insert(ctx, collection, documents); err != nil {
				return fmt.Errorf("insert %s fixtures: %w", collection, err)
			}
		}
		return s.transition(ctx, m.RunID, []string{"preparing"}, bson.M{"state": "prepared"})
	}
	if err := prepare(); err != nil {
		return s.recordFailure(ctx, m.RunID, "preparing", "failed", "preparationError", err)
	}
	return s.releaseRunLock(ctx, m.RunID, true)
}

func (s *mongoStore) Load(ctx context.Context, runID string) (Manifest, error) {
	if err := s.checkRunID(runID); err != nil {
		return Manifest{}, err
	}
	if err := s.guard(ctx, false); err != nil {
		return Manifest{}, err
	}
	var m Manifest
	if err := s.findOne(ctx, manifestCollection, bson.M{"_id": runID, "schema": Schema}, &m); err != nil {
		return Manifest{}, fmt.Errorf("load run manifest: %w", err)
	}
	if m.RunID != runID {
		return Manifest{}, errors.New("stored run ID does not match requested run")
	}
	if err := validateFixtureManifest(m); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

func (s *mongoStore) Claim(ctx context.Context, runID string) error {
	m, err := s.Load(ctx, runID)
	if err != nil {
		return err
	}
	if m.State != "prepared" {
		return errors.New("only a prepared run may be claimed")
	}
	if err := s.acquireRunLock(ctx, runID, "running"); err != nil {
		return err
	}
	if err := s.transition(ctx, runID, []string{"prepared"}, bson.M{"state": "running"}); err != nil {
		// A network failure may hide a committed transition. Releasing the lock
		// here would allow overlapping runs, so retain it for diagnosis.
		return fmt.Errorf("claim run state; database lock retained for diagnosis: %w", err)
	}
	return nil
}

func (s *mongoStore) acquireRunLock(ctx context.Context, runID, phase string) error {
	// Preparation and cleanup must not contaminate another run's measurements.
	if err := s.insert(ctx, sentinelCollection, []any{bson.M{
		"_id": "active-run", "schema": Schema, "runId": runID,
		"phase": phase, "claimedAt": time.Now().UTC(),
	}}); err != nil {
		return fmt.Errorf("claim database active-run lock (failed/interrupted operations retain their lock): %w", err)
	}
	return nil
}

func (s *mongoStore) releaseRunLock(ctx context.Context, runID string, requireOwned bool) error {
	return s.operation(ctx, func(op context.Context) error {
		result, err := s.db.Collection(sentinelCollection).DeleteOne(op, bson.M{
			"_id": "active-run", "schema": Schema, "runId": runID,
		})
		if err != nil {
			return err
		}
		if requireOwned && result.DeletedCount != 1 {
			return errors.New("database active-run lock was not owned by this run")
		}
		return nil
	})
}

func (s *mongoStore) SaveReport(ctx context.Context, report Report) error {
	if report.Schema != Schema {
		return errors.New("invalid report schema")
	}
	if _, err := s.Load(ctx, report.RunID); err != nil {
		return err
	}
	var lock bson.M
	if err := s.findOne(ctx, sentinelCollection, bson.M{
		"_id": "active-run", "schema": Schema, "runId": report.RunID,
	}, &lock); err != nil {
		return fmt.Errorf("verify database active-run lock before finalizing report: %w", err)
	}
	state := "failed"
	if report.Passed {
		state = "finished"
	}
	if err := s.transition(ctx, report.RunID, []string{"running"}, bson.M{"state": state, "report": report}); err != nil {
		return err
	}
	// A failed run can still have in-flight work; never silently unlock it.
	if report.Passed {
		if err := s.releaseRunLock(ctx, report.RunID, true); err != nil {
			return fmt.Errorf("report saved but database active-run lock could not be released: %w", err)
		}
	}
	return nil
}

func (s *mongoStore) find(ctx context.Context, collection string, filter, projection bson.M) ([]bson.Raw, error) {
	var rows []bson.Raw
	err := s.operation(ctx, func(op context.Context) error {
		cursor, err := s.db.Collection(collection).Find(op, filter, options.Find().SetProjection(projection).SetBatchSize(fixtureBatchSize))
		if err != nil {
			return err
		}
		defer cursor.Close(op)
		return cursor.All(op, &rows)
	})
	return rows, err
}

func rawString(raw bson.Raw, path ...string) string {
	value, _ := raw.Lookup(path...).StringValueOK()
	return value
}

func rawNumber(raw bson.Raw, path ...string) float64 {
	value := raw.Lookup(path...)
	switch value.Type {
	case bsontype.Double:
		return value.Double()
	case bsontype.Int32:
		return float64(value.Int32())
	case bsontype.Int64:
		return float64(value.Int64())
	default:
		return -1
	}
}

func verifyMediaRows(m Manifest, start, end int, rows []bson.Raw, result *Verification) {
	expected := make(map[string]FixtureIdentity, end-start)
	for index := start; index < end; index++ {
		identity, _ := ExpectedIdentity(m, index)
		expected[RecordingKey(m, index)] = identity
	}
	seen := make(map[string]bool, end-start)
	for _, row := range rows {
		key := rawString(row, "videoFile")
		identity, ok := expected[key]
		if !ok {
			continue
		}
		if seen[key] {
			result.Duplicates++
		} else {
			seen[key] = true
			result.Found++
		}
		project, projectOK := row.Lookup("projectId").ObjectIDOK()
		if rawString(row, "organisationId") != identity.OrganisationID.Hex() || !projectOK || project != identity.ProjectID ||
			rawString(row, "deviceId") != identity.DeviceKey || rawString(row, "deviceKey") != identity.DeviceKey {
			result.WrongScope++
		}
		if rawNumber(row, "duration") != 30000 || rawNumber(row, "metadata", "fps") != 25 ||
			rawNumber(row, "metadata", "fileSize") != 1024*1024 ||
			rawNumber(row, "startTimestamp") != float64(m.Timestamp) || rawNumber(row, "endTimestamp") != float64(m.Timestamp+30) {
			result.WrongMetadata++
		}
	}
}

func (s *mongoStore) Verify(ctx context.Context, requested Manifest) (Verification, error) {
	m, err := s.Load(ctx, requested.RunID)
	if err != nil {
		return Verification{}, err
	}
	result := Verification{Expected: m.Total}
	for start := 0; start < m.Total; start += fixtureBatchSize {
		end := min(start+fixtureBatchSize, m.Total)
		keys := make([]string, 0, end-start)
		for index := start; index < end; index++ {
			keys = append(keys, RecordingKey(m, index))
		}
		rows, err := s.find(ctx, "media", bson.M{"videoFile": bson.M{"$in": keys}}, bson.M{
			"_id": 0, "videoFile": 1, "organisationId": 1, "projectId": 1,
			"deviceId": 1, "deviceKey": 1, "duration": 1,
			"metadata.fps": 1, "metadata.fileSize": 1, "startTimestamp": 1, "endTimestamp": 1,
		})
		if err != nil {
			return result, fmt.Errorf("verify media: %w", err)
		}
		verifyMediaRows(m, start, end, rows, &result)
	}
	result.Missing = result.Expected - result.Found
	result.MissingDates, err = s.verifyDates(ctx, m)
	if err != nil {
		return result, fmt.Errorf("verify calendar dates: %w", err)
	}
	return result, nil
}

type calendarTarget struct {
	id       any
	identity FixtureIdentity
}

func calendarTargets(m Manifest) map[string][]calendarTarget {
	targets := map[string][]calendarTarget{}
	seen := map[string]bool{}
	for device := 0; device < min(m.Total, fixtureDeviceCount(m)); device++ {
		identity := identityForDevice(m, device)
		if m.Legacy {
			key := identity.OrganisationID.Hex()
			if !seen[key] {
				targets["users"] = append(targets["users"], calendarTarget{identity.OrganisationID, identity})
				seen[key] = true
			}
			continue
		}
		key := identity.ProjectID.Hex()
		if !seen[key] {
			targets["project"] = append(targets["project"], calendarTarget{identity.ProjectID, identity})
			seen[key] = true
		}
		targets["project_device_days"] = append(targets["project_device_days"], calendarTarget{deviceDaysID(identity), identity})
	}
	return targets
}

func hasDate(row bson.Raw, day string) bool {
	array, ok := row.Lookup("dates").ArrayOK()
	if !ok {
		return false
	}
	values, err := array.Values()
	if err != nil {
		return false
	}
	for _, value := range values {
		if date, ok := value.StringValueOK(); ok && date == day {
			return true
		}
	}
	return false
}

func (s *mongoStore) verifyDates(ctx context.Context, m Manifest) (int, error) {
	day := time.Unix(m.Timestamp, 0).UTC().Format("02-01-2006")
	missing := 0
	allTargets := calendarTargets(m)
	for _, collection := range []string{"users", "project", "project_device_days"} {
		targets := allTargets[collection]
		for start := 0; start < len(targets); start += fixtureBatchSize {
			batch := targets[start:min(start+fixtureBatchSize, len(targets))]
			ids := make([]any, 0, len(batch))
			expected := map[any]FixtureIdentity{}
			for _, target := range batch {
				ids = append(ids, target.id)
				expected[target.id] = target.identity
			}
			rows, err := s.find(ctx, collection, bson.M{"_id": bson.M{"$in": ids}},
				bson.M{"_id": 1, "dates": 1, "organisationId": 1, "projectId": 1})
			if err != nil {
				return missing, err
			}
			found := map[any]bool{}
			for _, row := range rows {
				var id any
				if collection == "project_device_days" {
					id = rawString(row, "_id")
				} else {
					id, _ = row.Lookup("_id").ObjectIDOK()
				}
				identity, ok := expected[id]
				if !ok || !hasDate(row, day) {
					continue
				}
				if collection != "users" {
					org, ok := row.Lookup("organisationId").ObjectIDOK()
					if !ok || org != identity.OrganisationID {
						continue
					}
				}
				if collection == "project_device_days" {
					project, ok := row.Lookup("projectId").ObjectIDOK()
					if !ok || project != identity.ProjectID {
						continue
					}
				}
				found[id] = true
			}
			missing += len(batch) - len(found)
		}
	}
	return missing, nil
}

func (s *mongoStore) delete(ctx context.Context, collection string, filter bson.M) error {
	return s.operation(ctx, func(op context.Context) error {
		_, err := s.db.Collection(collection).DeleteMany(op, filter)
		return err
	})
}

func (s *mongoStore) cleanupResources(ctx context.Context, m Manifest) error {
	// Live media is created by Sequence, so it has no harness tag. Restrict each
	// deletion to an exact generated filename AND its immutable expected owner.
	for start := 0; start < m.Total; start += fixtureBatchSize {
		filters := bson.A{}
		for index := start; index < min(start+fixtureBatchSize, m.Total); index++ {
			identity, _ := ExpectedIdentity(m, index)
			filters = append(filters, bson.M{
				"videoFile": RecordingKey(m, index), "organisationId": identity.OrganisationID.Hex(),
				"projectId": identity.ProjectID, "deviceId": identity.DeviceKey, "deviceKey": identity.DeviceKey,
			})
		}
		if err := s.delete(ctx, "media", bson.M{"$or": filters}); err != nil {
			return fmt.Errorf("delete run media: %w", err)
		}
	}
	for collection, targets := range calendarTargets(m) {
		if collection != "project_device_days" {
			continue
		}
		for start := 0; start < len(targets); start += fixtureBatchSize {
			filters := bson.A{}
			for _, target := range targets[start:min(start+fixtureBatchSize, len(targets))] {
				filters = append(filters, bson.M{"_id": target.id, "organisationId": target.identity.OrganisationID, "projectId": target.identity.ProjectID})
			}
			if err := s.delete(ctx, collection, bson.M{"$or": filters}); err != nil {
				return fmt.Errorf("delete run device dates: %w", err)
			}
		}
	}
	buffers := map[string][]any{}
	flush := func(collection string) error {
		ids := buffers[collection]
		if len(ids) == 0 {
			return nil
		}
		err := s.delete(ctx, collection, bson.M{
			"_id": bson.M{"$in": ids}, "loadtestRunID": m.RunID, "loadtestSchema": Schema,
		})
		if err != nil {
			return fmt.Errorf("delete %s run fixtures: %w", collection, err)
		}
		buffers[collection] = nil
		return nil
	}
	if err := walkFixtures(m, func(collection string, doc bson.M) error {
		buffers[collection] = append(buffers[collection], doc["_id"])
		if len(buffers[collection]) == fixtureBatchSize {
			return flush(collection)
		}
		return nil
	}); err != nil {
		return err
	}
	for collection := range buffers {
		if err := flush(collection); err != nil {
			return err
		}
	}
	return nil
}

func (s *mongoStore) Cleanup(ctx context.Context, requested Manifest) error {
	m, err := s.Load(ctx, requested.RunID)
	if err != nil {
		return err
	}
	if m.State != "finished" || m.Report == nil || !m.Report.Passed {
		return errors.New("cleanup requires a successfully finished run; active, unrun and failed runs are retained for diagnosis")
	}
	if err := s.acquireRunLock(ctx, m.RunID, "cleaning"); err != nil {
		return err
	}
	if err := s.transition(ctx, m.RunID, []string{"finished"}, bson.M{"state": "cleaning"}); err != nil {
		return err
	}
	if err := s.cleanupResources(ctx, m); err != nil {
		return s.recordFailure(ctx, m.RunID, "cleaning", "cleanup_failed", "cleanupError", err)
	}
	if err := s.releaseRunLock(ctx, m.RunID, false); err != nil {
		return s.recordFailure(ctx, m.RunID, "cleaning", "cleanup_failed", "cleanupError", err)
	}
	if err := s.transition(ctx, m.RunID, []string{"cleaning"}, bson.M{"state": "cleaned"}); err != nil {
		return s.recordFailure(ctx, m.RunID, "cleaning", "cleanup_failed", "cleanupError", err)
	}
	return nil
}

func (s *mongoStore) Close(ctx context.Context) error {
	return s.operation(ctx, s.client.Disconnect)
}
