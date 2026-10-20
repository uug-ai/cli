package loadtest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func mockStore(mt *mtest.T) *mongoStore {
	cfg := Config{
		RunID: "fixture-test", Database: "loadtest_mock", OperationTimeout: time.Second, Execute: true, ConfirmTestTarget: true,
	}
	return &mongoStore{client: mt.Client, db: mt.Client.Database(cfg.Database), cfg: cfg}
}

func mockDocument(t testing.TB, value any) bson.D {
	t.Helper()
	data, err := bson.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var doc bson.D
	if err := bson.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

func mockCursor(collection string, docs ...bson.D) bson.D {
	return mtest.CreateCursorResponse(0, "loadtest_mock."+collection, mtest.FirstBatch, docs...)
}

func mockGuard() bson.D {
	return mockCursor(sentinelCollection, bson.D{
		{Key: "_id", Value: "database"}, {Key: "schema", Value: Schema}, {Key: "database", Value: "loadtest_mock"},
	})
}

func mockUpdate(n int) bson.D {
	return mtest.CreateSuccessResponse(bson.E{Key: "n", Value: n}, bson.E{Key: "nModified", Value: n})
}

func mockRunLock(runID string) bson.D {
	return mockCursor(sentinelCollection, bson.D{
		{Key: "_id", Value: "active-run"}, {Key: "schema", Value: Schema}, {Key: "runId", Value: runID},
	})
}

func TestMongoTargetGuardWithoutCLI(t *testing.T) {
	for _, cfg := range []Config{
		{Database: "production", OperationTimeout: time.Second, Execute: true, ConfirmTestTarget: true},
		{Database: "loadtest_", OperationTimeout: time.Second, Execute: true, ConfirmTestTarget: true},
		{Database: "loadtest_bad.name", OperationTimeout: time.Second, Execute: true, ConfirmTestTarget: true},
		{Database: "loadtest_safe", OperationTimeout: 0, Execute: true, ConfirmTestTarget: true},
		{Database: "loadtest_safe", OperationTimeout: time.Second},
		{Database: "loadtest_safe", OperationTimeout: time.Second, ConfirmTestTarget: true},
		{Database: "Kerberos", OperationTimeout: time.Second, Execute: true, ConfirmTestTarget: true},
		{Database: "Kerberos", OperationTimeout: time.Second, Execute: true, AllowDefaultNames: true},
		{Database: "production", OperationTimeout: time.Second, Execute: true, ConfirmTestTarget: true, AllowDefaultNames: true},
		{Database: "admin", OperationTimeout: time.Second, Execute: true, ConfirmTestTarget: true, AllowDefaultNames: true},
		{Database: "config", OperationTimeout: time.Second, Execute: true, ConfirmTestTarget: true, AllowDefaultNames: true},
		{Database: "local", OperationTimeout: time.Second, Execute: true, ConfirmTestTarget: true, AllowDefaultNames: true},
	} {
		if _, err := NewMongoStore(context.Background(), cfg); err == nil {
			t.Fatalf("unsafe constructor accepted: %+v", cfg)
		}
		if err := validateMongoTarget(Config{
			Database: "Kerberos", OperationTimeout: time.Second, Execute: true, ConfirmTestTarget: true, AllowDefaultNames: true,
		}); err != nil {
			t.Fatalf("explicit isolated legacy target rejected: %v", err)
		}
		store := &mongoStore{cfg: cfg}
		if err := store.guard(context.Background(), true); err == nil {
			t.Fatalf("unsafe method accepted: %+v", cfg)
		}
	}
}

func TestMongoOperationsHaveBoundedContexts(t *testing.T) {
	s := &mongoStore{cfg: Config{OperationTimeout: time.Second}}
	if err := s.operation(context.Background(), func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > time.Second {
			t.Fatal("missing operation deadline")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(50*time.Millisecond))
	defer cancel()
	deadline, _ := ctx.Deadline()
	_ = s.operation(ctx, func(op context.Context) error {
		got, ok := op.Deadline()
		if !ok || got != deadline {
			t.Fatal("operation extended caller deadline")
		}
		return nil
	})
}

func TestMongoSentinelGuards(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("existing owned database", func(mt *mtest.T) {
		s := mockStore(mt)
		mt.AddMockResponses(mockGuard())
		if err := s.guard(context.Background(), false); err != nil {
			mt.Fatal(err)
		}
		if len(mt.GetAllStartedEvents()) != 1 {
			mt.Fatal("existing sentinel should require only one read")
		}
	})
	mt.Run("fresh empty database", func(mt *mtest.T) {
		s := mockStore(mt)
		mt.AddMockResponses(mockCursor(sentinelCollection), mockCursor("$cmd.listCollections"), mtest.CreateSuccessResponse())
		if err := s.guard(context.Background(), true); err != nil {
			mt.Fatal(err)
		}
		events := mt.GetAllStartedEvents()
		if len(events) != 3 || events[1].CommandName != "listCollections" || events[2].CommandName != "insert" {
			mt.Fatalf("expected emptiness check before sentinel insertion: %v", events)
		}
	})
	mt.Run("nonempty database never adopted", func(mt *mtest.T) {
		s := mockStore(mt)
		mt.AddMockResponses(mockCursor(sentinelCollection),
			mockCursor("$cmd.listCollections", bson.D{{Key: "name", Value: "users"}}))
		if err := s.guard(context.Background(), true); err == nil || !strings.Contains(err.Error(), "nonempty") {
			mt.Fatalf("arbitrary test-named database adopted: %v", err)
		}
		for _, event := range mt.GetAllStartedEvents() {
			if event.CommandName == "insert" {
				mt.Fatal("unowned database mutated")
			}
		}
	})
	mt.Run("invalid sentinel", func(mt *mtest.T) {
		s := mockStore(mt)
		mt.AddMockResponses(mockCursor(sentinelCollection, bson.D{
			{Key: "_id", Value: "database"}, {Key: "schema", Value: Schema}, {Key: "database", Value: "different"},
		}))
		if err := s.guard(context.Background(), true); err == nil {
			mt.Fatal("invalid sentinel accepted")
		}
	})
	mt.Run("read error is not absence", func(mt *mtest.T) {
		s := mockStore(mt)
		mt.AddMockResponses(mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 13, Message: "not authorized"}))
		if err := s.guard(context.Background(), true); err == nil || !strings.Contains(err.Error(), "not authorized") {
			mt.Fatalf("sentinel error swallowed: %v", err)
		}
		if len(mt.GetAllStartedEvents()) != 1 {
			mt.Fatal("sentinel read error must not cause writes")
		}
	})
	mt.Run("missing sentinel cannot be initialized by readers", func(mt *mtest.T) {
		s := mockStore(mt)
		mt.AddMockResponses(mockCursor(sentinelCollection))
		if _, err := s.Load(context.Background(), "fixture-test"); err == nil {
			mt.Fatal("reader accepted unowned database")
		}
		if len(mt.GetAllStartedEvents()) != 1 {
			mt.Fatal("reader must not initialize sentinel")
		}
	})
	mt.Run("legacy default exception still rejects nonempty unowned database", func(mt *mtest.T) {
		s := mockStore(mt)
		s.cfg.Database, s.cfg.AllowDefaultNames = "Kerberos", true
		s.db = mt.Client.Database("Kerberos")
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, "Kerberos."+sentinelCollection, mtest.FirstBatch),
			mtest.CreateCursorResponse(0, "Kerberos.$cmd.listCollections", mtest.FirstBatch,
				bson.D{{Key: "name", Value: "users"}}))
		if err := s.guard(context.Background(), true); err == nil || !strings.Contains(err.Error(), "nonempty") {
			mt.Fatalf("legacy override adopted nonempty database: %v", err)
		}
		for _, event := range mt.GetAllStartedEvents() {
			if event.CommandName != "find" && event.CommandName != "listCollections" {
				mt.Fatalf("unowned legacy target was mutated: %s", event.Command)
			}
			if event.DatabaseName != "Kerberos" {
				mt.Fatalf("guard accessed another database: %s", event.DatabaseName)
			}
		}
	})
	mt.Run("legacy default exception accepts its matching sentinel", func(mt *mtest.T) {
		s := mockStore(mt)
		s.cfg.Database, s.cfg.AllowDefaultNames = "Kerberos", true
		s.db = mt.Client.Database("Kerberos")
		mt.AddMockResponses(mtest.CreateCursorResponse(0, "Kerberos."+sentinelCollection, mtest.FirstBatch, bson.D{
			{Key: "_id", Value: "database"}, {Key: "schema", Value: Schema}, {Key: "database", Value: "Kerberos"},
		}))
		if err := s.guard(context.Background(), false); err != nil {
			mt.Fatalf("owned isolated legacy target rejected: %v", err)
		}
	})
}

func TestMongoPreparePreservesDiagnosticsAndSettings(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("duplicate run cannot reset manifest", func(mt *mtest.T) {
		s := mockStore(mt)
		mt.AddMockResponses(mockGuard(), mtest.CreateSuccessResponse(),
			mtest.CreateWriteErrorsResponse(mtest.WriteError{Index: 0, Code: 11000, Message: "duplicate key"}),
			mtest.CreateSuccessResponse(bson.E{Key: "n", Value: 1}))
		if err := s.Prepare(context.Background(), fixtureManifest()); err == nil {
			mt.Fatal("duplicate run accepted")
		}
		events := mt.GetAllStartedEvents()
		if len(events) != 4 || events[2].CommandName != "insert" || events[3].CommandName != "delete" {
			mt.Fatal("duplicate run must not modify existing manifest or fixtures")
		}
	})
	mt.Run("unrelated settings preserved and failure retained", func(mt *mtest.T) {
		s := mockStore(mt)
		mt.AddMockResponses(mockGuard(), mtest.CreateSuccessResponse(), mtest.CreateSuccessResponse(),
			mockCursor("settings", bson.D{{Key: "_id", Value: "unrelated"}, {Key: "key", Value: "plan"}}), mockUpdate(1))
		if err := s.Prepare(context.Background(), fixtureManifest()); err == nil || !strings.Contains(err.Error(), "refusing to overwrite") {
			mt.Fatalf("arbitrary settings were accepted: %v", err)
		}
		events := mt.GetAllStartedEvents()
		last := events[len(events)-1]
		if last.CommandName != "update" || rawString(last.Command, "update") != manifestCollection ||
			!strings.Contains(last.Command.String(), "preparationError") || !strings.Contains(last.Command.String(), "failed") {
			mt.Fatalf("preparation failure not retained: %v", last.Command)
		}
		for _, event := range events {
			if event.CommandName == "update" && rawString(event.Command, "update") == "settings" {
				mt.Fatal("existing settings overwritten")
			}
		}
	})
	mt.Run("success initializes fixtures and prepared state", func(mt *mtest.T) {
		s := mockStore(mt)
		m := fixtureManifest()
		mt.AddMockResponses(mockGuard(), mtest.CreateSuccessResponse(), mtest.CreateSuccessResponse(),
			mockCursor("settings", mockDocument(mt, planFixture())))
		// Eight indexes, six fixture collection batches, one state update.
		for i := 0; i < 8+6; i++ {
			mt.AddMockResponses(mtest.CreateSuccessResponse())
		}
		mt.AddMockResponses(mockUpdate(1), mtest.CreateSuccessResponse(bson.E{Key: "n", Value: 1}))
		if err := s.Prepare(context.Background(), m); err != nil {
			mt.Fatal(err)
		}
		events := mt.GetAllStartedEvents()
		for _, event := range events {
			if event.CommandName == "insert" && rawString(event.Command, "insert") == "media" {
				var command struct {
					Documents []bson.M `bson:"documents"`
				}
				if err := bson.Unmarshal(event.Command, &command); err != nil {
					mt.Fatal(err)
				}
				if len(command.Documents) != fixtureDeviceCount(m)*m.HistoryPerDevice {
					mt.Fatalf("incorrect history count: %d", len(command.Documents))
				}
			}
		}
		last := events[len(events)-2]
		if last.CommandName != "update" || !strings.Contains(last.Command.String(), "prepared") {
			mt.Fatalf("prepare not finalized: %v", last.Command)
		}
	})
	mt.Run("index failure is retained before fixture writes", func(mt *mtest.T) {
		s := mockStore(mt)
		mt.AddMockResponses(mockGuard(), mtest.CreateSuccessResponse(), mtest.CreateSuccessResponse(),
			mockCursor("settings", mockDocument(mt, planFixture())),
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 13, Message: "index denied"}), mockUpdate(1))
		if err := s.Prepare(context.Background(), fixtureManifest()); err == nil || !strings.Contains(err.Error(), "index denied") {
			mt.Fatalf("index failure ignored: %v", err)
		}
		for _, event := range mt.GetAllStartedEvents() {
			if event.CommandName == "insert" && rawString(event.Command, "insert") != manifestCollection && rawString(event.Command, "insert") != sentinelCollection {
				mt.Fatalf("fixture inserted before indexes completed: %s", event.Command)
			}
		}
	})
}

func TestMongoClaimIsOneShotAndReportFinalizes(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("atomic prepared compare and swap", func(mt *mtest.T) {
		s := mockStore(mt)
		m := fixtureManifest()
		mt.AddMockResponses(mockGuard(), mockCursor(manifestCollection, mockDocument(mt, m)), mtest.CreateSuccessResponse(), mockUpdate(1))
		if err := s.Claim(context.Background(), m.RunID); err != nil {
			mt.Fatal(err)
		}
		m.State = "running"
		mt.AddMockResponses(mockGuard(), mockCursor(manifestCollection, mockDocument(mt, m)))
		if err := s.Claim(context.Background(), m.RunID); err == nil {
			mt.Fatal("run claimed twice")
		}
		for _, event := range mt.GetAllStartedEvents() {
			if event.CommandName != "update" {
				continue
			}
			if !strings.Contains(event.Command.String(), `"state": {"$in": ["prepared"]}`) ||
				!strings.Contains(event.Command.String(), `"state": "running"`) {
				mt.Fatalf("claim lacks state CAS: %s", event.Command)
			}
		}
	})
	for _, passed := range []bool{false, true} {
		name := map[bool]string{false: "failed", true: "finished"}[passed]
		mt.Run(name, func(mt *mtest.T) {
			s := mockStore(mt)
			m := fixtureManifest()
			m.State = "running"
			mt.AddMockResponses(mockGuard(), mockCursor(manifestCollection, mockDocument(mt, m)), mockRunLock(m.RunID), mockUpdate(1))
			if passed {
				mt.AddMockResponses(mtest.CreateSuccessResponse(bson.E{Key: "n", Value: 1}))
			}
			if err := s.SaveReport(context.Background(), Report{Schema: Schema, RunID: m.RunID, Passed: passed}); err != nil {
				mt.Fatal(err)
			}
			events := mt.GetAllStartedEvents()
			command := events[3].Command.String()
			if !strings.Contains(command, `"state": "`+name+`"`) || !strings.Contains(command, `"report":`) {
				mt.Fatalf("report not atomically finalized: %s", command)
			}
			deletedLock := false
			for _, event := range events {
				if event.CommandName == "delete" {
					deletedLock = true
					if !strings.Contains(event.Command.String(), `"active-run"`) || !strings.Contains(event.Command.String(), `"runId": "fixture-test"`) {
						mt.Fatalf("lock release is not owner scoped: %s", event.Command)
					}
				}
			}
			if deletedLock != passed {
				mt.Fatal("only successful runs may release the database lock")
			}
		})
	}
	mt.Run("different runs contend on one fixed database lock", func(mt *mtest.T) {
		s := mockStore(mt)
		m := fixtureManifest()
		mt.AddMockResponses(mockGuard(), mockCursor(manifestCollection, mockDocument(mt, m)),
			mtest.CreateWriteErrorsResponse(mtest.WriteError{Index: 0, Code: 11000, Message: "active run exists"}))
		if err := s.Claim(context.Background(), m.RunID); err == nil || !strings.Contains(err.Error(), "active-run lock") {
			mt.Fatalf("concurrent database run accepted: %v", err)
		}
		events := mt.GetAllStartedEvents()
		if len(events) != 3 || events[2].CommandName != "insert" ||
			!strings.Contains(events[2].Command.String(), `"active-run"`) {
			mt.Fatal("claim did not use the unique fixed database lock")
		}
	})
	mt.Run("ambiguous state transition retains database lock", func(mt *mtest.T) {
		s := mockStore(mt)
		m := fixtureManifest()
		mt.AddMockResponses(mockGuard(), mockCursor(manifestCollection, mockDocument(mt, m)),
			mtest.CreateSuccessResponse(),
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 13, Message: "transition denied"}))
		if err := s.Claim(context.Background(), m.RunID); err == nil || !strings.Contains(err.Error(), "retained") {
			mt.Fatalf("ambiguous claim not surfaced: %v", err)
		}
		for _, event := range mt.GetAllStartedEvents() {
			if event.CommandName == "delete" {
				mt.Fatal("ambiguous claim must retain database lock")
			}
		}
	})
	mt.Run("report cannot finalize without its own lock", func(mt *mtest.T) {
		s := mockStore(mt)
		m := fixtureManifest()
		m.State = "running"
		mt.AddMockResponses(mockGuard(), mockCursor(manifestCollection, mockDocument(mt, m)), mockCursor(sentinelCollection))
		if err := s.SaveReport(context.Background(), Report{Schema: Schema, RunID: m.RunID, Passed: true}); err == nil {
			mt.Fatal("report finalized without owning the database lock")
		}
		if len(mt.GetAllStartedEvents()) != 3 {
			mt.Fatal("missing lock must prevent report writes")
		}
	})
}

func mediaFixture(m Manifest, index int) bson.M {
	identity, _ := ExpectedIdentity(m, index)
	return bson.M{
		"_id": primitive.NewObjectID(), "videoFile": RecordingKey(m, index),
		"organisationId": identity.OrganisationID.Hex(), "projectId": identity.ProjectID,
		"deviceId": identity.DeviceKey, "deviceKey": identity.DeviceKey,
		"duration": 30000, "startTimestamp": m.Timestamp, "endTimestamp": m.Timestamp + 30,
		"metadata": bson.M{"fps": 25.0, "fileSize": int64(1024 * 1024)},
	}
}

func TestVerifyMediaCountsUniquenessScopeAndMetadata(t *testing.T) {
	m := fixtureManifest()
	m.Total = 4
	first := mediaFixture(m, 0)
	wrongScope := mediaFixture(m, 1)
	wrongScope["projectId"] = identityForDevice(m, 1).ProjectID.Hex()
	wrongMetadata := mediaFixture(m, 2)
	wrongMetadata["metadata"] = bson.M{"fps": 0, "fileSize": int64(1024 * 1024)}
	var rows []bson.Raw
	for _, document := range []bson.M{first, first, wrongScope, wrongMetadata} {
		raw, err := bson.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, raw)
	}
	result := Verification{Expected: m.Total}
	verifyMediaRows(m, 0, m.Total, rows, &result)
	if result.Found != 3 || result.Duplicates != 1 || result.WrongScope != 1 || result.WrongMetadata != 1 {
		t.Fatalf("wrong verification counts: %+v", result)
	}
}

func TestMongoVerifyUsesBatchedProjectedReads(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("legacy owner calendar and missing media", func(mt *mtest.T) {
		s := mockStore(mt)
		m := fixtureManifest()
		m.Legacy, m.Organisations, m.ProjectsPerOrganisation, m.DevicesPerProject = true, 1, 1, 1
		m.Total = fixtureBatchSize + 1
		identity := identityForDevice(m, 0)
		mt.AddMockResponses(mockGuard(), mockCursor(manifestCollection, mockDocument(mt, m)),
			mockCursor("media", mockDocument(mt, mediaFixture(m, 0))), mockCursor("media"),
			mockCursor("users", bson.D{{Key: "_id", Value: identity.OrganisationID}, {Key: "dates", Value: bson.A{"18-09-2026"}}}))
		result, err := s.Verify(context.Background(), m)
		if err != nil {
			mt.Fatal(err)
		}
		if result.Expected != 257 || result.Found != 1 || result.Missing != 256 || result.MissingDates != 0 {
			mt.Fatalf("wrong verification result: %+v", result)
		}
		findCount := 0
		for _, event := range mt.GetAllStartedEvents() {
			if event.CommandName != "find" || rawString(event.Command, "find") != "media" {
				continue
			}
			findCount++
			if event.Command.Lookup("projection").Type == 0 || event.Command.Lookup("filter", "videoFile", "$in").Type == 0 {
				mt.Fatalf("verification requires indexed batches and projection: %s", event.Command)
			}
		}
		if findCount != 2 {
			mt.Fatalf("expected 2 media batch reads, got %d", findCount)
		}
	})
	mt.Run("read errors are surfaced", func(mt *mtest.T) {
		s := mockStore(mt)
		m := fixtureManifest()
		mt.AddMockResponses(mockGuard(), mockCursor(manifestCollection, mockDocument(mt, m)),
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 13, Message: "verification denied"}))
		if _, err := s.Verify(context.Background(), m); err == nil || !strings.Contains(err.Error(), "verification denied") {
			mt.Fatalf("verification error swallowed: %v", err)
		}
	})
	mt.Run("canonical project and device calendars", func(mt *mtest.T) {
		s := mockStore(mt)
		m := fixtureManifest()
		m.Organisations, m.ProjectsPerOrganisation, m.DevicesPerProject, m.Total = 1, 1, 1, 1
		identity := identityForDevice(m, 0)
		mt.AddMockResponses(mockGuard(), mockCursor(manifestCollection, mockDocument(mt, m)),
			mockCursor("media", mockDocument(mt, mediaFixture(m, 0))),
			mockCursor("project", bson.D{
				{Key: "_id", Value: identity.ProjectID}, {Key: "organisationId", Value: identity.OrganisationID},
				{Key: "dates", Value: bson.A{"18-09-2026"}},
			}),
			mockCursor("project_device_days", bson.D{
				{Key: "_id", Value: deviceDaysID(identity)}, {Key: "organisationId", Value: identity.OrganisationID},
				{Key: "projectId", Value: identity.ProjectID}, {Key: "dates", Value: bson.A{"18-09-2026"}},
			}))
		result, err := s.Verify(context.Background(), m)
		if err != nil || result.Found != 1 || result.Missing != 0 || result.MissingDates != 0 {
			mt.Fatalf("canonical dates not verified: %+v / %v", result, err)
		}
	})
	mt.Run("wrong calendar scope and missing day count as missing", func(mt *mtest.T) {
		s := mockStore(mt)
		m := fixtureManifest()
		m.Organisations, m.ProjectsPerOrganisation, m.DevicesPerProject, m.Total = 1, 1, 1, 1
		identity := identityForDevice(m, 0)
		mt.AddMockResponses(
			mockCursor("project", bson.D{
				{Key: "_id", Value: identity.ProjectID}, {Key: "organisationId", Value: identity.OrganisationID},
				{Key: "dates", Value: bson.A{"17-09-2026"}},
			}),
			mockCursor("project_device_days", bson.D{
				{Key: "_id", Value: deviceDaysID(identity)}, {Key: "organisationId", Value: identity.OrganisationID},
				{Key: "projectId", Value: identity.ProjectID.Hex()}, {Key: "dates", Value: bson.A{"18-09-2026"}},
			}))
		missing, err := s.verifyDates(context.Background(), m)
		if err != nil || missing != 2 {
			mt.Fatalf("invalid dates not detected: %d / %v", missing, err)
		}
	})
}

func TestMongoCleanupRefusesUnsafeStatesAndScopesDeletes(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	for _, state := range []string{"preparing", "prepared", "running", "failed", "cleaning", "cleanup_failed", "cleaned"} {
		mt.Run(state, func(mt *mtest.T) {
			s := mockStore(mt)
			m := fixtureManifest()
			m.State, m.Report = state, &Report{Passed: true}
			mt.AddMockResponses(mockGuard(), mockCursor(manifestCollection, mockDocument(mt, m)))
			if err := s.Cleanup(context.Background(), m); err == nil {
				mt.Fatalf("unsafe cleanup of state %s accepted", state)
			}
			if len(mt.GetAllStartedEvents()) != 2 {
				mt.Fatal("unsafe cleanup must be read-only")
			}
		})
	}
	mt.Run("finished requires a passed report", func(mt *mtest.T) {
		s := mockStore(mt)
		m := fixtureManifest()
		m.State, m.Report = "finished", &Report{Passed: false}
		mt.AddMockResponses(mockGuard(), mockCursor(manifestCollection, mockDocument(mt, m)))
		if err := s.Cleanup(context.Background(), m); err == nil {
			mt.Fatal("failed report accepted for cleanup")
		}
	})
	mt.Run("only exact successful run resources", func(mt *mtest.T) {
		s := mockStore(mt)
		m := fixtureManifest()
		m.State, m.Report = "finished", &Report{Passed: true}
		mt.AddMockResponses(mockGuard(), mockCursor(manifestCollection, mockDocument(mt, m)), mtest.CreateSuccessResponse(), mockUpdate(1))
		// Runtime media + device dates + six tagged fixture collections + an
		// owner-scoped retry of a successful run's lock release.
		for i := 0; i < 9; i++ {
			mt.AddMockResponses(mtest.CreateSuccessResponse(bson.E{Key: "n", Value: 1}))
		}
		mt.AddMockResponses(mockUpdate(1))
		if err := s.Cleanup(context.Background(), m); err != nil {
			mt.Fatal(err)
		}
		deletes := 0
		for _, event := range mt.GetAllStartedEvents() {
			if strings.HasPrefix(event.CommandName, "drop") {
				mt.Fatal("cleanup must never drop a collection or database")
			}
			if event.CommandName != "delete" {
				continue
			}
			deletes++
			collection := rawString(event.Command, "delete")
			if collection == "settings" || collection == manifestCollection {
				mt.Fatalf("persistent harness state deleted: %s", event.Command)
			}
			command := event.Command.String()
			if collection == sentinelCollection {
				if !strings.Contains(command, `"active-run"`) || !strings.Contains(command, `"runId": "fixture-test"`) {
					mt.Fatalf("cleanup touched the database sentinel or another lock: %s", command)
				}
				continue
			}
			if !strings.Contains(command, "loadtestRunID") && !strings.Contains(command, "organisationId") {
				mt.Fatalf("delete is not run scoped: %s", command)
			}
			if !strings.Contains(command, `"_id"`) && !strings.Contains(command, `"videoFile"`) {
				mt.Fatalf("delete lacks exact resource identity: %s", command)
			}
		}
		if deletes != 9 {
			mt.Fatalf("wrong cleanup operation count: %d", deletes)
		}
	})
	mt.Run("delete errors preserve retryable diagnostic", func(mt *mtest.T) {
		s := mockStore(mt)
		m := fixtureManifest()
		m.State, m.Report = "finished", &Report{Passed: true}
		mt.AddMockResponses(mockGuard(), mockCursor(manifestCollection, mockDocument(mt, m)), mtest.CreateSuccessResponse(), mockUpdate(1),
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 13, Message: "delete denied"}), mockUpdate(1))
		if err := s.Cleanup(context.Background(), m); err == nil || !strings.Contains(err.Error(), "delete denied") {
			mt.Fatalf("cleanup error swallowed: %v", err)
		}
		events := mt.GetAllStartedEvents()
		last := events[len(events)-1].Command.String()
		if !strings.Contains(last, "cleanup_failed") || !strings.Contains(last, "cleanupError") {
			mt.Fatalf("cleanup failure not retained: %s", last)
		}
	})
}

func TestMongoPlanFixtureNeverUpdatesExistingSettings(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("exact shared fixture reused", func(mt *mtest.T) {
		s := mockStore(mt)
		mt.AddMockResponses(mockCursor("settings", mockDocument(mt, planFixture())))
		if err := s.ensurePlan(context.Background()); err != nil {
			mt.Fatal(err)
		}

		if len(mt.GetAllStartedEvents()) != 1 {
			mt.Fatal("unchanged plan fixture should not be written")
		}
	})
	mt.Run("missing document creates insert only", func(mt *mtest.T) {
		s := mockStore(mt)
		mt.AddMockResponses(mockCursor("settings"), mtest.CreateSuccessResponse())
		if err := s.ensurePlan(context.Background()); err != nil {
			mt.Fatal(err)
		}
		events := mt.GetAllStartedEvents()
		if len(events) != 2 || events[1].CommandName != "insert" {
			mt.Fatal("settings must never be replaced/upserted")
		}
	})
	mt.Run("read failure does not cause replacement", func(mt *mtest.T) {
		s := mockStore(mt)
		mt.AddMockResponses(mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 13, Message: "denied"}))
		if err := s.ensurePlan(context.Background()); err == nil || errors.Is(err, mongo.ErrNoDocuments) {
			mt.Fatalf("read failure mistaken for missing settings: %v", err)
		}
		if len(mt.GetAllStartedEvents()) != 1 {
			mt.Fatal("plan read failure caused a write")
		}
	})
}
