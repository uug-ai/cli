package actions

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

// DocumentDB gives no cross-collection transaction, so an ambiguous write must
// be settled by reading the deterministic identity back rather than retried.
func TestProjectsBootstrapAmbiguousDefaultProjectUpsertReconciles(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("committed default project", func(mt *mtest.T) {
		organisationID := primitive.NewObjectID()
		ownerID := primitive.NewObjectID()
		now := time.Date(2026, time.August, 6, 12, 0, 0, 0, time.UTC)
		namespace := mt.DB.Name() + "." + projectsBootstrapCollection
		mt.AddMockResponses(
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 123, Message: "ambiguous update result"}),
			mtest.CreateCursorResponse(0, namespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: organisationID},
				{Key: "organisationId", Value: organisationID},
				{Key: "name", Value: projectsBootstrapDefaultName},
				{Key: "slug", Value: projectsBootstrapDefaultSlug},
				{Key: "isActive", Value: true},
				{Key: "audit", Value: bson.D{
					{Key: "createdBy", Value: ownerID.Hex()},
					{Key: "createdAt", Value: now},
					{Key: "updatedBy", Value: ownerID.Hex()},
					{Key: "updatedAt", Value: now},
					{Key: "lastAction", Value: projectsBootstrapLastAction},
				}},
			}),
		)
		report := projectsBootstrapReport{}
		runner := projectsBootstrapRunner{
			config:   ProjectsBootstrapConfig{Mode: "live"},
			database: mt.DB,
			now:      now,
			report:   &report,
		}

		ok, changed, err := runner.createDefaultProject(context.Background(), organisationID, ownerID, ownerID, now)
		if err != nil {
			t.Fatalf("createDefaultProject() error = %v", err)
		}
		if !ok || !changed {
			t.Fatalf("createDefaultProject() = (%v, %v), want (true, true)", ok, changed)
		}
		if report.Writes.Reconciled != 1 || report.Writes.Failed != 0 || report.Projects.Reconciled != 1 {
			t.Fatalf("write counts = %+v, project counts = %+v", report.Writes, report.Projects)
		}
		if runner.hasConflict {
			t.Fatal("a reconciled write must not raise a conflict")
		}
	})
}

func TestProjectsBootstrapAmbiguousUpsertLandingOnAnotherIdentityBlocks(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("foreign default project", func(mt *mtest.T) {
		organisationID := primitive.NewObjectID()
		ownerID := primitive.NewObjectID()
		now := time.Date(2026, time.August, 6, 12, 0, 0, 0, time.UTC)
		namespace := mt.DB.Name() + "." + projectsBootstrapCollection
		mt.AddMockResponses(
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 123, Message: "ambiguous update result"}),
			mtest.CreateCursorResponse(0, namespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: organisationID},
				{Key: "organisationId", Value: primitive.NewObjectID()},
				{Key: "slug", Value: projectsBootstrapDefaultSlug},
			}),
		)
		report := projectsBootstrapReport{}
		runner := projectsBootstrapRunner{
			config:   ProjectsBootstrapConfig{Mode: "live"},
			database: mt.DB,
			now:      now,
			report:   &report,
		}

		ok, _, err := runner.createDefaultProject(context.Background(), organisationID, ownerID, ownerID, now)
		if err != nil {
			t.Fatalf("createDefaultProject() error = %v", err)
		}
		if ok || !runner.hasConflict || runner.blockingConflictCount != 1 {
			t.Fatalf("ok = %v, hasConflict = %v, blocking = %d", ok, runner.hasConflict, runner.blockingConflictCount)
		}
		if report.Conflicts[0].Code != "default-project-reconcile-failed" || report.Writes.Failed != 1 {
			t.Fatalf("conflicts = %+v, writes = %+v", report.Conflicts, report.Writes)
		}
	})
}

func TestProjectsBootstrapAmbiguousUserSelectionReconciles(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("committed selection", func(mt *mtest.T) {
		userID := primitive.NewObjectID()
		targetID := primitive.NewObjectID()
		namespace := mt.DB.Name() + ".users"
		mt.AddMockResponses(
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 123, Message: "ambiguous update result"}),
			mtest.CreateCursorResponse(0, namespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: userID},
				{Key: "organisationId", Value: targetID},
				{Key: "projectId", Value: targetID},
			}),
		)
		report := projectsBootstrapReport{}
		runner := projectsBootstrapRunner{
			config:   ProjectsBootstrapConfig{Mode: "live"},
			database: mt.DB,
			report:   &report,
		}
		user := projectsBootstrapUser{organisationsBootstrapUser: organisationsBootstrapUser{ID: userID, Selection: targetID, SelectionState: organisationsBootstrapFieldValue}}

		ok, changed, err := runner.ensureUserProjectSelection(context.Background(), user, targetID, userID)
		if err != nil {
			t.Fatalf("ensureUserProjectSelection() error = %v", err)
		}
		if !ok || !changed {
			t.Fatalf("ensureUserProjectSelection() = (%v, %v), want (true, true)", ok, changed)
		}
		if report.Writes.Attempted != 1 || report.Writes.Reconciled != 1 || report.Writes.Failed != 0 {
			t.Fatalf("write counts = %+v", report.Writes)
		}
	})
}

func TestProjectsBootstrapAmbiguousUserSelectionConflictBlocks(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("selection landed elsewhere", func(mt *mtest.T) {
		userID := primitive.NewObjectID()
		targetID := primitive.NewObjectID()
		namespace := mt.DB.Name() + ".users"
		mt.AddMockResponses(
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 123, Message: "ambiguous update result"}),
			mtest.CreateCursorResponse(0, namespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: userID},
				{Key: "projectId", Value: primitive.NewObjectID()},
			}),
		)
		report := projectsBootstrapReport{}
		runner := projectsBootstrapRunner{
			config:   ProjectsBootstrapConfig{Mode: "live"},
			database: mt.DB,
			report:   &report,
		}
		user := projectsBootstrapUser{organisationsBootstrapUser: organisationsBootstrapUser{ID: userID, Selection: targetID, SelectionState: organisationsBootstrapFieldValue}}

		ok, _, err := runner.ensureUserProjectSelection(context.Background(), user, targetID, userID)
		if err != nil {
			t.Fatalf("ensureUserProjectSelection() error = %v", err)
		}
		if ok || runner.blockingConflictCount != 1 {
			t.Fatalf("ok = %v, blocking conflicts = %d", ok, runner.blockingConflictCount)
		}
		if report.Conflicts[0].Code != "user-project-selection-reconcile-failed" || report.Writes.Failed != 1 {
			t.Fatalf("conflicts = %+v, writes = %+v", report.Conflicts, report.Writes)
		}
	})
}

// A default project minted with a random _id is exactly the Phase 3g case that
// must fail closed: Hub API resolves the default by {organisationId, slug} and
// requires a single match, so this tool refuses to write anything for the tenant.
func TestProjectsBootstrapLegacyDefaultProjectBlocksTheTenant(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("random id default", func(mt *mtest.T) {
		organisationID := primitive.NewObjectID()
		ownerID := primitive.NewObjectID()
		legacyID := primitive.NewObjectID()
		now := time.Date(2026, time.August, 6, 12, 0, 0, 0, time.UTC)
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, mt.DB.Name()+".organisation", mtest.FirstBatch, bson.D{
				{Key: "_id", Value: organisationID},
				{Key: "ownerId", Value: ownerID},
				{Key: "audit", Value: bson.D{{Key: "createdAt", Value: now}}},
			}),
			mtest.CreateCursorResponse(0, mt.DB.Name()+"."+projectsBootstrapCollection, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: legacyID},
			}),
		)
		report := projectsBootstrapReport{}
		runner := projectsBootstrapRunner{
			config:   ProjectsBootstrapConfig{Mode: "live"},
			database: mt.DB,
			now:      now,
			report:   &report,
		}

		ok, changed, err := runner.ensureDefaultProject(context.Background(), organisationID, ownerID)
		if err != nil {
			t.Fatalf("ensureDefaultProject() error = %v", err)
		}
		if ok || changed {
			t.Fatalf("ensureDefaultProject() = (%v, %v), want (false, false)", ok, changed)
		}
		if report.Projects.LegacyDefaults != 1 || report.Conflicts[0].Code != "default-project-conflict" {
			t.Fatalf("projects = %+v, conflicts = %+v", report.Projects, report.Conflicts)
		}
		if report.Conflicts[0].DocumentID != legacyID.Hex() {
			t.Fatalf("conflict documentId = %q, want the legacy project id", report.Conflicts[0].DocumentID)
		}
		if report.Writes.Attempted != 0 {
			t.Fatalf("a blocked tenant must not attempt a write: %+v", report.Writes)
		}
		for _, event := range mt.GetAllStartedEvents() {
			if event.CommandName == "update" || event.CommandName == "insert" || event.CommandName == "delete" {
				t.Fatalf("blocked tenant issued a %s command", event.CommandName)
			}
		}
	})
}

func TestProjectsBootstrapExistingDefaultProjectIsIdempotent(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("complete default project", func(mt *mtest.T) {
		organisationID := primitive.NewObjectID()
		ownerID := primitive.NewObjectID()
		now := time.Date(2026, time.August, 6, 12, 0, 0, 0, time.UTC)
		projectNamespace := mt.DB.Name() + "." + projectsBootstrapCollection
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, mt.DB.Name()+".organisation", mtest.FirstBatch, bson.D{
				{Key: "_id", Value: organisationID},
				{Key: "ownerId", Value: ownerID},
				{Key: "audit", Value: bson.D{{Key: "createdAt", Value: now}}},
			}),
			mtest.CreateCursorResponse(0, projectNamespace, mtest.FirstBatch),
			mtest.CreateCursorResponse(0, projectNamespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: organisationID},
				{Key: "organisationId", Value: organisationID},
				{Key: "name", Value: projectsBootstrapDefaultName},
				{Key: "slug", Value: projectsBootstrapDefaultSlug},
				{Key: "isActive", Value: true},
				{Key: "audit", Value: bson.D{
					{Key: "createdBy", Value: ownerID.Hex()},
					{Key: "createdAt", Value: now},
					{Key: "updatedBy", Value: ownerID.Hex()},
					{Key: "updatedAt", Value: now},
					{Key: "lastAction", Value: "project.created"},
				}},
			}),
		)
		report := projectsBootstrapReport{}
		runner := projectsBootstrapRunner{
			config:   ProjectsBootstrapConfig{Mode: "live"},
			database: mt.DB,
			now:      now,
			report:   &report,
		}

		ok, changed, err := runner.ensureDefaultProject(context.Background(), organisationID, ownerID)
		if err != nil {
			t.Fatalf("ensureDefaultProject() error = %v", err)
		}
		if !ok || changed {
			t.Fatalf("ensureDefaultProject() = (%v, %v), want (true, false)", ok, changed)
		}
		if report.Projects.AlreadyPresent != 1 || report.Writes.Attempted != 0 {
			t.Fatalf("projects = %+v, writes = %+v", report.Projects, report.Writes)
		}
		for _, event := range mt.GetAllStartedEvents() {
			if event.CommandName == "update" {
				t.Fatal("an already-migrated tenant issued an update")
			}
		}
	})
}

func TestProjectsBootstrapDryRunPlansWithoutWriting(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("missing default project", func(mt *mtest.T) {
		organisationID := primitive.NewObjectID()
		ownerID := primitive.NewObjectID()
		now := time.Date(2026, time.August, 6, 12, 0, 0, 0, time.UTC)
		projectNamespace := mt.DB.Name() + "." + projectsBootstrapCollection
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, mt.DB.Name()+".organisation", mtest.FirstBatch, bson.D{
				{Key: "_id", Value: organisationID},
				{Key: "ownerId", Value: ownerID},
			}),
			mtest.CreateCursorResponse(0, projectNamespace, mtest.FirstBatch),
			mtest.CreateCursorResponse(0, projectNamespace, mtest.FirstBatch),
		)
		report := projectsBootstrapReport{}
		runner := projectsBootstrapRunner{
			config:   ProjectsBootstrapConfig{Mode: "dry-run"},
			database: mt.DB,
			now:      now,
			report:   &report,
		}

		ok, changed, err := runner.ensureDefaultProject(context.Background(), organisationID, ownerID)
		if err != nil {
			t.Fatalf("ensureDefaultProject() error = %v", err)
		}
		if !ok || !changed {
			t.Fatalf("ensureDefaultProject() = (%v, %v), want (true, true)", ok, changed)
		}
		if report.Projects.Planned != 1 || report.Writes.Attempted != 0 || report.Projects.Inserted != 0 {
			t.Fatalf("projects = %+v, writes = %+v", report.Projects, report.Writes)
		}
		for _, event := range mt.GetAllStartedEvents() {
			if event.CommandName == "update" {
				t.Fatal("dry-run planning issued an update")
			}
		}
	})
}

// verify never plans work: a missing default project has to surface as a
// conflict, otherwise a partially applied live run could report success.
func TestProjectsBootstrapStrictMissingDefaultProjectIsAConflict(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("verify stage", func(mt *mtest.T) {
		organisationID := primitive.NewObjectID()
		ownerID := primitive.NewObjectID()
		now := time.Date(2026, time.August, 6, 12, 0, 0, 0, time.UTC)
		projectNamespace := mt.DB.Name() + "." + projectsBootstrapCollection
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, mt.DB.Name()+".organisation", mtest.FirstBatch, bson.D{
				{Key: "_id", Value: organisationID},
				{Key: "ownerId", Value: ownerID},
			}),
			mtest.CreateCursorResponse(0, projectNamespace, mtest.FirstBatch),
			mtest.CreateCursorResponse(0, projectNamespace, mtest.FirstBatch),
		)
		report := projectsBootstrapReport{}
		runner := projectsBootstrapRunner{
			config:   ProjectsBootstrapConfig{Mode: "dry-run"},
			database: mt.DB,
			now:      now,
			report:   &report,
			strict:   true,
		}

		ok, _, err := runner.ensureDefaultProject(context.Background(), organisationID, ownerID)
		if err != nil {
			t.Fatalf("ensureDefaultProject() error = %v", err)
		}
		if ok || runner.blockingConflictCount != 1 {
			t.Fatalf("ok = %v, blocking conflicts = %d", ok, runner.blockingConflictCount)
		}
		if report.Conflicts[0].Code != "default-project-missing" || report.Projects.Planned != 0 {
			t.Fatalf("conflicts = %+v, projects = %+v", report.Conflicts, report.Projects)
		}
	})
}

func TestProjectsBootstrapMissingOrganisationBlocks(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("organisation absent", func(mt *mtest.T) {
		organisationID := primitive.NewObjectID()
		ownerID := primitive.NewObjectID()
		mt.AddMockResponses(mtest.CreateCursorResponse(0, mt.DB.Name()+".organisation", mtest.FirstBatch))
		report := projectsBootstrapReport{}
		runner := projectsBootstrapRunner{
			config:   ProjectsBootstrapConfig{Mode: "live"},
			database: mt.DB,
			now:      time.Now().UTC(),
			report:   &report,
		}

		ok, _, err := runner.ensureDefaultProject(context.Background(), organisationID, ownerID)
		if err != nil {
			t.Fatalf("ensureDefaultProject() error = %v", err)
		}
		if ok || report.Conflicts[0].Code != "organisation-bootstrap-incomplete" {
			t.Fatalf("ok = %v, conflicts = %+v", ok, report.Conflicts)
		}
	})
}

func TestProjectsBootstrapResumeRestoresCheckpoint(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("blocked checkpoint", func(mt *mtest.T) {
		lastVerifiedMasterID := primitive.NewObjectID()
		checkpointID := "projects-bootstrap:v1:test:owners:all"
		mt.AddMockResponses(mtest.CreateSuccessResponse(
			bson.E{Key: "lastErrorObject", Value: bson.D{{Key: "n", Value: int32(1)}, {Key: "updatedExisting", Value: true}}},
			bson.E{Key: "value", Value: bson.D{
				{Key: "_id", Value: checkpointID},
				{Key: "migrationVersion", Value: 1},
				{Key: "database", Value: "test"},
				{Key: "stage", Value: "owners"},
				{Key: "scope", Value: "all"},
				{Key: "mode", Value: "live"},
				{Key: "status", Value: "running"},
				{Key: "leaseOwner", Value: "new-owner"},
				{Key: "leaseExpiresAt", Value: time.Now().UTC().Add(projectsBootstrapLeaseDuration)},
				{Key: "lastVerifiedMasterId", Value: lastVerifiedMasterID},
				{Key: "startedAt", Value: time.Now().UTC().Add(-time.Hour)},
				{Key: "updatedAt", Value: time.Now().UTC()},
				{Key: "counters", Value: bson.D{
					{Key: "masters", Value: bson.D{{Key: "scanned", Value: int64(8)}}},
					{Key: "projects", Value: bson.D{{Key: "inserted", Value: int64(6)}}},
					{Key: "users", Value: bson.D{{Key: "mastersupdated", Value: int64(4)}}},
					{Key: "writes", Value: bson.D{{Key: "applied", Value: int64(5)}}},
				}},
				{Key: "conflicts", Value: bson.A{bson.D{{Key: "code", Value: "default-project-conflict"}}}},
			}},
		))
		report := projectsBootstrapReport{Checkpoint: projectsBootstrapCheckpoint{ID: checkpointID}}
		runner := projectsBootstrapRunner{
			config: ProjectsBootstrapConfig{
				Mode:                       "live",
				Stage:                      "owners",
				Resume:                     true,
				MigrationVersion:           1,
				MongoDBDestinationDatabase: "test",
			},
			database: mt.DB,
			report:   &report,
		}

		if err := runner.acquireCheckpoint(context.Background()); err != nil {
			t.Fatalf("acquireCheckpoint() error = %v", err)
		}
		if runner.checkpointLastMaster != lastVerifiedMasterID {
			t.Fatalf("resumed from %s, want %s", runner.checkpointLastMaster.Hex(), lastVerifiedMasterID.Hex())
		}
		if report.Masters.Scanned != 8 || report.Projects.Inserted != 6 || report.Users.MastersUpdated != 4 || report.Writes.Applied != 5 {
			t.Fatalf("restored report = %+v", report)
		}
		if runner.hasConflict || len(report.Conflicts) != 0 {
			t.Fatalf("a resumed run must re-observe conflicts, not inherit them: %+v", report.Conflicts)
		}
	})
}

func TestProjectsBootstrapRefreshCheckpointLease(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("owned lease", func(mt *mtest.T) {
		mt.AddMockResponses(mtest.CreateSuccessResponse(
			bson.E{Key: "n", Value: int32(1)},
			bson.E{Key: "nModified", Value: int32(1)},
		))
		report := projectsBootstrapReport{Checkpoint: projectsBootstrapCheckpoint{ID: "checkpoint"}}
		runner := projectsBootstrapRunner{
			database:             mt.DB,
			report:               &report,
			checkpointLeaseOwner: "lease-owner",
		}
		if err := runner.refreshCheckpointLease(context.Background()); err != nil {
			t.Fatalf("refreshCheckpointLease() error = %v", err)
		}
	})
}
