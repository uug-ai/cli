package actions

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestOrganisationsBootstrapAmbiguousCanonicalInsertReconciles(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("committed organisation", func(mt *mtest.T) {
		masterID := primitive.NewObjectID()
		now := time.Date(2026, time.August, 6, 12, 0, 0, 0, time.UTC)
		user := organisationsBootstrapUser{ID: masterID, Username: "owner", CreatedAt: now}
		namespace := mt.DB.Name() + ".organisation"
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, namespace, mtest.FirstBatch),
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 123, Message: "ambiguous update result"}),
			mtest.CreateCursorResponse(0, namespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: masterID},
				{Key: "name", Value: "owner"},
				{Key: "ownerId", Value: masterID},
				{Key: "isActive", Value: true},
				{Key: "audit", Value: bson.D{
					{Key: "createdBy", Value: masterID.Hex()},
					{Key: "createdAt", Value: now},
					{Key: "updatedBy", Value: masterID.Hex()},
					{Key: "updatedAt", Value: now},
					{Key: "lastAction", Value: "organisation.migrated"},
				}},
			}),
		)
		report := organisationsBootstrapReport{}
		runner := organisationsBootstrapRunner{
			config:   OrganisationsBootstrapConfig{Mode: "live"},
			database: mt.DB,
			now:      now,
			report:   &report,
		}

		ready, changed, err := runner.ensureCanonicalOrganisation(context.Background(), user)
		if err != nil {
			t.Fatalf("ensureCanonicalOrganisation() error = %v", err)
		}
		if !ready || !changed {
			t.Fatalf("ensureCanonicalOrganisation() = (%v, %v), want (true, true)", ready, changed)
		}
		if report.Writes.Reconciled != 1 || report.Writes.Failed != 0 {
			t.Fatalf("write counts = %+v", report.Writes)
		}
	})
}

func TestOrganisationsBootstrapAmbiguousMembershipUpsertReconciles(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("committed membership", func(mt *mtest.T) {
		userID := primitive.NewObjectID()
		organisationID := primitive.NewObjectID()
		now := time.Date(2026, time.August, 6, 12, 0, 0, 0, time.UTC)
		namespace := mt.DB.Name() + ".organisation_users"
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, namespace, mtest.FirstBatch, bson.D{{Key: "n", Value: int32(0)}}),
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 123, Message: "ambiguous update result"}),
			mtest.CreateCursorResponse(0, namespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: primitive.NewObjectID()},
				{Key: "userId", Value: userID},
				{Key: "organisationId", Value: organisationID},
				{Key: "status", Value: "active"},
			}),
		)
		report := organisationsBootstrapReport{}
		runner := organisationsBootstrapRunner{
			config:   OrganisationsBootstrapConfig{Mode: "live"},
			database: mt.DB,
			now:      now,
			report:   &report,
		}

		ready, err := runner.ensureMembership(context.Background(), userID, organisationID, userID)
		if err != nil {
			t.Fatalf("ensureMembership() error = %v", err)
		}
		if !ready {
			t.Fatal("ensureMembership() did not reconcile the committed membership")
		}
		if report.Writes.Reconciled != 1 || report.Writes.Failed != 0 {
			t.Fatalf("write counts = %+v", report.Writes)
		}
	})
}

func TestOrganisationsBootstrapExistingMembershipIsIdempotent(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("active membership", func(mt *mtest.T) {
		userID := primitive.NewObjectID()
		organisationID := primitive.NewObjectID()
		namespace := mt.DB.Name() + ".organisation_users"
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, namespace, mtest.FirstBatch, bson.D{{Key: "n", Value: int32(1)}}),
			mtest.CreateCursorResponse(0, namespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: primitive.NewObjectID()},
				{Key: "userId", Value: userID},
				{Key: "organisationId", Value: organisationID},
				{Key: "status", Value: "active"},
			}),
		)
		report := organisationsBootstrapReport{}
		runner := organisationsBootstrapRunner{database: mt.DB, now: time.Now().UTC(), report: &report}

		ready, err := runner.ensureMembership(context.Background(), userID, organisationID, userID)
		if err != nil || !ready {
			t.Fatalf("ensureMembership() = (%v, %v), want (true, nil)", ready, err)
		}
		if report.Writes.Attempted != 0 || report.Memberships.AlreadyPresent != 1 {
			t.Fatalf("report = %+v", report)
		}
		for _, event := range mt.GetAllStartedEvents() {
			if event.CommandName == "update" {
				t.Fatal("idempotent membership rerun issued an update")
			}
		}
	})
}

func TestOrganisationsBootstrapDryRunMembershipPerformsNoWrite(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("missing membership", func(mt *mtest.T) {
		userID := primitive.NewObjectID()
		organisationID := primitive.NewObjectID()
		namespace := mt.DB.Name() + ".organisation_users"
		mt.AddMockResponses(mtest.CreateCursorResponse(0, namespace, mtest.FirstBatch, bson.D{{Key: "n", Value: int32(0)}}))
		report := organisationsBootstrapReport{}
		runner := organisationsBootstrapRunner{
			config:   OrganisationsBootstrapConfig{Mode: "dry-run"},
			database: mt.DB,
			now:      time.Now().UTC(),
			report:   &report,
		}

		ready, err := runner.ensureMembership(context.Background(), userID, organisationID, userID)
		if err != nil || !ready {
			t.Fatalf("ensureMembership() = (%v, %v), want (true, nil)", ready, err)
		}
		if report.Memberships.Planned != 1 || report.Writes.Attempted != 0 {
			t.Fatalf("report = %+v", report)
		}
		for _, event := range mt.GetAllStartedEvents() {
			if event.CommandName == "update" {
				t.Fatal("dry-run membership planning issued an update")
			}
		}
	})
}

func TestOrganisationsBootstrapAmbiguousUserSelectionReconciles(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("committed selection", func(mt *mtest.T) {
		userID := primitive.NewObjectID()
		targetID := primitive.NewObjectID()
		namespace := mt.DB.Name() + ".users"
		mt.AddMockResponses(
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 123, Message: "ambiguous update result"}),
			mtest.CreateCursorResponse(0, namespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: userID},
				{Key: "organisation_id", Value: targetID},
			}),
		)
		report := organisationsBootstrapReport{}
		runner := organisationsBootstrapRunner{
			config:   OrganisationsBootstrapConfig{Mode: "live"},
			database: mt.DB,
			report:   &report,
		}

		ready, updated, err := runner.ensureUserSelection(context.Background(), organisationsBootstrapUser{ID: userID}, targetID, userID)
		if err != nil {
			t.Fatalf("ensureUserSelection() error = %v", err)
		}
		if !ready || !updated {
			t.Fatalf("ensureUserSelection() = (%v, %v), want (true, true)", ready, updated)
		}
		if report.Writes.Applied != 1 || report.Writes.Reconciled != 1 || report.Writes.Failed != 0 {
			t.Fatalf("write counts = %+v", report.Writes)
		}
	})
}

func TestOrganisationsBootstrapAmbiguousUserSelectionConflictBlocks(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("conflicting selection", func(mt *mtest.T) {
		userID := primitive.NewObjectID()
		targetID := primitive.NewObjectID()
		namespace := mt.DB.Name() + ".users"
		mt.AddMockResponses(
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 123, Message: "ambiguous update result"}),
			mtest.CreateCursorResponse(0, namespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: userID},
				{Key: "organisation_id", Value: primitive.NewObjectID()},
			}),
		)
		report := organisationsBootstrapReport{}
		runner := organisationsBootstrapRunner{
			config:   OrganisationsBootstrapConfig{Mode: "live"},
			database: mt.DB,
			report:   &report,
		}

		ready, _, err := runner.ensureUserSelection(context.Background(), organisationsBootstrapUser{ID: userID}, targetID, userID)
		if err != nil {
			t.Fatalf("ensureUserSelection() error = %v", err)
		}
		if ready || !runner.hasConflict || runner.blockingConflictCount != 1 {
			t.Fatalf("ready = %v, hasConflict = %v, blocking conflicts = %d", ready, runner.hasConflict, runner.blockingConflictCount)
		}
		if report.Writes.Failed != 1 {
			t.Fatalf("write counts = %+v", report.Writes)
		}
	})
}

func TestOrganisationsBootstrapResumeRestoresCheckpoint(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("failed checkpoint", func(mt *mtest.T) {
		lastVerifiedMasterID := primitive.NewObjectID()
		checkpointID := "organisations-bootstrap:v1:test:owners:all"
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
				{Key: "leaseExpiresAt", Value: time.Now().UTC().Add(organisationsBootstrapLeaseDuration)},
				{Key: "lastVerifiedMasterId", Value: lastVerifiedMasterID},
				{Key: "startedAt", Value: time.Now().UTC().Add(-time.Hour)},
				{Key: "updatedAt", Value: time.Now().UTC()},
				{Key: "counters", Value: bson.D{
					{Key: "masters", Value: bson.D{{Key: "scanned", Value: int64(8)}}},
					{Key: "users", Value: bson.D{{Key: "mastersupdated", Value: int64(4)}}},
					{Key: "organisations", Value: bson.D{{Key: "legacyreported", Value: int64(3)}}},
					{Key: "writes", Value: bson.D{{Key: "applied", Value: int64(5)}}},
				}},
			}},
		))
		report := organisationsBootstrapReport{Checkpoint: organisationsBootstrapCheckpoint{ID: checkpointID}}
		runner := organisationsBootstrapRunner{
			config: OrganisationsBootstrapConfig{
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
		if runner.checkpointLastMaster != lastVerifiedMasterID || report.Masters.Scanned != 8 || report.Users.MastersUpdated != 4 || report.Organisations.LegacyReported != 3 || report.Writes.Applied != 5 {
			t.Fatalf("restored checkpoint = last %s, report %+v", runner.checkpointLastMaster.Hex(), report)
		}
	})
}

func TestOrganisationsBootstrapRefreshCheckpointLease(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("owned lease", func(mt *mtest.T) {
		mt.AddMockResponses(mtest.CreateSuccessResponse(
			bson.E{Key: "n", Value: int32(1)},
			bson.E{Key: "nModified", Value: int32(1)},
		))
		report := organisationsBootstrapReport{Checkpoint: organisationsBootstrapCheckpoint{ID: "checkpoint"}}
		runner := organisationsBootstrapRunner{
			database:             mt.DB,
			report:               &report,
			checkpointLeaseOwner: "lease-owner",
		}
		if err := runner.refreshCheckpointLease(context.Background()); err != nil {
			t.Fatalf("refreshCheckpointLease() error = %v", err)
		}
	})
}
