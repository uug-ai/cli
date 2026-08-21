package actions

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestInspectOrganisationsBackfillSubscriptionsIsReadOnly(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("legacy sub-user subscription", func(mt *mtest.T) {
		subscriptionID := primitive.NewObjectID()
		userID := primitive.NewObjectID()
		organisationID := primitive.NewObjectID()
		subscriptionsNamespace := mt.DB.Name() + ".subscriptions"
		usersNamespace := mt.DB.Name() + ".users"
		organisationsNamespace := mt.DB.Name() + ".organisation"
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, subscriptionsNamespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: subscriptionID},
				{Key: "user_id", Value: userID.Hex()},
			}),
			mtest.CreateCursorResponse(0, usersNamespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: userID},
				{Key: "user_id", Value: organisationID.Hex()},
			}),
			mtest.CreateCursorResponse(0, organisationsNamespace, mtest.FirstBatch, bson.D{{Key: "_id", Value: organisationID}}),
			mtest.CreateCursorResponse(0, subscriptionsNamespace, mtest.FirstBatch,
				bson.D{{Key: "name", Value: "canonical_active"}, {Key: "key", Value: bson.D{{Key: "organisation_id", Value: int32(1)}, {Key: "ends_at", Value: int32(1)}}}},
				bson.D{{Key: "name", Value: "legacy_rollback_extended"}, {Key: "key", Value: bson.D{{Key: "user_id", Value: int32(1)}, {Key: "ends_at", Value: int32(1)}, {Key: "_id", Value: int32(-1)}}}},
				bson.D{{Key: "name", Value: "cleanup"}, {Key: "key", Value: bson.D{{Key: "organisation_id", Value: int32(1)}, {Key: "updated_at", Value: int32(-1)}, {Key: "created_at", Value: int32(-1)}, {Key: "_id", Value: int32(-1)}}}},
			),
		)

		report, err := inspectOrganisationsBackfillSubscriptions(
			context.Background(),
			mt.DB,
			organisationsBackfillAdapters()["subscriptions"],
			OrganisationsBackfillConfig{BatchSize: 500},
			organisationsBackfillCollection{LegacyCandidateCount: map[string]int64{"user_id": 1}},
		)
		if err != nil {
			t.Fatalf("inspectOrganisationsBackfillSubscriptions() error = %v", err)
		}
		if report.Resolution == nil || report.Resolution.Scanned != 1 || report.Resolution.Resolved != 1 || report.Resolution.ProposedWrites != 1 || report.Resolution.Conflicts != 0 {
			t.Fatalf("resolution = %+v", report.Resolution)
		}
		statuses := map[string]string{}
		for _, contract := range report.IndexContracts {
			statuses[contract.Name] = contract.Status
		}
		if statuses["canonical-active-lookup"] != "exact" || statuses["legacy-rollback"] != "prefix" || statuses["cleanup"] != "exact" {
			t.Fatalf("index statuses = %+v", statuses)
		}
		for _, event := range mt.GetAllStartedEvents() {
			if event.CommandName != "find" && event.CommandName != "listIndexes" {
				t.Fatalf("subscription dry-run issued non-read command %q", event.CommandName)
			}
		}
	})
}

func TestInspectOrganisationsBackfillSubscriptionsRejectsMissingScopeOrganisation(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("missing scoped organisation", func(mt *mtest.T) {
		organisationID := primitive.NewObjectID()
		subscriptionsNamespace := mt.DB.Name() + ".subscriptions"
		organisationsNamespace := mt.DB.Name() + ".organisation"
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, subscriptionsNamespace, mtest.FirstBatch),
			mtest.CreateCursorResponse(0, organisationsNamespace, mtest.FirstBatch),
			mtest.CreateCursorResponse(0, subscriptionsNamespace, mtest.FirstBatch),
		)

		report, err := inspectOrganisationsBackfillSubscriptions(
			context.Background(),
			mt.DB,
			organisationsBackfillAdapters()["subscriptions"],
			OrganisationsBackfillConfig{BatchSize: 500, OrganisationID: organisationID.Hex()},
			organisationsBackfillCollection{LegacyCandidateCount: map[string]int64{"user_id": 0}},
		)
		if err != nil {
			t.Fatalf("inspectOrganisationsBackfillSubscriptions() error = %v", err)
		}
		if report.Resolution == nil || report.Resolution.Conflicts != 1 || report.Resolution.OrphanOrganisations != 1 {
			t.Fatalf("resolution = %+v", report.Resolution)
		}
		entry := report.Resolution.ConflictEntries[0]
		if entry.Code != "scope-organisation-not-found" || entry.CanonicalOrganisation != organisationID.Hex() {
			t.Fatalf("scope conflict = %+v", entry)
		}
		for _, event := range mt.GetAllStartedEvents() {
			if event.CommandName != "find" && event.CommandName != "listIndexes" {
				t.Fatalf("subscription dry-run issued non-read command %q", event.CommandName)
			}
		}
	})
}
