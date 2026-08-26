package actions

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestInspectOrganisationsBackfillIOIsReadOnly(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("legacy sub-user actor", func(mt *mtest.T) {
		ioID := primitive.NewObjectID()
		actorID := primitive.NewObjectID()
		organisationID := primitive.NewObjectID()
		ioNamespace := mt.DB.Name() + ".io"
		usersNamespace := mt.DB.Name() + ".users"
		organisationsNamespace := mt.DB.Name() + ".organisation"
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, ioNamespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: ioID},
				{Key: "user_id", Value: actorID.Hex()},
				{Key: "device", Value: "device-1"},
				{Key: "hash", Value: "hash-1"},
			}),
			mtest.CreateCursorResponse(0, usersNamespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: actorID},
				{Key: "user_id", Value: organisationID.Hex()},
			}),
			mtest.CreateCursorResponse(0, organisationsNamespace, mtest.FirstBatch, bson.D{{Key: "_id", Value: organisationID}}),
			mtest.CreateCursorResponse(0, ioNamespace, mtest.FirstBatch,
				bson.D{{Key: "name", Value: "hash_1"}, {Key: "key", Value: bson.D{{Key: "hash", Value: int32(1)}}}},
			),
		)

		report, err := inspectOrganisationsBackfillIO(
			context.Background(),
			mt.DB,
			organisationsBackfillAdapters()["io"],
			OrganisationsBackfillConfig{BatchSize: 500},
			organisationsBackfillCollection{LegacyCandidateCount: map[string]int64{"user_id": 1}},
		)
		if err != nil {
			mt.Fatalf("inspectOrganisationsBackfillIO() error = %v", err)
		}
		if report.Resolution == nil || report.Resolution.Resolved != 1 || report.Resolution.ProposedWrites != 1 ||
			report.Resolution.ProjectResolved != 1 || report.Resolution.ProposedProjectWrites != 1 || report.Resolution.Conflicts != 0 {
			mt.Fatalf("resolution = %+v", report.Resolution)
		}
		if len(report.IndexContracts) != 5 || report.IndexContracts[4].Status != "exact" {
			mt.Fatalf("index contracts = %+v", report.IndexContracts)
		}
		for _, event := range mt.GetAllStartedEvents() {
			if event.CommandName != "find" && event.CommandName != "listIndexes" {
				mt.Fatalf("IO dry-run issued non-read command %q", event.CommandName)
			}
		}
	})
}
