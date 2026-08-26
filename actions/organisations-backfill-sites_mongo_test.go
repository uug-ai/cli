package actions

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestInspectOrganisationsBackfillSitesIsReadOnly(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("legacy default site", func(mt *mtest.T) {
		siteID := primitive.NewObjectID()
		organisationID := primitive.NewObjectID()
		sitesNamespace := mt.DB.Name() + ".sites"
		organisationsNamespace := mt.DB.Name() + ".organisation"
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, sitesNamespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: siteID},
				{Key: "user_id", Value: organisationID.Hex()},
				{Key: "devices", Value: bson.A{"device-1"}},
			}),
			mtest.CreateCursorResponse(0, organisationsNamespace, mtest.FirstBatch, bson.D{{Key: "_id", Value: organisationID}}),
			mtest.CreateCursorResponse(0, sitesNamespace, mtest.FirstBatch,
				bson.D{{Key: "name", Value: "devices_1"}, {Key: "key", Value: bson.D{{Key: "devices", Value: int32(1)}}}},
			),
		)

		report, err := inspectOrganisationsBackfillSites(
			context.Background(),
			mt.DB,
			organisationsBackfillAdapters()["sites"],
			OrganisationsBackfillConfig{BatchSize: 500},
			organisationsBackfillCollection{LegacyCandidateCount: map[string]int64{"user_id": 1}},
		)
		if err != nil {
			mt.Fatalf("inspectOrganisationsBackfillSites() error = %v", err)
		}
		if report.Resolution == nil || report.Resolution.Resolved != 1 || report.Resolution.ProposedWrites != 1 ||
			report.Resolution.ProjectResolved != 1 || report.Resolution.ProposedProjectWrites != 1 || report.Resolution.Conflicts != 0 {
			mt.Fatalf("resolution = %+v", report.Resolution)
		}
		if len(report.IndexContracts) != 3 || report.IndexContracts[2].Status != "exact" {
			mt.Fatalf("index contracts = %+v", report.IndexContracts)
		}
		for _, event := range mt.GetAllStartedEvents() {
			if event.CommandName != "find" && event.CommandName != "listIndexes" {
				mt.Fatalf("site dry-run issued non-read command %q", event.CommandName)
			}
		}
	})
}
