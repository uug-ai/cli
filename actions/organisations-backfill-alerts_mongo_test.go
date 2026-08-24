package actions

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestInspectOrganisationsBackfillAlertsIsReadOnly(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("legacy master alert", func(mt *mtest.T) {
		alertID := primitive.NewObjectID()
		organisationID := primitive.NewObjectID()
		alertsNamespace := mt.DB.Name() + ".alerts"
		organisationsNamespace := mt.DB.Name() + ".organisation"
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, alertsNamespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: alertID},
				{Key: "master_user_id", Value: organisationID.Hex()},
				{Key: "user_id", Value: primitive.NewObjectID().Hex()},
				{Key: "enabled", Value: true},
			}),
			mtest.CreateCursorResponse(0, organisationsNamespace, mtest.FirstBatch, bson.D{{Key: "_id", Value: organisationID}}),
			mtest.CreateCursorResponse(0, alertsNamespace, mtest.FirstBatch,
				bson.D{{Key: "name", Value: "master_user_id_1_enabled_1"}, {Key: "key", Value: bson.D{{Key: "master_user_id", Value: int32(1)}, {Key: "enabled", Value: int32(1)}}}},
			),
		)

		report, err := inspectOrganisationsBackfillAlerts(
			context.Background(),
			mt.DB,
			organisationsBackfillAdapters()["alerts"],
			OrganisationsBackfillConfig{BatchSize: 500},
			organisationsBackfillCollection{LegacyCandidateCount: map[string]int64{"master_user_id": 1, "user_id": 1}},
		)
		if err != nil {
			mt.Fatalf("inspectOrganisationsBackfillAlerts() error = %v", err)
		}
		if report.Resolution == nil || report.Resolution.Resolved != 1 || report.Resolution.ProposedWrites != 1 ||
			report.Resolution.ProjectResolved != 1 || report.Resolution.ProposedProjectWrites != 1 || report.Resolution.Conflicts != 0 {
			mt.Fatalf("resolution = %+v", report.Resolution)
		}
		if len(report.IndexContracts) != 4 || report.IndexContracts[3].Status != "exact" {
			mt.Fatalf("index contracts = %+v", report.IndexContracts)
		}
		for _, event := range mt.GetAllStartedEvents() {
			if event.CommandName != "find" && event.CommandName != "listIndexes" {
				mt.Fatalf("alert dry-run issued non-read command %q", event.CommandName)
			}
		}
	})
}
