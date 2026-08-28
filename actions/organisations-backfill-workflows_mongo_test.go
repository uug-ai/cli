package actions

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestInspectOrganisationsBackfillWorkflowsIsReadOnly(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("legacy organisation", func(mt *mtest.T) {
		workflowID := primitive.NewObjectID()
		organisationID := primitive.NewObjectID()
		workflowNamespace := mt.DB.Name() + ".workflows"
		organisationNamespace := mt.DB.Name() + ".organisation"
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, workflowNamespace, mtest.FirstBatch, bson.D{{Key: "_id", Value: workflowID}, {Key: "organisation_id", Value: organisationID.Hex()}, {Key: "enabled", Value: true}, {Key: "name", Value: "notify"}}),
			mtest.CreateCursorResponse(0, organisationNamespace, mtest.FirstBatch, bson.D{{Key: "_id", Value: organisationID}}),
			mtest.CreateCursorResponse(0, workflowNamespace, mtest.FirstBatch, bson.D{{Key: "name", Value: "organisationId_1_projectId_1_enabled_1"}, {Key: "key", Value: bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "enabled", Value: int32(1)}}}}),
		)
		report, err := inspectOrganisationsBackfillWorkflows(context.Background(), mt.DB, organisationsBackfillAdapters()["workflows"], OrganisationsBackfillConfig{BatchSize: 500}, organisationsBackfillCollection{LegacyCandidateCount: map[string]int64{"organisation_id": 1, "user_id": 0}})
		if err != nil {
			mt.Fatalf("inspectOrganisationsBackfillWorkflows() error = %v", err)
		}
		if report.Resolution == nil || report.Resolution.Resolved != 1 || report.Resolution.ProjectResolved != 1 || report.Resolution.Conflicts != 0 {
			mt.Fatalf("resolution = %+v", report.Resolution)
		}
		if len(report.IndexContracts) != 6 || report.IndexContracts[0].Status != "exact" {
			mt.Fatalf("index contracts = %+v", report.IndexContracts)
		}
		assertOrganisationsBackfillReadOnlyCommands(mt)
	})
}

func assertOrganisationsBackfillReadOnlyCommands(mt *mtest.T) {
	for _, event := range mt.GetAllStartedEvents() {
		if event.CommandName != "find" && event.CommandName != "listIndexes" {
			mt.Fatalf("dry-run issued non-read command %q", event.CommandName)
		}
	}
}
