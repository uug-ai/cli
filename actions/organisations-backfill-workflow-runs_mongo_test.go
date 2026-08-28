package actions

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestInspectOrganisationsBackfillWorkflowRunsIsReadOnly(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("legacy default project config run", func(mt *mtest.T) {
		runID := primitive.NewObjectID()
		organisationID := primitive.NewObjectID()
		runNamespace := mt.DB.Name() + ".workflow_runs"
		organisationNamespace := mt.DB.Name() + ".organisation"
		mediaNamespace := mt.DB.Name() + ".media"
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, runNamespace, mtest.FirstBatch, bson.D{{Key: "_id", Value: runID}, {Key: "userid", Value: organisationID.Hex()}, {Key: "key", Value: "media-1"}, {Key: "workflowid", Value: "config-workflow"}}),
			mtest.CreateCursorResponse(0, organisationNamespace, mtest.FirstBatch, bson.D{{Key: "_id", Value: organisationID}}),
			mtest.CreateCursorResponse(0, mediaNamespace, mtest.FirstBatch, bson.D{{Key: "key", Value: "media-1"}, {Key: "organisationId", Value: organisationID.Hex()}}),
			mtest.CreateCursorResponse(0, runNamespace, mtest.FirstBatch, bson.D{{Key: "name", Value: "organisationId_1_projectId_1_key_1"}, {Key: "key", Value: bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "key", Value: int32(1)}}}}),
		)
		report, err := inspectOrganisationsBackfillWorkflowRuns(context.Background(), mt.DB, organisationsBackfillAdapters()["workflow_runs"], OrganisationsBackfillConfig{BatchSize: 500}, organisationsBackfillCollection{LegacyCandidateCount: map[string]int64{"userid": 1, "user_id": 0}})
		if err != nil {
			mt.Fatalf("inspectOrganisationsBackfillWorkflowRuns() error = %v", err)
		}
		if report.Resolution == nil || report.Resolution.Resolved != 1 || report.Resolution.ProjectResolved != 1 || report.Resolution.Conflicts != 0 {
			mt.Fatalf("resolution = %+v", report.Resolution)
		}
		if len(report.IndexContracts) != 4 || report.IndexContracts[2].Status != "exact" {
			mt.Fatalf("index contracts = %+v", report.IndexContracts)
		}
		assertOrganisationsBackfillReadOnlyCommands(mt)
	})
}
