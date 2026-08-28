package actions

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestInspectOrganisationsBackfillAnalysisIsReadOnly(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("legacy default project", func(mt *mtest.T) {
		organisationID := primitive.NewObjectID()
		analysisNamespace := mt.DB.Name() + ".analysis"
		organisationNamespace := mt.DB.Name() + ".organisation"
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, analysisNamespace, mtest.FirstBatch, bson.D{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "key", Value: "media-1"}, {Key: "userid", Value: organisationID.Hex()}}),
			mtest.CreateCursorResponse(0, organisationNamespace, mtest.FirstBatch, bson.D{{Key: "_id", Value: organisationID}}),
			mtest.CreateCursorResponse(0, analysisNamespace, mtest.FirstBatch, bson.D{{Key: "name", Value: "userid_1_projectId_1_key_1"}, {Key: "key", Value: bson.D{{Key: "userid", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "key", Value: int32(1)}}}}),
		)
		report, err := inspectOrganisationsBackfillAnalysis(context.Background(), mt.DB, organisationsBackfillAdapters()["analysis"], OrganisationsBackfillConfig{BatchSize: 500}, organisationsBackfillCollection{LegacyCandidateCount: map[string]int64{"userid": 1, "user_id": 0}})
		if err != nil {
			mt.Fatalf("inspectOrganisationsBackfillAnalysis() error = %v", err)
		}
		if report.Resolution == nil || report.Resolution.Resolved != 1 || report.Resolution.ProjectResolved != 1 || report.Resolution.Conflicts != 0 {
			mt.Fatalf("resolution = %+v", report.Resolution)
		}
		if len(report.IndexContracts) != 3 || report.IndexContracts[1].Status != "exact" {
			mt.Fatalf("index contracts = %+v", report.IndexContracts)
		}
		assertOrganisationsBackfillReadOnlyCommands(mt)
	})
}
