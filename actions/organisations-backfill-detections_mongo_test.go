package actions

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestInspectOrganisationsBackfillDetectionsIsReadOnly(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("source derived non-default project", func(mt *mtest.T) {
		organisationID := primitive.NewObjectID()
		projectID := primitive.NewObjectID()
		detectionNamespace := mt.DB.Name() + ".detections"
		analysisNamespace := mt.DB.Name() + ".analysis"
		organisationNamespace := mt.DB.Name() + ".organisation"
		projectNamespace := mt.DB.Name() + ".project"
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, detectionNamespace, mtest.FirstBatch, bson.D{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "key", Value: "media-1"}}),
			mtest.CreateCursorResponse(0, analysisNamespace, mtest.FirstBatch, bson.D{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "key", Value: "media-1"}, {Key: "organisationId", Value: organisationID.Hex()}, {Key: "projectId", Value: projectID}}),
			mtest.CreateCursorResponse(0, organisationNamespace, mtest.FirstBatch, bson.D{{Key: "_id", Value: organisationID}}),
			mtest.CreateCursorResponse(0, projectNamespace, mtest.FirstBatch, bson.D{{Key: "_id", Value: projectID}, {Key: "organisationId", Value: organisationID}}),
			mtest.CreateCursorResponse(0, detectionNamespace, mtest.FirstBatch, bson.D{{Key: "name", Value: "organisationId_1_projectId_1_key_1"}, {Key: "key", Value: bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "key", Value: int32(1)}}}}),
		)
		report, err := inspectOrganisationsBackfillDetections(context.Background(), mt.DB, organisationsBackfillAdapters()["detections"], OrganisationsBackfillConfig{BatchSize: 500}, organisationsBackfillCollection{LegacyCandidateCount: map[string]int64{"key": 1}})
		if err != nil {
			mt.Fatalf("inspectOrganisationsBackfillDetections() error = %v", err)
		}
		if report.Resolution == nil || report.Resolution.Resolved != 1 || report.Resolution.ProjectResolved != 1 || report.Resolution.ProposedWrites != 1 || report.Resolution.ProposedProjectWrites != 1 || report.Resolution.Conflicts != 0 {
			mt.Fatalf("resolution = %+v", report.Resolution)
		}
		if len(report.IndexContracts) != 3 || report.IndexContracts[0].Status != "exact" {
			mt.Fatalf("index contracts = %+v", report.IndexContracts)
		}
		assertOrganisationsBackfillReadOnlyCommands(mt)
	})
}
