package actions

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestInspectOrganisationsBackfillCaseChildrenIsReadOnly(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("legacy default parent", func(mt *mtest.T) {
		organisationID := primitive.NewObjectID()
		taskID := primitive.NewObjectID()
		childID := primitive.NewObjectID()
		childrenNamespace := mt.DB.Name() + ".case_media"
		tasksNamespace := mt.DB.Name() + ".tasks"
		organisationsNamespace := mt.DB.Name() + ".organisation"
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, childrenNamespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: childID},
				{Key: "task_id", Value: taskID},
				{Key: "role", Value: "source"},
			}),
			mtest.CreateCursorResponse(0, tasksNamespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: taskID},
				{Key: "user_id", Value: organisationID.Hex()},
			}),
			mtest.CreateCursorResponse(0, organisationsNamespace, mtest.FirstBatch, bson.D{{Key: "_id", Value: organisationID}}),
			mtest.CreateCursorResponse(0, childrenNamespace, mtest.FirstBatch,
				bson.D{{Key: "name", Value: "project_task_list"}, {Key: "key", Value: bson.D{
					{Key: "organisation_id", Value: int32(1)},
					{Key: "projectId", Value: int32(1)},
					{Key: "task_id", Value: int32(1)},
					{Key: "role", Value: int32(1)},
					{Key: "created_at", Value: int32(1)},
				}}},
			),
		)

		report, err := inspectOrganisationsBackfillCaseChildren(
			context.Background(),
			mt.DB,
			organisationsBackfillAdapters()["case_media"],
			OrganisationsBackfillConfig{BatchSize: 500},
			organisationsBackfillCollection{LegacyCandidateCount: map[string]int64{}},
		)
		if err != nil {
			mt.Fatalf("inspectOrganisationsBackfillCaseChildren() error = %v", err)
		}
		if report.Resolution == nil || report.Resolution.Resolved != 1 || report.Resolution.ProposedWrites != 1 ||
			report.Resolution.ProjectResolved != 1 || report.Resolution.ProposedProjectWrites != 1 || report.Resolution.Conflicts != 0 {
			mt.Fatalf("resolution = %+v", report.Resolution)
		}
		if len(report.IndexContracts) != 3 || report.IndexContracts[0].Status != "exact" {
			mt.Fatalf("index contracts = %+v", report.IndexContracts)
		}
		for _, event := range mt.GetAllStartedEvents() {
			if event.CommandName != "find" && event.CommandName != "listIndexes" {
				mt.Fatalf("case child dry-run issued non-read command %q", event.CommandName)
			}
		}
	})
}
