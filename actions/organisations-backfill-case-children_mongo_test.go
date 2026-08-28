package actions

import (
	"context"
	"strings"
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

func TestInspectOrganisationsBackfillCaseSharesReportsDuplicateActiveTokens(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("duplicate active token", func(mt *mtest.T) {
		organisationID := primitive.NewObjectID()
		taskID := primitive.NewObjectID()
		sharesNamespace := mt.DB.Name() + ".case_shares"
		tasksNamespace := mt.DB.Name() + ".tasks"
		organisationsNamespace := mt.DB.Name() + ".organisation"
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, sharesNamespace, mtest.FirstBatch,
				bson.D{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "task_id", Value: taskID.Hex()}, {Key: "token", Value: "secret-token"}, {Key: "is_active", Value: true}},
				bson.D{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "task_id", Value: taskID.Hex()}, {Key: "token", Value: "secret-token"}, {Key: "is_active", Value: true}},
			),
			mtest.CreateCursorResponse(0, tasksNamespace, mtest.FirstBatch, bson.D{{Key: "_id", Value: taskID}, {Key: "user_id", Value: organisationID.Hex()}}),
			mtest.CreateCursorResponse(0, organisationsNamespace, mtest.FirstBatch, bson.D{{Key: "_id", Value: organisationID}}),
			mtest.CreateCursorResponse(0, sharesNamespace, mtest.FirstBatch,
				bson.D{{Key: "name", Value: "token_1_is_active_1"}, {Key: "key", Value: bson.D{{Key: "token", Value: int32(1)}, {Key: "is_active", Value: int32(1)}}}},
				bson.D{{Key: "name", Value: "task_id_1_user_id_1_created_at_-1"}, {Key: "key", Value: bson.D{{Key: "task_id", Value: int32(1)}, {Key: "user_id", Value: int32(1)}, {Key: "created_at", Value: int32(-1)}}}},
			),
		)

		report, err := inspectOrganisationsBackfillCaseChildren(
			context.Background(), mt.DB, organisationsBackfillAdapters()["case_shares"], OrganisationsBackfillConfig{BatchSize: 500},
			organisationsBackfillCollection{LegacyCandidateCount: map[string]int64{}},
		)
		if err != nil {
			mt.Fatalf("inspect case shares: %v", err)
		}
		resolution := report.Resolution
		if resolution == nil || resolution.Resolved != 2 || resolution.ProjectResolved != 2 ||
			resolution.DuplicateActiveTokens != 1 || resolution.DuplicateActiveTokenDocuments != 2 || resolution.Conflicts != 2 {
			mt.Fatalf("resolution = %+v", resolution)
		}
		if len(resolution.ConflictEntries) != 1 || resolution.ConflictEntries[0].Code != "duplicate-active-token" ||
			strings.Contains(resolution.ConflictEntries[0].Message, "secret-token") {
			mt.Fatalf("duplicate conflict = %+v", resolution.ConflictEntries)
		}
		if len(report.IndexContracts) != 3 || report.IndexContracts[0].Status != "missing" ||
			report.IndexContracts[1].Status != "exact" || report.IndexContracts[2].Status != "exact" {
			mt.Fatalf("index contracts = %+v", report.IndexContracts)
		}
		for _, event := range mt.GetAllStartedEvents() {
			if event.CommandName != "find" && event.CommandName != "listIndexes" {
				mt.Fatalf("case share dry-run issued non-read command %q", event.CommandName)
			}
		}
	})
}

func TestInspectOrganisationsBackfillTaskCommentsIsReadOnly(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("legacy default parent", func(mt *mtest.T) {
		organisationID := primitive.NewObjectID()
		taskID := primitive.NewObjectID()
		commenterID := primitive.NewObjectID()
		commentsNamespace := mt.DB.Name() + ".comments"
		tasksNamespace := mt.DB.Name() + ".tasks"
		organisationsNamespace := mt.DB.Name() + ".organisation"
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, commentsNamespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: primitive.NewObjectID()},
				{Key: "parent_id", Value: taskID.Hex()},
				{Key: "user_id", Value: commenterID.Hex()},
				{Key: "comment", Value: "evidence note"},
			}),
			mtest.CreateCursorResponse(0, tasksNamespace, mtest.FirstBatch, bson.D{{Key: "_id", Value: taskID}, {Key: "user_id", Value: organisationID.Hex()}}),
			mtest.CreateCursorResponse(0, organisationsNamespace, mtest.FirstBatch, bson.D{{Key: "_id", Value: organisationID}}),
			mtest.CreateCursorResponse(0, commentsNamespace, mtest.FirstBatch),
		)

		report, err := inspectOrganisationsBackfillCaseChildren(
			context.Background(), mt.DB, organisationsBackfillAdapters()["comments"], OrganisationsBackfillConfig{BatchSize: 500},
			organisationsBackfillCollection{LegacyCandidateCount: map[string]int64{}},
		)
		if err != nil {
			mt.Fatalf("inspect comments: %v", err)
		}
		resolution := report.Resolution
		if resolution == nil || resolution.Resolved != 1 || resolution.ProjectResolved != 1 || resolution.ProposedWrites != 1 || resolution.ProposedProjectWrites != 1 || resolution.Conflicts != 0 {
			mt.Fatalf("resolution = %+v", resolution)
		}
		if len(report.IndexContracts) != 1 || report.IndexContracts[0].Status != "missing" {
			mt.Fatalf("index contracts = %+v", report.IndexContracts)
		}
		if resolution.ObservedFieldTypes["user_id"]["string"] != 1 {
			mt.Fatalf("commenter provenance evidence = %+v", resolution.ObservedFieldTypes)
		}
		for _, event := range mt.GetAllStartedEvents() {
			if event.CommandName != "find" && event.CommandName != "listIndexes" {
				mt.Fatalf("comment dry-run issued non-read command %q", event.CommandName)
			}
		}
	})
}
