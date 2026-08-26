package actions

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestInspectOrganisationsBackfillTokensIsReadOnly(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("canonical wins with non-default project", func(mt *mtest.T) {
		tokenID := primitive.NewObjectID()
		organisationID := primitive.NewObjectID()
		projectID := primitive.NewObjectID()
		tokensNamespace := mt.DB.Name() + ".tokens"
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, tokensNamespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: tokenID},
				{Key: "organisationId", Value: organisationID.Hex()},
				{Key: "projectId", Value: projectID},
				{Key: "userId", Value: "malformed-provenance"},
				{Key: "token", Value: "secret"},
				{Key: "scopes", Value: bson.A{"read"}},
			}),
			mtest.CreateCursorResponse(0, mt.DB.Name()+".organisation", mtest.FirstBatch, bson.D{{Key: "_id", Value: organisationID}}),
			mtest.CreateCursorResponse(0, mt.DB.Name()+".project", mtest.FirstBatch, bson.D{{Key: "_id", Value: projectID}, {Key: "organisationId", Value: organisationID}}),
			mtest.CreateCursorResponse(0, tokensNamespace, mtest.FirstBatch,
				bson.D{{Key: "name", Value: "_id_"}, {Key: "key", Value: bson.D{{Key: "_id", Value: int32(1)}}}},
			),
		)

		report := inspectOrganisationsBackfillTokensForTest(mt)
		if report.Resolution.CanonicalValid != 1 || report.Resolution.Resolved != 0 || report.Resolution.ProjectResolved != 1 ||
			report.Resolution.ProposedWrites != 0 || report.Resolution.ProposedProjectWrites != 0 || report.Resolution.Conflicts != 0 {
			mt.Fatalf("resolution = %+v", report.Resolution)
		}
		assertOrganisationsBackfillTokensReadOnly(mt, report)
	})

	mt.Run("legacy sub-user resolves default project", func(mt *mtest.T) {
		tokenID := primitive.NewObjectID()
		creatorID := primitive.NewObjectID()
		organisationID := primitive.NewObjectID()
		tokensNamespace := mt.DB.Name() + ".tokens"
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, tokensNamespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: tokenID},
				{Key: "userId", Value: creatorID.Hex()},
				{Key: "expiration", Value: int64(0)},
			}),
			mtest.CreateCursorResponse(0, mt.DB.Name()+".users", mtest.FirstBatch, bson.D{{Key: "_id", Value: creatorID}, {Key: "user_id", Value: organisationID.Hex()}}),
			mtest.CreateCursorResponse(0, mt.DB.Name()+".organisation", mtest.FirstBatch, bson.D{{Key: "_id", Value: organisationID}}),
			mtest.CreateCursorResponse(0, tokensNamespace, mtest.FirstBatch,
				bson.D{{Key: "name", Value: "_id_"}, {Key: "key", Value: bson.D{{Key: "_id", Value: int32(1)}}}},
			),
		)

		report := inspectOrganisationsBackfillTokensForTest(mt)
		if report.Resolution.CanonicalMissing != 1 || report.Resolution.Resolved != 1 || report.Resolution.ProjectResolved != 1 ||
			report.Resolution.ProposedWrites != 1 || report.Resolution.ProposedProjectWrites != 1 || report.Resolution.Conflicts != 0 {
			mt.Fatalf("resolution = %+v", report.Resolution)
		}
		assertOrganisationsBackfillTokensReadOnly(mt, report)
	})
}

func inspectOrganisationsBackfillTokensForTest(mt *mtest.T) organisationsBackfillCollection {
	report, err := inspectOrganisationsBackfillTokens(
		context.Background(),
		mt.DB,
		organisationsBackfillAdapters()["tokens"],
		OrganisationsBackfillConfig{BatchSize: 500},
		organisationsBackfillCollection{LegacyCandidateCount: map[string]int64{"userId": 1}},
	)
	if err != nil {
		mt.Fatalf("inspectOrganisationsBackfillTokens() error = %v", err)
	}
	return report
}

func assertOrganisationsBackfillTokensReadOnly(mt *mtest.T, report organisationsBackfillCollection) {
	if len(report.IndexContracts) != 3 || report.IndexContracts[0].Status != "missing" || report.IndexContracts[1].Status != "missing" || report.IndexContracts[2].Status != "exact" {
		mt.Fatalf("index contracts = %+v", report.IndexContracts)
	}
	for _, event := range mt.GetAllStartedEvents() {
		if event.CommandName != "find" && event.CommandName != "listIndexes" {
			mt.Fatalf("tokens dry-run issued non-read command %q", event.CommandName)
		}
	}
}
