package actions

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestResolveOrganisationsBackfillTokenPrecedenceAndProjects(t *testing.T) {
	organisationID := primitive.NewObjectID()
	otherOrganisationID := primitive.NewObjectID()
	creatorID := primitive.NewObjectID()
	projectID := primitive.NewObjectID()
	documentID := primitive.NewObjectID()
	organisations := map[primitive.ObjectID]bool{organisationID: true, otherOrganisationID: true}
	users := map[primitive.ObjectID]bson.Raw{
		creatorID: organisationsBackfillTestRaw(t, bson.D{
			{Key: "_id", Value: creatorID},
			{Key: "user_id", Value: organisationID.Hex()},
			{Key: "organisationId", Value: otherOrganisationID},
		}),
	}

	t.Run("legacy sub-user resolves stable owner and default project", func(t *testing.T) {
		outcome := resolveOrganisationsBackfillToken(
			organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: documentID}, {Key: "userId", Value: creatorID.Hex()}}),
			users,
			organisations,
			map[primitive.ObjectID]primitive.ObjectID{},
		)
		if !outcome.resolved || outcome.resolvedID != organisationID || !outcome.proposedWrite ||
			!outcome.projectResolved || outcome.resolvedProjectID != organisationID || !outcome.proposedProjectWrite || len(outcome.conflicts) != 0 {
			t.Fatalf("outcome = %+v", outcome)
		}
	})

	t.Run("canonical ownership wins and keeps non-default project", func(t *testing.T) {
		outcome := resolveOrganisationsBackfillToken(
			organisationsBackfillTestRaw(t, bson.D{
				{Key: "_id", Value: documentID},
				{Key: "organisationId", Value: organisationID.Hex()},
				{Key: "projectId", Value: projectID},
				{Key: "userId", Value: "invalid-provenance"},
			}),
			users,
			organisations,
			map[primitive.ObjectID]primitive.ObjectID{projectID: organisationID},
		)
		if !outcome.canonicalValid || outcome.invalidLegacy || outcome.resolved || outcome.proposedWrite ||
			!outcome.projectResolved || outcome.resolvedProjectID != projectID || outcome.proposedProjectWrite || len(outcome.conflicts) != 0 {
			t.Fatalf("outcome = %+v", outcome)
		}
	})
}

func TestResolveOrganisationsBackfillTokenRejectsCrossOrganisationProject(t *testing.T) {
	organisationID := primitive.NewObjectID()
	otherOrganisationID := primitive.NewObjectID()
	projectID := primitive.NewObjectID()
	outcome := resolveOrganisationsBackfillToken(
		organisationsBackfillTestRaw(t, bson.D{
			{Key: "_id", Value: primitive.NewObjectID()},
			{Key: "organisationId", Value: organisationID.Hex()},
			{Key: "projectId", Value: projectID},
		}),
		map[primitive.ObjectID]bson.Raw{},
		map[primitive.ObjectID]bool{organisationID: true, otherOrganisationID: true},
		map[primitive.ObjectID]primitive.ObjectID{projectID: otherOrganisationID},
	)
	if len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "project-organisation-mismatch" {
		t.Fatalf("outcome = %+v", outcome)
	}
}
