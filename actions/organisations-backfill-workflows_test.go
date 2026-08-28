package actions

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestResolveOrganisationsBackfillWorkflowPrecedence(t *testing.T) {
	organisationID := primitive.NewObjectID()
	creatorID := primitive.NewObjectID()
	organisations := map[primitive.ObjectID]bool{organisationID: true}
	users := map[primitive.ObjectID]bson.Raw{creatorID: organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: creatorID}, {Key: "user_id", Value: organisationID.Hex()}})}

	t.Run("canonical preserves malformed creator provenance", func(t *testing.T) {
		outcome := resolveOrganisationsBackfillWorkflow(organisationsBackfillTestRaw(t, bson.D{
			{Key: "_id", Value: primitive.NewObjectID()},
			{Key: "organisationId", Value: organisationID.Hex()},
			{Key: "organisation_id", Value: "stale"},
			{Key: "user_id", Value: "invalid"},
		}), users, organisations, nil)
		if !outcome.canonicalValid || outcome.invalidLegacy || len(outcome.conflicts) != 0 || !outcome.projectResolved || outcome.resolvedProjectID != organisationID {
			t.Fatalf("outcome = %+v", outcome)
		}
	})

	t.Run("legacy organisation precedes creator", func(t *testing.T) {
		outcome := resolveOrganisationsBackfillWorkflow(organisationsBackfillTestRaw(t, bson.D{
			{Key: "_id", Value: primitive.NewObjectID()},
			{Key: "organisation_id", Value: organisationID.Hex()},
			{Key: "user_id", Value: "invalid"},
		}), users, organisations, nil)
		if !outcome.resolved || outcome.resolvedID != organisationID || outcome.invalidLegacy || !outcome.proposedWrite || len(outcome.conflicts) != 0 {
			t.Fatalf("outcome = %+v", outcome)
		}
	})

	t.Run("creator resolves only when both organisation fields are absent", func(t *testing.T) {
		outcome := resolveOrganisationsBackfillWorkflow(organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "user_id", Value: creatorID.Hex()}}), users, organisations, nil)
		if !outcome.resolved || outcome.resolvedID != organisationID || outcome.orphanUser || len(outcome.conflicts) != 0 {
			t.Fatalf("outcome = %+v", outcome)
		}
	})
}

func TestResolveOrganisationsBackfillWorkflowValidatesExplicitProject(t *testing.T) {
	organisationID := primitive.NewObjectID()
	otherOrganisationID := primitive.NewObjectID()
	projectID := primitive.NewObjectID()
	outcome := resolveOrganisationsBackfillWorkflow(organisationsBackfillTestRaw(t, bson.D{
		{Key: "_id", Value: primitive.NewObjectID()},
		{Key: "organisationId", Value: organisationID.Hex()},
		{Key: "projectId", Value: projectID},
	}), nil, map[primitive.ObjectID]bool{organisationID: true}, map[primitive.ObjectID]primitive.ObjectID{projectID: otherOrganisationID})
	if len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "project-organisation-mismatch" {
		t.Fatalf("outcome = %+v", outcome)
	}
}
