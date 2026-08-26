package actions

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestResolveOrganisationsBackfillIOPrecedence(t *testing.T) {
	organisationID := primitive.NewObjectID()
	actorID := primitive.NewObjectID()
	documentID := primitive.NewObjectID()
	organisations := map[primitive.ObjectID]bool{organisationID: true}
	users := map[primitive.ObjectID]bson.Raw{
		actorID: organisationsBackfillTestRaw(t, bson.D{
			{Key: "_id", Value: actorID},
			{Key: "user_id", Value: organisationID.Hex()},
		}),
	}

	t.Run("legacy actor resolves stable default ownership", func(t *testing.T) {
		outcome := resolveOrganisationsBackfillIO(
			organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: documentID}, {Key: "user_id", Value: actorID.Hex()}}),
			users,
			organisations,
			map[primitive.ObjectID]primitive.ObjectID{},
		)
		if !outcome.resolved || outcome.resolvedID != organisationID || !outcome.proposedWrite ||
			!outcome.projectResolved || outcome.resolvedProjectID != organisationID || !outcome.proposedProjectWrite || len(outcome.conflicts) != 0 {
			t.Fatalf("outcome = %+v", outcome)
		}
	})

	t.Run("canonical ownership ignores malformed actor provenance", func(t *testing.T) {
		outcome := resolveOrganisationsBackfillIO(
			organisationsBackfillTestRaw(t, bson.D{
				{Key: "_id", Value: documentID},
				{Key: "organisationId", Value: organisationID.Hex()},
				{Key: "user_id", Value: "invalid"},
			}),
			users,
			organisations,
			map[primitive.ObjectID]primitive.ObjectID{},
		)
		if !outcome.canonicalValid || outcome.invalidLegacy || !outcome.projectResolved ||
			outcome.resolvedProjectID != organisationID || len(outcome.conflicts) != 0 {
			t.Fatalf("outcome = %+v", outcome)
		}
	})
}

func TestResolveOrganisationsBackfillIOConflicts(t *testing.T) {
	organisationID := primitive.NewObjectID()
	otherOrganisationID := primitive.NewObjectID()
	projectID := primitive.NewObjectID()
	actorID := primitive.NewObjectID()
	documentID := primitive.NewObjectID()
	organisations := map[primitive.ObjectID]bool{organisationID: true, otherOrganisationID: true}

	t.Run("legacy actor must resolve to a persisted user", func(t *testing.T) {
		outcome := resolveOrganisationsBackfillIO(
			organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: documentID}, {Key: "user_id", Value: actorID.Hex()}}),
			map[primitive.ObjectID]bson.Raw{},
			organisations,
			map[primitive.ObjectID]primitive.ObjectID{},
		)
		if !outcome.orphanUser || len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "orphan-user" {
			t.Fatalf("outcome = %+v", outcome)
		}
	})

	t.Run("project must belong to canonical organisation", func(t *testing.T) {
		outcome := resolveOrganisationsBackfillIO(
			organisationsBackfillTestRaw(t, bson.D{
				{Key: "_id", Value: documentID},
				{Key: "organisationId", Value: organisationID.Hex()},
				{Key: "projectId", Value: projectID},
			}),
			map[primitive.ObjectID]bson.Raw{},
			organisations,
			map[primitive.ObjectID]primitive.ObjectID{projectID: otherOrganisationID},
		)
		if len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "project-organisation-mismatch" {
			t.Fatalf("outcome = %+v", outcome)
		}
	})
}
