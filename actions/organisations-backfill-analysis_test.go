package actions

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestResolveOrganisationsBackfillAnalysisUsesOrderedPrecedence(t *testing.T) {
	organisationID := primitive.NewObjectID()
	otherID := primitive.NewObjectID()
	organisations := map[primitive.ObjectID]bool{organisationID: true, otherID: true}

	canonical := resolveOrganisationsBackfillAliasedProjectResource(organisationsBackfillTestRaw(t, bson.D{
		{Key: "_id", Value: primitive.NewObjectID()},
		{Key: "organisationId", Value: organisationID.Hex()},
		{Key: "userid", Value: otherID.Hex()},
	}), "analysis", []string{"userid", "user_id"}, organisations, nil)
	if !canonical.canonicalValid || canonical.resolved || len(canonical.conflicts) != 0 || canonical.resolvedProjectID != organisationID {
		t.Fatalf("canonical outcome = %+v", canonical)
	}

	legacy := resolveOrganisationsBackfillAliasedProjectResource(organisationsBackfillTestRaw(t, bson.D{
		{Key: "_id", Value: primitive.NewObjectID()},
		{Key: "userid", Value: organisationID.Hex()},
		{Key: "user_id", Value: otherID.Hex()},
	}), "analysis", []string{"userid", "user_id"}, organisations, nil)
	if !legacy.resolved || legacy.resolvedID != organisationID || !legacy.proposedWrite || len(legacy.conflicts) != 0 {
		t.Fatalf("legacy outcome = %+v", legacy)
	}
}

func TestResolveOrganisationsBackfillAnalysisValidatesNonDefaultProject(t *testing.T) {
	organisationID := primitive.NewObjectID()
	projectID := primitive.NewObjectID()
	outcome := resolveOrganisationsBackfillAliasedProjectResource(organisationsBackfillTestRaw(t, bson.D{
		{Key: "_id", Value: primitive.NewObjectID()},
		{Key: "organisationId", Value: organisationID.Hex()},
		{Key: "projectId", Value: projectID},
	}), "analysis", []string{"userid", "user_id"}, map[primitive.ObjectID]bool{organisationID: true}, map[primitive.ObjectID]primitive.ObjectID{projectID: organisationID})
	if !outcome.projectResolved || outcome.resolvedProjectID != projectID || len(outcome.conflicts) != 0 {
		t.Fatalf("outcome = %+v", outcome)
	}
}
