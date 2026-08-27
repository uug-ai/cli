package actions

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestResolveOrganisationsBackfillVideowallUsesStableTenant(t *testing.T) {
	organisationId := primitive.NewObjectID()
	document := organisationsBackfillTestRaw(t, bson.D{
		{Key: "_id", Value: primitive.NewObjectID()},
		{Key: "master_user_id", Value: organisationId.Hex()},
		{Key: "user_id", Value: primitive.NewObjectID().Hex()},
	})
	outcome := resolveOrganisationsBackfillProjectResource(document, "videowall", "master_user_id", map[primitive.ObjectID]bool{organisationId: true}, nil)
	if !outcome.resolved || outcome.resolvedID != organisationId || !outcome.projectResolved || outcome.resolvedProjectID != organisationId || len(outcome.conflicts) != 0 {
		t.Fatalf("videowall outcome = %+v", outcome)
	}
}

func TestResolveOrganisationsBackfillVideowallCanonicalWinsOverCreator(t *testing.T) {
	organisationId := primitive.NewObjectID()
	document := organisationsBackfillTestRaw(t, bson.D{
		{Key: "_id", Value: primitive.NewObjectID()},
		{Key: "organisationId", Value: organisationId.Hex()},
		{Key: "master_user_id", Value: "invalid"},
		{Key: "user_id", Value: primitive.NewObjectID().Hex()},
	})
	outcome := resolveOrganisationsBackfillProjectResource(document, "videowall", "master_user_id", map[primitive.ObjectID]bool{organisationId: true}, nil)
	if !outcome.canonicalValid || outcome.resolved || outcome.invalidLegacy || !outcome.projectResolved || len(outcome.conflicts) != 0 {
		t.Fatalf("videowall outcome = %+v", outcome)
	}
}
