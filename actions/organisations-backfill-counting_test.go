package actions

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestResolveOrganisationsBackfillCountingUsesStableTenant(t *testing.T) {
	organisationId := primitive.NewObjectID()
	projectId := primitive.NewObjectID()
	document := organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "user_id", Value: organisationId.Hex()}, {Key: "projectId", Value: projectId}})
	outcome := resolveOrganisationsBackfillProjectResource(document, "counting", "user_id", map[primitive.ObjectID]bool{organisationId: true}, map[primitive.ObjectID]primitive.ObjectID{projectId: organisationId})
	if !outcome.resolved || outcome.resolvedID != organisationId || !outcome.projectResolved || outcome.resolvedProjectID != projectId || len(outcome.conflicts) != 0 {
		t.Fatalf("counting outcome = %+v", outcome)
	}
}
