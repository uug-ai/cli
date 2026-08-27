package actions

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestResolveOrganisationsBackfillDetectionDerivesSourceOwnership(t *testing.T) {
	organisationID := primitive.NewObjectID()
	projectID := primitive.NewObjectID()
	document := organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "key", Value: "recording"}})
	sources := map[string]bson.Raw{"recording": organisationsBackfillTestRaw(t, bson.D{
		{Key: "_id", Value: primitive.NewObjectID()},
		{Key: "key", Value: "recording"},
		{Key: "organisationId", Value: organisationID.Hex()},
		{Key: "projectId", Value: projectID},
	})}
	outcome := resolveOrganisationsBackfillDetection(document, sources, map[primitive.ObjectID]bool{organisationID: true}, map[primitive.ObjectID]primitive.ObjectID{projectID: organisationID})
	if !outcome.resolved || outcome.resolvedID != organisationID || !outcome.projectResolved || outcome.resolvedProjectID != projectID || !outcome.proposedWrite || !outcome.proposedProjectWrite || outcome.canonicalValid || outcome.projectPresent || len(outcome.conflicts) != 0 {
		t.Fatalf("outcome = %+v", outcome)
	}
}

func TestResolveOrganisationsBackfillDetectionReportsSourceMismatch(t *testing.T) {
	organisationID := primitive.NewObjectID()
	otherID := primitive.NewObjectID()
	document := organisationsBackfillTestRaw(t, bson.D{
		{Key: "_id", Value: primitive.NewObjectID()},
		{Key: "key", Value: "recording"},
		{Key: "organisationId", Value: organisationID.Hex()},
	})
	sources := map[string]bson.Raw{"recording": organisationsBackfillTestRaw(t, bson.D{{Key: "key", Value: "recording"}, {Key: "organisationId", Value: otherID.Hex()}})}
	outcome := resolveOrganisationsBackfillDetection(document, sources, map[primitive.ObjectID]bool{organisationID: true, otherID: true}, nil)
	if len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "source-organisation-mismatch" {
		t.Fatalf("outcome = %+v", outcome)
	}
}

func TestResolveOrganisationsBackfillDetectionReportsMissingSource(t *testing.T) {
	document := organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "key", Value: "missing"}})
	outcome := resolveOrganisationsBackfillDetection(document, nil, nil, nil)
	if !outcome.zeroCandidate || len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "unresolved-source" {
		t.Fatalf("outcome = %+v", outcome)
	}
}
