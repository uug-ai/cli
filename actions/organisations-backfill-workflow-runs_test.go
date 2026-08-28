package actions

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestResolveOrganisationsBackfillWorkflowRunRelationships(t *testing.T) {
	organisationID := primitive.NewObjectID()
	projectID := primitive.NewObjectID()
	taskID := primitive.NewObjectID()
	workflowID := primitive.NewObjectID()
	document := organisationsBackfillTestRaw(t, bson.D{
		{Key: "_id", Value: primitive.NewObjectID()},
		{Key: "organisationId", Value: organisationID.Hex()},
		{Key: "projectId", Value: projectID},
		{Key: "userid", Value: "stale"},
		{Key: "user_id", Value: "actor-provenance"},
		{Key: "sourceref", Value: taskID.Hex()},
		{Key: "workflowid", Value: workflowID.Hex()},
	})
	tasks := map[primitive.ObjectID]bson.Raw{taskID: organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: taskID}, {Key: "organisationId", Value: organisationID.Hex()}, {Key: "projectId", Value: projectID}})}
	workflows := map[primitive.ObjectID]bson.Raw{workflowID: organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: workflowID}, {Key: "organisationId", Value: organisationID.Hex()}, {Key: "projectId", Value: projectID}})}
	outcome := resolveOrganisationsBackfillWorkflowRun(document, map[primitive.ObjectID]bool{organisationID: true}, map[primitive.ObjectID]primitive.ObjectID{projectID: organisationID}, tasks, nil, workflows, nil)
	if !outcome.canonicalValid || !outcome.accidentalUserPresent || outcome.invalidLegacy || !outcome.projectResolved || len(outcome.conflicts) != 0 {
		t.Fatalf("outcome = %+v", outcome)
	}
}

func TestResolveOrganisationsBackfillWorkflowRunConfigDefinitionMayBeAbsent(t *testing.T) {
	organisationID := primitive.NewObjectID()
	document := organisationsBackfillTestRaw(t, bson.D{
		{Key: "_id", Value: primitive.NewObjectID()},
		{Key: "userid", Value: organisationID.Hex()},
		{Key: "key", Value: "media-1"},
		{Key: "workflowid", Value: "config-workflow"},
	})
	media := map[string]bson.Raw{"media-1": organisationsBackfillTestRaw(t, bson.D{{Key: "key", Value: "media-1"}, {Key: "organisationId", Value: organisationID.Hex()}})}
	outcome := resolveOrganisationsBackfillWorkflowRun(document, map[primitive.ObjectID]bool{organisationID: true}, nil, nil, media, nil, nil)
	if !outcome.resolved || !outcome.projectResolved || !outcome.proposedProjectWrite || len(outcome.conflicts) != 0 {
		t.Fatalf("outcome = %+v", outcome)
	}
}

func TestResolveOrganisationsBackfillWorkflowRunReportsSourceAndDefinitionConflicts(t *testing.T) {
	organisationID := primitive.NewObjectID()
	otherOrganisationID := primitive.NewObjectID()
	workflowID := primitive.NewObjectID()
	document := organisationsBackfillTestRaw(t, bson.D{
		{Key: "_id", Value: primitive.NewObjectID()},
		{Key: "organisationId", Value: organisationID.Hex()},
		{Key: "key", Value: "missing-media"},
		{Key: "workflowid", Value: workflowID.Hex()},
	})
	workflows := map[primitive.ObjectID]bson.Raw{workflowID: organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: workflowID}, {Key: "organisationId", Value: otherOrganisationID.Hex()}})}
	outcome := resolveOrganisationsBackfillWorkflowRun(document, map[primitive.ObjectID]bool{organisationID: true}, nil, nil, nil, workflows, nil)
	if len(outcome.conflicts) != 2 || outcome.conflicts[0].Code != "unresolved-source" || outcome.conflicts[1].Code != "definition-organisation-mismatch" {
		t.Fatalf("outcome = %+v", outcome)
	}
}
