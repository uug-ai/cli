package actions

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestResolveOrganisationsBackfillGroupPrecedence(t *testing.T) {
	organisationID := primitive.NewObjectID()
	otherOrganisationID := primitive.NewObjectID()
	projectID := primitive.NewObjectID()
	documentID := primitive.NewObjectID()
	organisations := map[primitive.ObjectID]bool{organisationID: true, otherOrganisationID: true}

	tests := []struct {
		name     string
		document bson.D
		projects map[primitive.ObjectID]primitive.ObjectID
		check    func(*testing.T, organisationsBackfillProjectResourceOutcome)
	}{
		{
			name:     "legacy tenant resolves both default axes",
			document: bson.D{{Key: "_id", Value: documentID}, {Key: "user_id", Value: organisationID.Hex()}},
			check: func(t *testing.T, outcome organisationsBackfillProjectResourceOutcome) {
				if !outcome.resolved || outcome.resolvedID != organisationID || !outcome.proposedWrite || !outcome.projectResolved || outcome.resolvedProjectID != organisationID || !outcome.proposedProjectWrite || len(outcome.conflicts) != 0 {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name: "canonical ownership ignores malformed lower precedence tenant",
			document: bson.D{
				{Key: "_id", Value: documentID},
				{Key: "organisationId", Value: organisationID.Hex()},
				{Key: "user_id", Value: "invalid"},
			},
			check: func(t *testing.T, outcome organisationsBackfillProjectResourceOutcome) {
				if !outcome.canonicalValid || outcome.invalidLegacy || !outcome.projectResolved || len(outcome.conflicts) != 0 {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name: "non-default project in organisation is preserved",
			document: bson.D{
				{Key: "_id", Value: documentID},
				{Key: "organisationId", Value: organisationID.Hex()},
				{Key: "projectId", Value: projectID},
			},
			projects: map[primitive.ObjectID]primitive.ObjectID{projectID: organisationID},
			check: func(t *testing.T, outcome organisationsBackfillProjectResourceOutcome) {
				if !outcome.projectResolved || outcome.resolvedProjectID != projectID || outcome.proposedProjectWrite || len(outcome.conflicts) != 0 {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name: "cross-organisation project conflicts",
			document: bson.D{
				{Key: "_id", Value: documentID},
				{Key: "organisationId", Value: organisationID.Hex()},
				{Key: "projectId", Value: projectID},
			},
			projects: map[primitive.ObjectID]primitive.ObjectID{projectID: otherOrganisationID},
			check: func(t *testing.T, outcome organisationsBackfillProjectResourceOutcome) {
				if len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "project-organisation-mismatch" {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.projects == nil {
				test.projects = map[primitive.ObjectID]primitive.ObjectID{}
			}
			outcome := resolveOrganisationsBackfillGroup(organisationsBackfillTestRaw(t, test.document), organisations, test.projects)
			test.check(t, outcome)
		})
	}
}

func TestResolveOrganisationsBackfillGroupUsesGroupConflictLabel(t *testing.T) {
	outcome := resolveOrganisationsBackfillGroup(
		organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: primitive.NewObjectID()}}),
		map[primitive.ObjectID]bool{},
		map[primitive.ObjectID]primitive.ObjectID{},
	)
	if len(outcome.conflicts) != 1 || outcome.conflicts[0].Message != "group has neither canonical organisationId nor legacy user_id" {
		t.Fatalf("outcome = %+v", outcome)
	}
}
