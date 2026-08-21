package actions

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestResolveOrganisationsBackfillAlertPrecedence(t *testing.T) {
	organisationA := primitive.NewObjectID()
	organisationB := primitive.NewObjectID()
	creatorID := primitive.NewObjectID()
	documentID := primitive.NewObjectID()

	tests := []struct {
		name          string
		document      bson.D
		users         map[primitive.ObjectID]bson.Raw
		organisations map[primitive.ObjectID]bool
		check         func(*testing.T, organisationsBackfillAlertOutcome)
	}{
		{
			name: "canonical ownership ignores lower precedence fields",
			document: bson.D{
				{Key: "_id", Value: documentID},
				{Key: "organisationId", Value: organisationA.Hex()},
				{Key: "master_user_id", Value: organisationB.Hex()},
				{Key: "user_id", Value: creatorID.Hex()},
			},
			organisations: map[primitive.ObjectID]bool{organisationA: true, organisationB: true},
			check: func(t *testing.T, outcome organisationsBackfillAlertOutcome) {
				if !outcome.canonicalValid || outcome.resolved || outcome.proposedWrite || len(outcome.conflicts) != 0 {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:          "object id canonical value is authoritative but wrong type",
			document:      bson.D{{Key: "_id", Value: documentID}, {Key: "organisationId", Value: organisationA}},
			organisations: map[primitive.ObjectID]bool{organisationA: true},
			check: func(t *testing.T, outcome organisationsBackfillAlertOutcome) {
				if !outcome.canonicalWrong || outcome.canonicalValid || len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "invalid-canonical-organisation" {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name: "master tenant wins over creator",
			document: bson.D{
				{Key: "_id", Value: documentID},
				{Key: "master_user_id", Value: organisationA.Hex()},
				{Key: "user_id", Value: creatorID.Hex()},
			},
			organisations: map[primitive.ObjectID]bool{organisationA: true},
			check: func(t *testing.T, outcome organisationsBackfillAlertOutcome) {
				if !outcome.resolved || outcome.resolvedID != organisationA || !outcome.proposedWrite ||
					!outcome.legacyMasterPresent || !outcome.legacyUserPresent || !outcome.projectMissing || len(outcome.conflicts) != 0 {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:          "oldest user field can directly name tenant",
			document:      bson.D{{Key: "_id", Value: documentID}, {Key: "user_id", Value: organisationA.Hex()}},
			organisations: map[primitive.ObjectID]bool{organisationA: true},
			check: func(t *testing.T, outcome organisationsBackfillAlertOutcome) {
				if !outcome.resolved || outcome.resolvedID != organisationA || len(outcome.conflicts) != 0 {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:     "creator resolves through stable parent",
			document: bson.D{{Key: "_id", Value: documentID}, {Key: "user_id", Value: creatorID.Hex()}},
			users: map[primitive.ObjectID]bson.Raw{
				creatorID: organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: creatorID}, {Key: "user_id", Value: organisationA.Hex()}}),
			},
			organisations: map[primitive.ObjectID]bool{organisationA: true},
			check: func(t *testing.T, outcome organisationsBackfillAlertOutcome) {
				if !outcome.resolved || outcome.resolvedID != organisationA || len(outcome.conflicts) != 0 {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:     "creator with direct and parent tenants is ambiguous",
			document: bson.D{{Key: "_id", Value: documentID}, {Key: "user_id", Value: creatorID.Hex()}},
			users: map[primitive.ObjectID]bson.Raw{
				creatorID: organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: creatorID}, {Key: "user_id", Value: organisationB.Hex()}}),
			},
			organisations: map[primitive.ObjectID]bool{creatorID: true, organisationB: true},
			check: func(t *testing.T, outcome organisationsBackfillAlertOutcome) {
				if !outcome.multipleCandidates || len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "multiple-candidates" {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name: "invalid master blocks creator fallback",
			document: bson.D{
				{Key: "_id", Value: documentID},
				{Key: "master_user_id", Value: "invalid"},
				{Key: "user_id", Value: organisationA.Hex()},
			},
			organisations: map[primitive.ObjectID]bool{organisationA: true},
			check: func(t *testing.T, outcome organisationsBackfillAlertOutcome) {
				if !outcome.invalidLegacy || outcome.resolved || len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "invalid-legacy-master-id" {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.users == nil {
				test.users = map[primitive.ObjectID]bson.Raw{}
			}
			if test.organisations == nil {
				test.organisations = map[primitive.ObjectID]bool{}
			}
			outcome := resolveOrganisationsBackfillAlert(
				organisationsBackfillTestRaw(t, test.document),
				test.users,
				test.organisations,
			)
			test.check(t, outcome)
		})
	}
}

func TestOrganisationsBackfillAlertScope(t *testing.T) {
	organisationID := primitive.NewObjectID()
	if !organisationsBackfillAlertInScope(organisationsBackfillAlertOutcome{resolvedID: organisationID}, organisationID) {
		t.Fatal("resolved legacy alert excluded from scope")
	}
	if organisationsBackfillAlertInScope(organisationsBackfillAlertOutcome{canonicalID: primitive.NewObjectID(), resolvedID: organisationID}, organisationID) {
		t.Fatal("lower-precedence resolution overrode canonical scope")
	}
}
