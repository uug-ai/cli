package actions

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestOrganisationsBackfillNotificationShape(t *testing.T) {
	tests := []struct {
		name     string
		document bson.D
		want     string
	}{
		{name: "mailbox", document: bson.D{{Key: "user_id", Value: "507f1f77bcf86cd799439011"}, {Key: "data", Value: bson.A{bson.D{{Key: "id", Value: "notification"}}}}}, want: organisationsBackfillNotificationMailbox},
		{name: "flat event with data map", document: bson.D{{Key: "userid", Value: "507f1f77bcf86cd799439011"}, {Key: "data", Value: bson.D{{Key: "label", Value: "value"}}}}, want: organisationsBackfillNotificationFlat},
		{name: "flat canonical", document: bson.D{{Key: "organisationId", Value: "507f1f77bcf86cd799439011"}}, want: organisationsBackfillNotificationFlat},
		{name: "unknown", document: bson.D{{Key: "unexpected", Value: true}}, want: organisationsBackfillNotificationUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw, err := bson.Marshal(test.document)
			if err != nil {
				t.Fatal(err)
			}
			if got := organisationsBackfillNotificationShape(raw); got != test.want {
				t.Fatalf("shape = %q, want %q", got, test.want)
			}
		})
	}
}

func TestResolveOrganisationsBackfillNotification(t *testing.T) {
	organisationA := primitive.NewObjectID()
	organisationB := primitive.NewObjectID()
	recipientID := primitive.NewObjectID()
	documentID := primitive.NewObjectID()
	user := organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: recipientID}, {Key: "user_id", Value: organisationA.Hex()}})

	tests := []struct {
		name          string
		document      bson.D
		users         map[primitive.ObjectID]bson.Raw
		organisations map[primitive.ObjectID]bool
		projects      map[primitive.ObjectID]primitive.ObjectID
		check         func(*testing.T, organisationsBackfillAlertOutcome)
	}{
		{
			name: "canonical ownership wins over recipient",
			document: bson.D{
				{Key: "_id", Value: documentID},
				{Key: "organisationId", Value: organisationA.Hex()},
				{Key: "alert_master_user", Value: organisationB.Hex()},
				{Key: "userid", Value: recipientID.Hex()},
				{Key: "projectId", Value: organisationA},
			},
			organisations: map[primitive.ObjectID]bool{organisationA: true, organisationB: true},
			check: func(t *testing.T, outcome organisationsBackfillAlertOutcome) {
				if !outcome.canonicalValid || outcome.resolved || outcome.proposedWrite || !outcome.projectResolved || len(outcome.conflicts) != 0 {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:          "legacy master resolves default project",
			document:      bson.D{{Key: "_id", Value: documentID}, {Key: "alert_master_user", Value: organisationA.Hex()}, {Key: "userid", Value: recipientID.Hex()}},
			organisations: map[primitive.ObjectID]bool{organisationA: true},
			check: func(t *testing.T, outcome organisationsBackfillAlertOutcome) {
				if !outcome.resolved || outcome.resolvedID != organisationA || !outcome.proposedWrite || !outcome.projectResolved || !outcome.proposedProjectWrite || len(outcome.conflicts) != 0 {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:          "recipient resolves through stable owner",
			document:      bson.D{{Key: "_id", Value: documentID}, {Key: "userid", Value: recipientID.Hex()}},
			users:         map[primitive.ObjectID]bson.Raw{recipientID: user},
			organisations: map[primitive.ObjectID]bool{organisationA: true},
			check: func(t *testing.T, outcome organisationsBackfillAlertOutcome) {
				if !outcome.resolved || outcome.resolvedID != organisationA || !outcome.proposedWrite || len(outcome.conflicts) != 0 {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:          "recipient is provenance when master exists",
			document:      bson.D{{Key: "_id", Value: documentID}, {Key: "alert_master_user", Value: organisationA.Hex()}, {Key: "userid", Value: "invalid"}},
			organisations: map[primitive.ObjectID]bool{organisationA: true},
			check: func(t *testing.T, outcome organisationsBackfillAlertOutcome) {
				if !outcome.resolved || outcome.invalidLegacy || len(outcome.conflicts) != 0 {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:          "orphan recipient conflicts",
			document:      bson.D{{Key: "_id", Value: documentID}, {Key: "userid", Value: recipientID.Hex()}},
			organisations: map[primitive.ObjectID]bool{organisationA: true},
			check: func(t *testing.T, outcome organisationsBackfillAlertOutcome) {
				if !outcome.orphanUser || len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "orphan-user" {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:          "cross organisation project conflicts",
			document:      bson.D{{Key: "_id", Value: documentID}, {Key: "alert_master_user", Value: organisationA.Hex()}, {Key: "projectId", Value: organisationB}},
			organisations: map[primitive.ObjectID]bool{organisationA: true},
			projects:      map[primitive.ObjectID]primitive.ObjectID{organisationB: organisationB},
			check: func(t *testing.T, outcome organisationsBackfillAlertOutcome) {
				if len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "project-organisation-mismatch" {
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
			if test.projects == nil {
				test.projects = map[primitive.ObjectID]primitive.ObjectID{}
			}
			outcome := resolveOrganisationsBackfillNotification(
				organisationsBackfillTestRaw(t, test.document),
				test.users,
				test.organisations,
				test.projects,
			)
			test.check(t, outcome)
		})
	}
}
