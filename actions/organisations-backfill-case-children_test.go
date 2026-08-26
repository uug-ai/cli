package actions

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestResolveOrganisationsBackfillCaseChildInheritsParentOwnership(t *testing.T) {
	organisationID := primitive.NewObjectID()
	projectID := primitive.NewObjectID()
	taskID := primitive.NewObjectID()
	childID := primitive.NewObjectID()
	organisations := map[primitive.ObjectID]bool{organisationID: true}
	projects := map[primitive.ObjectID]primitive.ObjectID{projectID: organisationID}
	tasks := map[primitive.ObjectID]bson.Raw{
		taskID: organisationsBackfillTestRaw(t, bson.D{
			{Key: "_id", Value: taskID},
			{Key: "organisationId", Value: organisationID.Hex()},
			{Key: "projectId", Value: projectID},
		}),
	}

	outcome := resolveOrganisationsBackfillCaseChild(
		organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: childID}, {Key: "task_id", Value: taskID}}),
		"case media",
		tasks,
		organisations,
		projects,
	)
	if !outcome.resolved || outcome.resolvedID != organisationID || !outcome.proposedWrite ||
		!outcome.projectResolved || outcome.resolvedProjectID != projectID || !outcome.proposedProjectWrite || len(outcome.conflicts) != 0 {
		t.Fatalf("outcome = %+v", outcome)
	}
}

func TestResolveOrganisationsBackfillCaseChildUsesLegacyParentDefault(t *testing.T) {
	organisationID := primitive.NewObjectID()
	taskID := primitive.NewObjectID()
	tasks := map[primitive.ObjectID]bson.Raw{
		taskID: organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: taskID}, {Key: "user_id", Value: organisationID.Hex()}}),
	}
	outcome := resolveOrganisationsBackfillCaseChild(
		organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "task_id", Value: taskID}}),
		"case attachment",
		tasks,
		map[primitive.ObjectID]bool{organisationID: true},
		map[primitive.ObjectID]primitive.ObjectID{},
	)
	if outcome.resolvedID != organisationID || outcome.resolvedProjectID != organisationID || len(outcome.conflicts) != 0 {
		t.Fatalf("outcome = %+v", outcome)
	}
}

func TestResolveOrganisationsBackfillCaseChildRejectsParentMismatches(t *testing.T) {
	organisationID := primitive.NewObjectID()
	otherOrganisationID := primitive.NewObjectID()
	projectID := primitive.NewObjectID()
	taskID := primitive.NewObjectID()
	tasks := map[primitive.ObjectID]bson.Raw{
		taskID: organisationsBackfillTestRaw(t, bson.D{
			{Key: "_id", Value: taskID},
			{Key: "organisationId", Value: organisationID.Hex()},
			{Key: "projectId", Value: projectID},
		}),
	}
	outcome := resolveOrganisationsBackfillCaseChild(
		organisationsBackfillTestRaw(t, bson.D{
			{Key: "_id", Value: primitive.NewObjectID()},
			{Key: "task_id", Value: taskID},
			{Key: "organisation_id", Value: otherOrganisationID.Hex()},
			{Key: "projectId", Value: primitive.NewObjectID()},
		}),
		"case media",
		tasks,
		map[primitive.ObjectID]bool{organisationID: true, otherOrganisationID: true},
		map[primitive.ObjectID]primitive.ObjectID{projectID: organisationID},
	)
	if len(outcome.conflicts) != 2 || outcome.conflicts[0].Code != "parent-organisation-mismatch" || outcome.conflicts[1].Code != "parent-project-mismatch" {
		t.Fatalf("outcome = %+v", outcome)
	}
}

func TestResolveOrganisationsBackfillCaseChildRejectsOrphanParent(t *testing.T) {
	outcome := resolveOrganisationsBackfillCaseChild(
		organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "task_id", Value: primitive.NewObjectID()}}),
		"case attachment",
		map[primitive.ObjectID]bson.Raw{},
		map[primitive.ObjectID]bool{},
		map[primitive.ObjectID]primitive.ObjectID{},
	)
	if !outcome.orphanParent || len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "orphan-parent-task" {
		t.Fatalf("outcome = %+v", outcome)
	}
}
