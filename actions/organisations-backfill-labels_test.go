package actions

import (
	"reflect"
	"testing"

	"github.com/uug-ai/cli/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestBuildDefaultLabelDocumentStampsDefaultOwnership(t *testing.T) {
	ownerId := primitive.NewObjectID()
	document, err := buildDefaultLabelDocument(models.Label{OwnerId: ownerId.Hex(), UserId: primitive.NewObjectID().Hex(), Name: "Incident"})
	if err != nil {
		t.Fatalf("build label: %v", err)
	}
	if document.OrganisationId != ownerId.Hex() || document.ProjectId != ownerId || document.UserId == document.OwnerId {
		t.Fatalf("default label document = %+v", document)
	}
}

func TestDefaultLabelMatchUsesCanonicalFirstDefaultScope(t *testing.T) {
	ownerId := primitive.NewObjectID()
	match, err := defaultLabelMatch(ownerId.Hex(), "Incident")
	if err != nil {
		t.Fatalf("build match: %v", err)
	}
	arms := match["$or"].([]bson.M)
	if arms[0]["organisationId"] != ownerId.Hex() || arms[1]["owner_id"] != ownerId.Hex() {
		t.Fatalf("ownership match = %#v", match)
	}
	wantProject := bson.M{"$in": bson.A{ownerId, nil}}
	if !reflect.DeepEqual(arms[0]["projectId"], wantProject) || !reflect.DeepEqual(arms[1]["projectId"], wantProject) {
		t.Fatalf("project match = %#v", match)
	}
}

func TestDefaultLabelNamesDropsEmptyValues(t *testing.T) {
	if got := defaultLabelNames(" , "); !reflect.DeepEqual(got, []string{"Incident", "suspicious", "unauthorized"}) {
		t.Fatalf("default names = %#v", got)
	}
}

func TestResolveOrganisationsBackfillLabelUsesStableOwner(t *testing.T) {
	organisationId := primitive.NewObjectID()
	document := organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "owner_id", Value: organisationId.Hex()}, {Key: "user_id", Value: primitive.NewObjectID().Hex()}})
	outcome := resolveOrganisationsBackfillProjectResource(document, "label", "owner_id", map[primitive.ObjectID]bool{organisationId: true}, nil)
	if !outcome.resolved || outcome.resolvedID != organisationId || !outcome.projectResolved || outcome.resolvedProjectID != organisationId || len(outcome.conflicts) != 0 {
		t.Fatalf("label outcome = %+v", outcome)
	}
}

func TestResolveOrganisationsBackfillLabelCanonicalWins(t *testing.T) {
	organisationId := primitive.NewObjectID()
	document := organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "organisationId", Value: organisationId.Hex()}, {Key: "owner_id", Value: "invalid"}})
	outcome := resolveOrganisationsBackfillProjectResource(document, "label", "owner_id", map[primitive.ObjectID]bool{organisationId: true}, nil)
	if !outcome.canonicalValid || outcome.resolved || outcome.invalidLegacy || !outcome.projectResolved || len(outcome.conflicts) != 0 {
		t.Fatalf("label outcome = %+v", outcome)
	}
}
