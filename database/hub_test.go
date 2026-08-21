package database

import (
	"reflect"
	"testing"
	"time"

	"github.com/uug-ai/models/pkg/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestSubscriptionOwnershipFilterCanonicalPrecedence(t *testing.T) {
	actorID := primitive.NewObjectID()
	legacyOwnerID := primitive.NewObjectID()
	organisationID := primitive.NewObjectID()
	user := models.User{
		Id:             actorID,
		OrganisationId: organisationID,
		MasterAccount:  legacyOwnerID.Hex(),
	}

	want := bson.M{"$or": bson.A{
		bson.M{"organisation_id": organisationID},
		bson.M{
			"organisation_id": bson.M{"$exists": false},
			"user_id":         legacyOwnerID.Hex(),
		},
	}}
	if got := SubscriptionOwnershipFilter(user); !reflect.DeepEqual(got, want) {
		t.Fatalf("SubscriptionOwnershipFilter() = %#v, want %#v", got, want)
	}
}

func TestSubscriptionOwnershipFilterDerivesOrganisationFromStableLegacyOwner(t *testing.T) {
	actorID := primitive.NewObjectID()
	legacyOwnerID := primitive.NewObjectID()

	tests := []struct {
		name string
		user models.User
	}{
		{
			name: "sub-user uses master identity",
			user: models.User{Id: actorID, MasterAccount: legacyOwnerID.Hex()},
		},
		{
			name: "master uses account identity",
			user: models.User{Id: legacyOwnerID},
		},
	}

	want := bson.M{"$or": bson.A{
		bson.M{"organisation_id": legacyOwnerID},
		bson.M{
			"organisation_id": bson.M{"$exists": false},
			"user_id":         legacyOwnerID.Hex(),
		},
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := SubscriptionOwnershipFilter(test.user); !reflect.DeepEqual(got, want) {
				t.Fatalf("SubscriptionOwnershipFilter() = %#v, want %#v", got, want)
			}
		})
	}
}

func TestSubscriptionOwnershipFilterDoesNotWidenStaleLegacyOwnership(t *testing.T) {
	organisationID := primitive.NewObjectID()
	legacyOwnerID := primitive.NewObjectID()
	user := models.User{OrganisationId: organisationID, MasterAccount: legacyOwnerID.Hex()}

	filter := SubscriptionOwnershipFilter(user)
	legacyBranch := filter["$or"].(bson.A)[1].(bson.M)
	wantGuard := bson.M{"$exists": false}
	if got := legacyBranch["organisation_id"]; !reflect.DeepEqual(got, wantGuard) {
		t.Fatalf("legacy organisation_id guard = %#v, want %#v", got, wantGuard)
	}
	if got := legacyBranch["user_id"]; got != legacyOwnerID.Hex() {
		t.Fatalf("legacy user_id = %#v, want %q", got, legacyOwnerID.Hex())
	}
}

func TestActiveSubscriptionFilterPreservesEndsAtPredicate(t *testing.T) {
	now := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	organisationID := primitive.NewObjectID()
	user := models.User{Id: organisationID, OrganisationId: organisationID}

	want := bson.M{"$and": bson.A{
		bson.M{"$or": bson.A{
			bson.M{"organisation_id": organisationID},
			bson.M{
				"organisation_id": bson.M{"$exists": false},
				"user_id":         organisationID.Hex(),
			},
		}},
		bson.M{"$or": bson.A{
			bson.M{"ends_at": bson.M{"$gt": now}},
			bson.M{"ends_at": nil},
		}},
	}}
	if got := activeSubscriptionFilter(user, now); !reflect.DeepEqual(got, want) {
		t.Fatalf("activeSubscriptionFilter() = %#v, want %#v", got, want)
	}
}
