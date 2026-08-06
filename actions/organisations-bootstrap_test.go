package actions

import (
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestValidateOrganisationsBootstrapConfig(t *testing.T) {
	valid := normalizeOrganisationsBootstrapConfig(OrganisationsBootstrapConfig{
		Mode:                       "dry-run",
		Stage:                      "owners",
		MongoDBURI:                 "mongodb://localhost:27017",
		MongoDBDestinationDatabase: "hub",
	})

	tests := []struct {
		name      string
		mutate    func(OrganisationsBootstrapConfig) OrganisationsBootstrapConfig
		wantError string
	}{
		{name: "valid owners", mutate: func(config OrganisationsBootstrapConfig) OrganisationsBootstrapConfig { return config }},
		{name: "valid sub-users", mutate: func(config OrganisationsBootstrapConfig) OrganisationsBootstrapConfig {
			config.Stage = "sub-users"
			return config
		}},
		{name: "valid verify", mutate: func(config OrganisationsBootstrapConfig) OrganisationsBootstrapConfig {
			config.Stage = "verify"
			return config
		}},
		{name: "requires stage", mutate: func(config OrganisationsBootstrapConfig) OrganisationsBootstrapConfig {
			config.Stage = ""
			return config
		}, wantError: "stage must be"},
		{name: "verify rejects live mode", mutate: func(config OrganisationsBootstrapConfig) OrganisationsBootstrapConfig {
			config.Stage = "verify"
			config.Mode = "live"
			return config
		}, wantError: "verify is read-only"},
		{name: "owners support checkpointed live mode", mutate: func(config OrganisationsBootstrapConfig) OrganisationsBootstrapConfig {
			config.Mode = "live"
			return config
		}},
		{name: "rejects conflicting scopes", mutate: func(config OrganisationsBootstrapConfig) OrganisationsBootstrapConfig {
			config.Username = "owner"
			config.OrganisationID = "507f1f77bcf86cd799439011"
			return config
		}, wantError: "mutually exclusive"},
		{name: "resume requires live mode", mutate: func(config OrganisationsBootstrapConfig) OrganisationsBootstrapConfig {
			config.Resume = true
			return config
		}, wantError: "require -mode live"},
		{name: "rejects destructive legacy policy", mutate: func(config OrganisationsBootstrapConfig) OrganisationsBootstrapConfig {
			config.LegacyOrganisationPolicy = "archive-delete"
			return config
		}, wantError: "archive-delete is disabled"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateOrganisationsBootstrapConfig(test.mutate(valid))
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("validateOrganisationsBootstrapConfig() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("validateOrganisationsBootstrapConfig() error = %v, want containing %q", err, test.wantError)
			}
		})
	}
}

func TestOrganisationsBootstrapExitCode(t *testing.T) {
	err := &organisationsBootstrapError{code: organisationsBootstrapExitData, err: errTestDataConflict}
	if got := OrganisationsBootstrapExitCode(err); got != organisationsBootstrapExitData {
		t.Fatalf("OrganisationsBootstrapExitCode() = %d, want %d", got, organisationsBootstrapExitData)
	}
}

func TestOrganisationsBootstrapCheckpointIDIncludesStageAndScope(t *testing.T) {
	config := normalizeOrganisationsBootstrapConfig(OrganisationsBootstrapConfig{
		Stage:                      "owners",
		MongoDBDestinationDatabase: "hub",
		OrganisationID:             "507f1f77bcf86cd799439011",
	})
	ownerID := organisationsBootstrapCheckpointID(config)
	config.Stage = "sub-users"
	subUserID := organisationsBootstrapCheckpointID(config)
	if ownerID == subUserID {
		t.Fatalf("checkpoint IDs must differ by stage: %q", ownerID)
	}
	for _, part := range []string{"v1", "hub", "owners", config.OrganisationID} {
		if !strings.Contains(ownerID, part) {
			t.Errorf("owner checkpoint ID %q does not contain %q", ownerID, part)
		}
	}
}

func TestOrganisationsBootstrapResumeFilterPreservesScopedMaster(t *testing.T) {
	scopeID := primitive.ObjectID{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2}
	earlierID := primitive.ObjectID{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	filter := organisationsBootstrapResumeFilter(bson.M{"_id": scopeID}, earlierID)
	if filter["_id"] != scopeID {
		t.Fatalf("resume filter = %v, want pending exact scope %v", filter, scopeID)
	}
	completed := organisationsBootstrapResumeFilter(bson.M{"_id": scopeID}, scopeID)
	idFilter, ok := completed["_id"].(bson.M)
	if !ok || idFilter["$exists"] != false {
		t.Fatalf("completed scope resume filter = %v, want no match", completed)
	}
}

func TestOrganisationsBootstrapSlugIndexRequiresPartialUniqueContract(t *testing.T) {
	base := bson.M{
		"key":    bson.D{{Key: "slug", Value: int32(1)}},
		"unique": true,
	}
	if organisationsBootstrapHasSlugIndex([]bson.M{base}) {
		t.Fatal("full unique slug index must not satisfy the partial index contract")
	}
	base["partialFilterExpression"] = bson.M{"slug": bson.M{"$type": "string"}}
	if !organisationsBootstrapHasSlugIndex([]bson.M{base}) {
		t.Fatal("partial unique string slug index did not satisfy the contract")
	}
}

func TestParseOrganisationsBootstrapUser(t *testing.T) {
	masterID := primitive.NewObjectID()
	subUserID := primitive.NewObjectID()

	tests := []struct {
		name            string
		document        bson.M
		wantParentState organisationsBootstrapFieldState
		wantParentID    primitive.ObjectID
	}{
		{
			name:            "master without parent",
			document:        bson.M{"_id": masterID, "username": "owner"},
			wantParentState: organisationsBootstrapFieldEmpty,
		},
		{
			name:            "sub-user with valid parent",
			document:        bson.M{"_id": subUserID, "username": "viewer", "user_id": masterID.Hex()},
			wantParentState: organisationsBootstrapFieldValue,
			wantParentID:    masterID,
		},
		{
			name:            "sub-user with invalid parent hex",
			document:        bson.M{"_id": subUserID, "username": "viewer", "user_id": "invalid"},
			wantParentState: organisationsBootstrapFieldWrong,
		},
		{
			name:            "sub-user with wrong parent type",
			document:        bson.M{"_id": subUserID, "username": "viewer", "user_id": masterID},
			wantParentState: organisationsBootstrapFieldWrong,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw, err := bson.Marshal(test.document)
			if err != nil {
				t.Fatal(err)
			}
			user, err := parseOrganisationsBootstrapUser(raw)
			if err != nil {
				t.Fatalf("parseOrganisationsBootstrapUser() error = %v", err)
			}
			if user.ParentState != test.wantParentState {
				t.Errorf("ParentState = %v, want %v", user.ParentState, test.wantParentState)
			}
			if user.ParentID != test.wantParentID {
				t.Errorf("ParentID = %v, want %v", user.ParentID, test.wantParentID)
			}
		})
	}
}

func TestOrganisationsBootstrapOrganisationDocumentIsMinimal(t *testing.T) {
	masterID := primitive.NewObjectID()
	createdAt := time.Date(2024, time.December, 7, 8, 16, 51, 0, time.UTC)
	document := organisationsBootstrapOrganisationDocument(organisationsBootstrapUser{
		ID:        masterID,
		Username:  "owner",
		Timezone:  "Europe/Brussels",
		CreatedAt: createdAt,
	}, time.Date(2026, time.August, 6, 12, 0, 0, 0, time.UTC))

	if document["_id"] != masterID || document["ownerId"] != masterID {
		t.Fatalf("organisation identity = (%v, %v), want %v", document["_id"], document["ownerId"], masterID)
	}
	for _, forbidden := range []string{"slug", "role", "subscription", "password", "channels", "groups"} {
		if _, exists := document[forbidden]; exists {
			t.Errorf("minimal organisation unexpectedly contains %q", forbidden)
		}
	}
	audit := document["audit"].(bson.M)
	if audit["createdAt"] != createdAt || audit["updatedAt"] != createdAt {
		t.Errorf("audit timestamps = (%v, %v), want %v", audit["createdAt"], audit["updatedAt"], createdAt)
	}
}

func TestOrganisationsBootstrapMissingOrganisationFieldsPreservesExistingValues(t *testing.T) {
	masterID := primitive.NewObjectID()
	raw, err := bson.Marshal(bson.M{
		"_id":      masterID,
		"name":     "Existing name",
		"ownerId":  masterID,
		"isActive": false,
		"settings": bson.M{"timezone": "UTC"},
		"audit": bson.M{
			"createdBy":  "existing",
			"createdAt":  time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
			"updatedBy":  "existing",
			"updatedAt":  time.Date(2020, time.January, 2, 0, 0, 0, 0, time.UTC),
			"lastAction": "organisation.updated",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	missing := organisationsBootstrapMissingOrganisationFields(raw, organisationsBootstrapUser{
		ID:       masterID,
		Username: "owner",
		Timezone: "Europe/Brussels",
	}, time.Now())
	if len(missing) != 0 {
		t.Fatalf("missing fields = %v, want none", missing)
	}
}

func TestOrganisationsBootstrapOrganisationFieldsRejectWrongTypes(t *testing.T) {
	masterID := primitive.NewObjectID()
	raw, err := bson.Marshal(bson.M{
		"name":     "owner",
		"ownerId":  masterID,
		"isActive": "true",
		"settings": bson.M{"timezone": "UTC"},
		"audit": bson.M{
			"createdBy":  masterID.Hex(),
			"createdAt":  time.Now(),
			"updatedBy":  masterID.Hex(),
			"updatedAt":  time.Now(),
			"lastAction": "organisation.migrated",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if organisationsBootstrapOrganisationFieldsValid(raw, true) {
		t.Fatal("string isActive must not satisfy canonical organisation validation")
	}
}

func TestOrganisationsBootstrapMissingUpdatedAtUsesCanonicalCreatedAt(t *testing.T) {
	masterID := primitive.NewObjectID()
	canonicalCreatedAt := time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
	raw, err := bson.Marshal(bson.M{
		"audit": bson.M{"createdAt": canonicalCreatedAt},
	})
	if err != nil {
		t.Fatal(err)
	}
	missing := organisationsBootstrapMissingOrganisationFields(raw, organisationsBootstrapUser{
		ID:        masterID,
		Username:  "owner",
		CreatedAt: time.Date(2024, time.December, 7, 0, 0, 0, 0, time.UTC),
	}, time.Now())
	updatedAt, ok := missing["audit.updatedAt"].(time.Time)
	if !ok || !updatedAt.Equal(canonicalCreatedAt) {
		t.Fatalf("audit.updatedAt = %v, want canonical createdAt %v", missing["audit.updatedAt"], canonicalCreatedAt)
	}
}
