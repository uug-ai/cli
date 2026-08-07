package actions

import (
	"context"
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

func TestOrganisationsBootstrapInterrupted(t *testing.T) {
	stopRequested := make(chan struct{})
	runner := organisationsBootstrapRunner{stopRequested: stopRequested}
	if runner.interrupted() {
		t.Fatal("runner reported an interruption before the stop channel closed")
	}
	close(stopRequested)
	if !runner.interrupted() {
		t.Fatal("runner did not report an interruption after the stop channel closed")
	}
}

func TestOrganisationsBootstrapHeartbeatSkipsDryRun(t *testing.T) {
	called := false
	runner := organisationsBootstrapRunner{}
	if err := runner.withCheckpointHeartbeat(t.Context(), func(context.Context) error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("withCheckpointHeartbeat() error = %v", err)
	}
	if !called {
		t.Fatal("withCheckpointHeartbeat() did not run the operation")
	}
}

func TestOrganisationsBootstrapRestoreCheckpointCounters(t *testing.T) {
	report := organisationsBootstrapReport{}
	runner := organisationsBootstrapRunner{report: &report}
	runner.restoreCheckpoint(organisationsBootstrapCheckpointDocument{
		Counters: organisationsBootstrapCheckpointCounters{
			Masters:       organisationsBootstrapMasterCounts{Scanned: 3},
			SubUsers:      organisationsBootstrapSubUserCounts{Updated: 4},
			Users:         organisationsBootstrapUserCounts{MastersUpdated: 5},
			Memberships:   organisationsBootstrapMembershipCounts{Inserted: 6},
			Organisations: organisationsBootstrapOrganisationCounts{LegacyReported: 7},
			Writes:        organisationsBootstrapWriteCounts{Applied: 8},
			Verification:  organisationsBootstrapVerificationCounts{Passed: 9},
		},
		Conflicts: []organisationsBootstrapConflict{{Code: "previous-conflict"}},
	})

	if report.Masters.Scanned != 3 || report.SubUsers.Updated != 4 || report.Users.MastersUpdated != 5 || report.Memberships.Inserted != 6 || report.Organisations.LegacyReported != 7 || report.Writes.Applied != 8 || report.Verification.Passed != 9 {
		t.Fatalf("restored counters = %+v", report)
	}
	if len(report.Conflicts) != 0 {
		t.Fatalf("historical checkpoint conflicts leaked into resumed report: %+v", report.Conflicts)
	}
	if runner.hasConflict {
		t.Fatal("historical checkpoint conflicts must not block a corrected resume")
	}
}

func TestOrganisationsBootstrapLegacyReportConflictDoesNotBlock(t *testing.T) {
	if organisationsBootstrapConflictBlocks("legacy-organisation-unresolved") {
		t.Fatal("legacy report conflict must allow the non-destructive live migration path")
	}
	if !organisationsBootstrapConflictBlocks("canonical-owner-conflict") {
		t.Fatal("canonical owner conflict must block live writes during preflight")
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
	base := organisationsBootstrapIndex{
		Key:    bson.D{{Key: "slug", Value: int32(1)}},
		Unique: true,
	}
	if organisationsBootstrapHasSlugIndex([]organisationsBootstrapIndex{base}) {
		t.Fatal("full unique slug index must not satisfy the partial index contract")
	}
	base.PartialFilterExpression = bson.M{"slug": bson.M{"$exists": true, "$type": "string"}}
	if !organisationsBootstrapHasSlugIndex([]organisationsBootstrapIndex{base}) {
		t.Fatal("partial unique string slug index did not satisfy the contract")
	}
}

func TestOrganisationsBootstrapIndexMetadataDecodesDriverShape(t *testing.T) {
	raw, err := bson.Marshal(bson.D{
		{Key: "key", Value: bson.D{{Key: "userId", Value: int32(1)}, {Key: "organisationId", Value: int32(1)}}},
		{Key: "unique", Value: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	var index organisationsBootstrapIndex
	if err := bson.Unmarshal(raw, &index); err != nil {
		t.Fatal(err)
	}
	if !organisationsBootstrapHasIndex([]organisationsBootstrapIndex{index}, bson.D{{Key: "userId", Value: int32(1)}, {Key: "organisationId", Value: int32(1)}}, true) {
		t.Fatalf("decoded index metadata did not match: %+v", index)
	}
}

func TestParseOrganisationsBootstrapUser(t *testing.T) {
	masterID := primitive.NewObjectID()
	subUserID := primitive.NewObjectID()

	tests := []struct {
		name                       string
		document                   bson.M
		wantParentState            organisationsBootstrapFieldState
		wantParentID               primitive.ObjectID
		wantLegacySelectionPresent bool
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
		{
			name:                       "user with legacy selection key",
			document:                   bson.M{"_id": masterID, "username": "owner", "organisation_id": primitive.NewObjectID()},
			wantParentState:            organisationsBootstrapFieldEmpty,
			wantLegacySelectionPresent: true,
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
			if user.LegacySelectionPresent != test.wantLegacySelectionPresent {
				t.Errorf("LegacySelectionPresent = %v, want %v", user.LegacySelectionPresent, test.wantLegacySelectionPresent)
			}
		})
	}
}

func TestOrganisationsBootstrapOwnerBlocksLegacySelectionField(t *testing.T) {
	report := organisationsBootstrapReport{}
	runner := organisationsBootstrapRunner{report: &report}
	userID := primitive.NewObjectID()

	err := runner.processOwner(context.Background(), organisationsBootstrapUser{
		ID:                     userID,
		Username:               "owner",
		LegacySelectionPresent: true,
	})
	if err != nil {
		t.Fatalf("processOwner() error = %v", err)
	}
	if runner.blockingConflictCount != 1 || report.Verification.Failed != 1 {
		t.Fatalf("blocking conflicts = %d, verification failures = %d", runner.blockingConflictCount, report.Verification.Failed)
	}
	if len(report.Conflicts) != 1 || report.Conflicts[0].Code != "legacy-user-selection-field" {
		t.Fatalf("conflicts = %+v", report.Conflicts)
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
