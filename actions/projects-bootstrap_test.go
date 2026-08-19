package actions

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestValidateProjectsBootstrapConfig(t *testing.T) {
	valid := normalizeProjectsBootstrapConfig(ProjectsBootstrapConfig{
		Mode:                       "dry-run",
		Stage:                      "owners",
		MongoDBURI:                 "mongodb://localhost:27017",
		MongoDBDestinationDatabase: "hub",
	})

	tests := []struct {
		name      string
		mutate    func(ProjectsBootstrapConfig) ProjectsBootstrapConfig
		wantError string
	}{
		{name: "valid owners", mutate: func(config ProjectsBootstrapConfig) ProjectsBootstrapConfig { return config }},
		{name: "valid sub-users", mutate: func(config ProjectsBootstrapConfig) ProjectsBootstrapConfig {
			config.Stage = "sub-users"
			return config
		}},
		{name: "valid verify", mutate: func(config ProjectsBootstrapConfig) ProjectsBootstrapConfig {
			config.Stage = "verify"
			return config
		}},
		{name: "requires stage", mutate: func(config ProjectsBootstrapConfig) ProjectsBootstrapConfig {
			config.Stage = ""
			return config
		}, wantError: "stage must be"},
		{name: "verify rejects live mode", mutate: func(config ProjectsBootstrapConfig) ProjectsBootstrapConfig {
			config.Stage = "verify"
			config.Mode = "live"
			return config
		}, wantError: "verify is read-only"},
		{name: "owners support checkpointed live mode", mutate: func(config ProjectsBootstrapConfig) ProjectsBootstrapConfig {
			config.Mode = "live"
			return config
		}},
		{name: "rejects conflicting scopes", mutate: func(config ProjectsBootstrapConfig) ProjectsBootstrapConfig {
			config.Username = "owner"
			config.OrganisationID = "507f1f77bcf86cd799439011"
			return config
		}, wantError: "mutually exclusive"},
		{name: "resume requires live mode", mutate: func(config ProjectsBootstrapConfig) ProjectsBootstrapConfig {
			config.Resume = true
			return config
		}, wantError: "require -mode live"},
		{name: "rejects resume with restart", mutate: func(config ProjectsBootstrapConfig) ProjectsBootstrapConfig {
			config.Mode = "live"
			config.Resume = true
			config.Restart = true
			return config
		}, wantError: "mutually exclusive"},
		{name: "rejects an invalid organisation scope", mutate: func(config ProjectsBootstrapConfig) ProjectsBootstrapConfig {
			config.OrganisationID = "not-an-objectid"
			return config
		}, wantError: "invalid organisation-id"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateProjectsBootstrapConfig(test.mutate(valid))
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("validateProjectsBootstrapConfig() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("validateProjectsBootstrapConfig() error = %v, want containing %q", err, test.wantError)
			}
		})
	}
}

func TestProjectsBootstrapExitCode(t *testing.T) {
	err := &projectsBootstrapError{code: projectsBootstrapExitData, err: errors.New("data conflict")}
	if got := ProjectsBootstrapExitCode(err); got != projectsBootstrapExitData {
		t.Fatalf("ProjectsBootstrapExitCode() = %d, want %d", got, projectsBootstrapExitData)
	}
	if got := ProjectsBootstrapExitCode(nil); got != 0 {
		t.Fatalf("ProjectsBootstrapExitCode(nil) = %d, want 0", got)
	}
	if got := ProjectsBootstrapExitCode(errors.New("connection refused")); got != projectsBootstrapExitOperational {
		t.Fatalf("ProjectsBootstrapExitCode(plain) = %d, want %d", got, projectsBootstrapExitOperational)
	}
}

func TestProjectsBootstrapCheckpointIDIsIndependentOfOrganisations(t *testing.T) {
	config := normalizeProjectsBootstrapConfig(ProjectsBootstrapConfig{
		Stage:                      "owners",
		MongoDBDestinationDatabase: "hub",
		OrganisationID:             "507f1f77bcf86cd799439011",
	})
	ownersID := projectsBootstrapCheckpointID(config)
	config.Stage = "sub-users"
	subUsersID := projectsBootstrapCheckpointID(config)
	if ownersID == subUsersID {
		t.Fatalf("checkpoint IDs must differ by stage: %q", ownersID)
	}
	for _, part := range []string{"projects-bootstrap", "v1", "hub", "owners", config.OrganisationID} {
		if !strings.Contains(ownersID, part) {
			t.Errorf("owner checkpoint ID %q does not contain %q", ownersID, part)
		}
	}

	organisationsConfig := normalizeOrganisationsBootstrapConfig(OrganisationsBootstrapConfig{
		Stage:                      "owners",
		MongoDBDestinationDatabase: "hub",
		OrganisationID:             "507f1f77bcf86cd799439011",
	})
	if ownersID == organisationsBootstrapCheckpointID(organisationsConfig) {
		t.Fatal("projects and organisations bootstrap must not share a checkpoint identity")
	}
}

func TestProjectsBootstrapEveryConflictBlocksLiveWrites(t *testing.T) {
	report := projectsBootstrapReport{}
	runner := projectsBootstrapRunner{report: &report}
	runner.addConflict("default-project-conflict", "", "", "legacy default")
	if runner.blockingConflictCount != 1 || !runner.hasConflict {
		t.Fatalf("blocking conflicts = %d, hasConflict = %v", runner.blockingConflictCount, runner.hasConflict)
	}
}

func TestProjectsBootstrapInterrupted(t *testing.T) {
	stopRequested := make(chan struct{})
	runner := projectsBootstrapRunner{stopRequested: stopRequested}
	if runner.interrupted() {
		t.Fatal("runner reported an interruption before the stop channel closed")
	}
	close(stopRequested)
	if !runner.interrupted() {
		t.Fatal("runner did not report an interruption after the stop channel closed")
	}
}

func TestProjectsBootstrapHeartbeatSkipsDryRun(t *testing.T) {
	called := false
	runner := projectsBootstrapRunner{}
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

func TestProjectsBootstrapRestoreCheckpointCounters(t *testing.T) {
	report := projectsBootstrapReport{}
	runner := projectsBootstrapRunner{report: &report}
	runner.restoreCheckpoint(projectsBootstrapCheckpointDocument{
		Counters: projectsBootstrapCheckpointCounters{
			Masters:       projectsBootstrapMasterCounts{Scanned: 3},
			SubUsers:      projectsBootstrapSubUserCounts{Updated: 4},
			Users:         projectsBootstrapUserCounts{MastersUpdated: 5},
			Organisations: projectsBootstrapOrganisationCounts{SecondaryOwned: 6},
			Projects:      projectsBootstrapProjectCounts{Inserted: 7},
			Writes:        projectsBootstrapWriteCounts{Applied: 8},
			Verification:  projectsBootstrapVerificationCounts{Passed: 9},
		},
		Conflicts: []projectsBootstrapConflict{{Code: "previous-conflict"}},
	})

	if report.Masters.Scanned != 3 || report.SubUsers.Updated != 4 || report.Users.MastersUpdated != 5 || report.Organisations.SecondaryOwned != 6 || report.Projects.Inserted != 7 || report.Writes.Applied != 8 || report.Verification.Passed != 9 {
		t.Fatalf("restored counters = %+v", report)
	}
	if len(report.Conflicts) != 0 {
		t.Fatalf("historical checkpoint conflicts leaked into the resumed report: %+v", report.Conflicts)
	}
	if runner.hasConflict {
		t.Fatal("historical checkpoint conflicts must not block a corrected resume")
	}
}

func TestProjectsBootstrapDefaultSlugIndexRequiresCompoundPartialUnique(t *testing.T) {
	compound := bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "slug", Value: int32(1)}}
	partial := bson.M{"slug": bson.M{"$exists": true, "$type": "string"}}

	if projectsBootstrapHasDefaultSlugIndex([]organisationsBootstrapIndex{{Key: compound, Unique: true}}) {
		t.Fatal("a full unique compound index must not satisfy the partial index contract")
	}
	if projectsBootstrapHasDefaultSlugIndex([]organisationsBootstrapIndex{{Key: compound, PartialFilterExpression: partial}}) {
		t.Fatal("a non-unique partial index must not satisfy the contract")
	}
	if projectsBootstrapHasDefaultSlugIndex([]organisationsBootstrapIndex{{
		Key:                     bson.D{{Key: "slug", Value: int32(1)}},
		Unique:                  true,
		PartialFilterExpression: partial,
	}}) {
		t.Fatal("the single-key organisation slug index must not satisfy the project contract")
	}
	if !projectsBootstrapHasDefaultSlugIndex([]organisationsBootstrapIndex{{
		Key:                     compound,
		Unique:                  true,
		PartialFilterExpression: partial,
	}}) {
		t.Fatal("the compound unique partial string index did not satisfy the contract")
	}
}

// The driver may decode a nested partialFilterExpression as either bson.M or
// bson.D. Missing the bson.D shape would block every live run with a spurious
// project-index-missing conflict.
func TestProjectsBootstrapDefaultSlugIndexDecodesDriverShape(t *testing.T) {
	raw, err := bson.Marshal(bson.D{
		{Key: "key", Value: bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "slug", Value: int32(1)}}},
		{Key: "unique", Value: true},
		{Key: "partialFilterExpression", Value: bson.D{
			{Key: "slug", Value: bson.D{{Key: "$exists", Value: true}, {Key: "$type", Value: "string"}}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var index organisationsBootstrapIndex
	if err := bson.Unmarshal(raw, &index); err != nil {
		t.Fatal(err)
	}
	if !projectsBootstrapHasDefaultSlugIndex([]organisationsBootstrapIndex{index}) {
		t.Fatalf("decoded index metadata did not match the contract: %+v", index)
	}
}

func TestParseProjectsBootstrapUserProjectSelection(t *testing.T) {
	userID := primitive.NewObjectID()
	projectID := primitive.NewObjectID()

	tests := []struct {
		name       string
		document   bson.M
		wantState  organisationsBootstrapFieldState
		wantTarget primitive.ObjectID
	}{
		{
			name:      "absent selection",
			document:  bson.M{"_id": userID, "username": "owner"},
			wantState: organisationsBootstrapFieldEmpty,
		},
		{
			name:      "zero selection is treated as absent",
			document:  bson.M{"_id": userID, "username": "owner", "projectId": primitive.NilObjectID},
			wantState: organisationsBootstrapFieldEmpty,
		},
		{
			name:       "populated selection",
			document:   bson.M{"_id": userID, "username": "owner", "projectId": projectID},
			wantState:  organisationsBootstrapFieldValue,
			wantTarget: projectID,
		},
		{
			name:      "wrong selection type",
			document:  bson.M{"_id": userID, "username": "owner", "projectId": projectID.Hex()},
			wantState: organisationsBootstrapFieldWrong,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw, err := bson.Marshal(test.document)
			if err != nil {
				t.Fatal(err)
			}
			user, err := parseProjectsBootstrapUser(raw)
			if err != nil {
				t.Fatalf("parseProjectsBootstrapUser() error = %v", err)
			}
			if user.ProjectSelectionState != test.wantState {
				t.Errorf("ProjectSelectionState = %v, want %v", user.ProjectSelectionState, test.wantState)
			}
			if user.ProjectSelection != test.wantTarget {
				t.Errorf("ProjectSelection = %v, want %v", user.ProjectSelection, test.wantTarget)
			}
			if user.ID != userID {
				t.Errorf("embedded user was not parsed: %v", user.ID)
			}
		})
	}
}

func TestProjectsBootstrapProjectDocumentIsTheReservedDefault(t *testing.T) {
	organisationID := primitive.NewObjectID()
	ownerID := primitive.NewObjectID()
	createdAt := time.Date(2024, time.December, 7, 8, 16, 51, 0, time.UTC)
	document := projectsBootstrapProjectDocument(organisationID, ownerID, createdAt)

	if document["_id"] != organisationID || document["organisationId"] != organisationID {
		t.Fatalf("project identity = (%v, %v), want %v", document["_id"], document["organisationId"], organisationID)
	}
	if document["slug"] != "default" || document["name"] != "Default" || document["isActive"] != true {
		t.Fatalf("default project shape = %+v", document)
	}
	for _, forbidden := range []string{"description", "keyset", "keys", "ownerId", "settings"} {
		if _, exists := document[forbidden]; exists {
			t.Errorf("reserved default project unexpectedly contains %q", forbidden)
		}
	}
	audit := document["audit"].(bson.M)
	if audit["createdBy"] != ownerID.Hex() || audit["updatedBy"] != ownerID.Hex() {
		t.Errorf("audit actors = (%v, %v), want %v", audit["createdBy"], audit["updatedBy"], ownerID.Hex())
	}
	if audit["createdAt"] != createdAt || audit["updatedAt"] != createdAt {
		t.Errorf("audit timestamps = (%v, %v), want %v", audit["createdAt"], audit["updatedAt"], createdAt)
	}

	raw, err := bson.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if !projectsBootstrapDefaultProjectValid(raw, organisationID) {
		t.Fatal("the minted document must satisfy the Hub API default project validator")
	}
	if !projectsBootstrapProjectFieldsValid(raw) {
		t.Fatal("the minted document must be canonically complete")
	}
}

// A default project minted lazily by Hub API's ensureDefaultProject carries no
// audit.createdBy/updatedBy: models.Audit marks both omitempty and Hub API sets
// neither. Such a document must read as complete, or `verify` fails for every
// organisation that saw a metadata read and the sub-users stage blocks.
func TestProjectsBootstrapAcceptsLazilyMintedDefaultProject(t *testing.T) {
	organisationID := primitive.NewObjectID()
	mintedAt := time.Date(2026, time.August, 19, 0, 0, 0, 0, time.UTC)
	raw, err := bson.Marshal(bson.M{
		"_id":            organisationID,
		"organisationId": organisationID,
		"name":           "Default",
		"slug":           "default",
		"isActive":       true,
		"audit": bson.M{
			"createdAt":  mintedAt,
			"updatedAt":  mintedAt,
			"lastAction": "project.default.created",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !projectsBootstrapDefaultProjectValid(raw, organisationID) {
		t.Fatal("a lazily-minted default must satisfy the Hub API identity contract")
	}
	if !projectsBootstrapExistingProjectTypesValid(raw) {
		t.Fatal("a lazily-minted default must satisfy project type validation")
	}
	if !projectsBootstrapProjectFieldsValid(raw) {
		t.Fatal("a lazily-minted default must read as complete")
	}
	if missing := projectsBootstrapMissingProjectFields(raw, organisationID, mintedAt); len(missing) != 0 {
		t.Fatalf("missing fields = %v, want none — the migration must not attribute creation to the owner", missing)
	}
}

func TestProjectsBootstrapMissingProjectFieldsPreservesExistingValues(t *testing.T) {
	organisationID := primitive.NewObjectID()
	existingCreatedAt := time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
	raw, err := bson.Marshal(bson.M{
		"_id":            organisationID,
		"organisationId": organisationID,
		"name":           "Renamed by the tenant",
		"slug":           "default",
		"isActive":       false,
		"audit": bson.M{
			"createdBy":  "existing",
			"createdAt":  existingCreatedAt,
			"updatedBy":  "existing",
			"updatedAt":  time.Date(2020, time.January, 2, 0, 0, 0, 0, time.UTC),
			"lastAction": "project.updated",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	missing := projectsBootstrapMissingProjectFields(raw, organisationID, time.Now())
	if len(missing) != 0 {
		t.Fatalf("missing fields = %v, want none — populated tenant values must never be rewritten", missing)
	}
}

func TestProjectsBootstrapMissingProjectFieldsAdoptsExistingCreatedAt(t *testing.T) {
	organisationID := primitive.NewObjectID()
	existingCreatedAt := time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
	raw, err := bson.Marshal(bson.M{
		"audit": bson.M{"createdAt": existingCreatedAt},
	})
	if err != nil {
		t.Fatal(err)
	}
	missing := projectsBootstrapMissingProjectFields(raw, organisationID, time.Now())
	updatedAt, ok := missing["audit.updatedAt"].(time.Time)
	if !ok || !updatedAt.Equal(existingCreatedAt) {
		t.Fatalf("audit.updatedAt = %v, want the existing createdAt %v", missing["audit.updatedAt"], existingCreatedAt)
	}
	if missing["organisationId"] != organisationID || missing["slug"] != "default" {
		t.Fatalf("missing identity fields = %+v", missing)
	}
}

func TestProjectsBootstrapExistingProjectTypesRejectWrongTypes(t *testing.T) {
	raw, err := bson.Marshal(bson.M{
		"name":     "Default",
		"slug":     "default",
		"isActive": "true",
	})
	if err != nil {
		t.Fatal(err)
	}
	if projectsBootstrapExistingProjectTypesValid(raw) {
		t.Fatal("a string isActive must not satisfy project type validation")
	}

	empty, err := bson.Marshal(bson.M{"slug": ""})
	if err != nil {
		t.Fatal(err)
	}
	if projectsBootstrapExistingProjectTypesValid(empty) {
		t.Fatal("an empty slug must not satisfy project type validation")
	}
}

func TestProjectsBootstrapDefaultProjectValidRejectsRandomIdentity(t *testing.T) {
	organisationID := primitive.NewObjectID()
	raw, err := bson.Marshal(bson.M{
		"_id":            primitive.NewObjectID(),
		"organisationId": organisationID,
		"slug":           "default",
	})
	if err != nil {
		t.Fatal(err)
	}
	if projectsBootstrapDefaultProjectValid(raw, organisationID) {
		t.Fatal("a default project minted with a random _id must not validate")
	}

	renamed, err := bson.Marshal(bson.M{
		"_id":            organisationID,
		"organisationId": organisationID,
		"slug":           "production",
	})
	if err != nil {
		t.Fatal(err)
	}
	if projectsBootstrapDefaultProjectValid(renamed, organisationID) {
		t.Fatal("a non-reserved slug must not validate as the default project")
	}
}

func TestProjectsBootstrapPreservedSelectionWritesNothing(t *testing.T) {
	organisationID := primitive.NewObjectID()
	report := projectsBootstrapReport{}
	runner := projectsBootstrapRunner{
		config: ProjectsBootstrapConfig{Mode: "live"},
		report: &report,
	}
	user := projectsBootstrapUser{
		organisationsBootstrapUser: organisationsBootstrapUser{ID: primitive.NewObjectID(), Selection: organisationID, SelectionState: organisationsBootstrapFieldValue},
		ProjectSelection:           organisationID,
		ProjectSelectionState:      organisationsBootstrapFieldValue,
	}

	// A nil database would panic on any write, which is exactly the assertion:
	// an already-correct selection must short-circuit before touching Mongo.
	ok, changed, err := runner.ensureUserProjectSelection(context.Background(), user, organisationID, organisationID)
	if err != nil {
		t.Fatalf("ensureUserProjectSelection() error = %v", err)
	}
	if !ok || changed {
		t.Fatalf("ensureUserProjectSelection() = (%v, %v), want (true, false)", ok, changed)
	}
	if report.Users.SelectionsPreserved != 1 || report.Writes.Attempted != 0 {
		t.Fatalf("preserved selection counters = %+v / %+v", report.Users, report.Writes)
	}
}

func TestProjectsBootstrapMismatchedSelectionBlocks(t *testing.T) {
	organisationID := primitive.NewObjectID()
	report := projectsBootstrapReport{}
	runner := projectsBootstrapRunner{
		config: ProjectsBootstrapConfig{Mode: "live"},
		report: &report,
	}
	user := projectsBootstrapUser{
		organisationsBootstrapUser: organisationsBootstrapUser{ID: primitive.NewObjectID(), Selection: organisationID, SelectionState: organisationsBootstrapFieldValue},
		ProjectSelection:           primitive.NewObjectID(),
		ProjectSelectionState:      organisationsBootstrapFieldValue,
	}

	ok, changed, err := runner.ensureUserProjectSelection(context.Background(), user, organisationID, organisationID)
	if err != nil {
		t.Fatalf("ensureUserProjectSelection() error = %v", err)
	}
	if ok || changed {
		t.Fatalf("ensureUserProjectSelection() = (%v, %v), want (false, false)", ok, changed)
	}
	if runner.blockingConflictCount != 1 || report.Conflicts[0].Code != "invalid-user-project-selection" {
		t.Fatalf("conflicts = %+v", report.Conflicts)
	}
	if report.Writes.Attempted != 0 {
		t.Fatalf("a conflicting selection must not attempt a write: %+v", report.Writes)
	}
}

func TestProjectsBootstrapStrictSelectionIsAConflict(t *testing.T) {
	organisationID := primitive.NewObjectID()
	report := projectsBootstrapReport{}
	runner := projectsBootstrapRunner{
		config: ProjectsBootstrapConfig{Mode: "dry-run"},
		report: &report,
		strict: true,
	}
	user := projectsBootstrapUser{
		organisationsBootstrapUser: organisationsBootstrapUser{ID: primitive.NewObjectID(), Selection: organisationID, SelectionState: organisationsBootstrapFieldValue},
	}

	ok, _, err := runner.ensureUserProjectSelection(context.Background(), user, organisationID, organisationID)
	if err != nil {
		t.Fatalf("ensureUserProjectSelection() error = %v", err)
	}
	if ok {
		t.Fatal("verify must treat a missing projectId as a conflict rather than planned work")
	}
	if report.Conflicts[0].Code != "user-project-selection-missing" {
		t.Fatalf("conflicts = %+v", report.Conflicts)
	}
}

func TestProjectsBootstrapReportOmitsTenantSecrets(t *testing.T) {
	report := projectsBootstrapReport{}
	runner := projectsBootstrapRunner{report: &report}
	for index := 0; index < 150; index++ {
		runner.addConflict("default-project-conflict", primitive.NewObjectID().Hex(), primitive.NewObjectID().Hex(), "legacy default")
	}
	if len(report.Conflicts) != 100 {
		t.Fatalf("report entries = %d, want capped at 100", len(report.Conflicts))
	}
	if runner.conflictCount != 150 {
		t.Fatalf("conflict count = %d, want every conflict counted even when not listed", runner.conflictCount)
	}
}
