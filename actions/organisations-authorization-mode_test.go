package actions

import (
	"errors"
	"testing"
	"time"

	"github.com/uug-ai/models/pkg/models"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func freshAuthorizationModeState() authorizationModeState {
	return authorizationModeState{organisationID: primitive.NewObjectID(), ownerID: primitive.NewObjectID(), ownerMember: true}
}

func TestPlanAuthorizationModeWritesTheOwnerDocuments(t *testing.T) {
	state := freshAuthorizationModeState()
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	for _, target := range []models.AuthorizationMode{models.AuthorizationModeRBAC, models.AuthorizationModeShadow} {
		plan, err := planAuthorizationMode(state, target, now)
		if err != nil {
			t.Fatalf("%s: %v", target, err)
		}
		if plan.mode != target || len(plan.roles) != 1 || len(plan.assignments) != 1 {
			t.Fatalf("%s plan %+v", target, plan)
		}
		role, assignment := plan.roles[0], plan.assignments[0]
		if !role.IsOwner() || role.OrganisationId != state.organisationID || role.Id != models.BuiltInRoleId(state.organisationID, models.RoleKeyOwner) {
			t.Errorf("role %+v", role)
		}
		if assignment.Principal != (models.Principal{Type: models.PrincipalTypeUser, Id: state.ownerID}) || assignment.RoleId != role.Id ||
			assignment.Source != models.AssignmentSourceMigration || !assignment.IsActive || assignment.Validate(role) != nil {
			t.Errorf("assignment %+v", assignment)
		}
		if !assignment.Audit.CreatedAt.Equal(now) || role.Audit.LastAction != "authorization.mode" {
			t.Errorf("audit %+v / %+v", assignment.Audit, role.Audit)
		}
	}
	// A rerun plans the same ids, so the upserts write the same documents.
	first, _ := planAuthorizationMode(state, models.AuthorizationModeRBAC, now)
	again, _ := planAuthorizationMode(state, models.AuthorizationModeRBAC, now.Add(time.Hour))
	if first.roles[0].Id != again.roles[0].Id || first.assignments[0].Id != again.assignments[0].Id {
		t.Error("ids are not derived from the organisation and owner")
	}
}

func TestPlanAuthorizationModeRefusesOrganisationsWithMembers(t *testing.T) {
	for name, mutate := range map[string]func(*authorizationModeState){
		"another member":     func(s *authorizationModeState) { s.otherMembers = 1 },
		"a sub-account":      func(s *authorizationModeState) { s.subAccounts = 2 },
		"owner not a member": func(s *authorizationModeState) { s.ownerMember = false },
	} {
		t.Run(name, func(t *testing.T) {
			state := freshAuthorizationModeState()
			mutate(&state)
			if _, err := planAuthorizationMode(state, models.AuthorizationModeRBAC, time.Now()); err == nil {
				t.Error("planned rbac for an organisation that is not fresh")
			}
		})
	}
	state := freshAuthorizationModeState()
	state.otherMembers = 3
	if _, err := planAuthorizationMode(state, models.AuthorizationModeRBAC, time.Now()); !errors.Is(err, errAuthorizationModeNotFresh) {
		t.Errorf("err = %v, want errAuthorizationModeNotFresh", err)
	}
}

func TestPlanAuthorizationModeLegacyOnlySetsTheMode(t *testing.T) {
	state := freshAuthorizationModeState()
	state.otherMembers, state.ownerMember = 5, false
	plan, err := planAuthorizationMode(state, models.AuthorizationModeLegacy, time.Now())
	if err != nil || plan.mode != models.AuthorizationModeLegacy || len(plan.roles) != 0 || len(plan.assignments) != 0 {
		t.Errorf("legacy plan %+v, err %v", plan, err)
	}
}

func TestOrganisationsAuthorizationModeValidatesFlags(t *testing.T) {
	valid := OrganisationsAuthorizationModeConfig{MongoDBURI: "mongodb://127.0.0.1:1", Database: "hub", OrganisationID: primitive.NewObjectID().Hex(), AuthorizationMode: "rbac"}
	for name, mutate := range map[string]func(*OrganisationsAuthorizationModeConfig){
		"no mode":             func(c *OrganisationsAuthorizationModeConfig) { c.AuthorizationMode = "" },
		"unknown mode":        func(c *OrganisationsAuthorizationModeConfig) { c.AuthorizationMode = "enforce" },
		"unknown run mode":    func(c *OrganisationsAuthorizationModeConfig) { c.Mode = "apply" },
		"bad organisation id": func(c *OrganisationsAuthorizationModeConfig) { c.OrganisationID = "acme" },
		"no database":         func(c *OrganisationsAuthorizationModeConfig) { c.Database = "" },
	} {
		t.Run(name, func(t *testing.T) {
			config := valid
			mutate(&config)
			if err := OrganisationsAuthorizationMode(config); err == nil {
				t.Error("accepted")
			}
		})
	}
}
