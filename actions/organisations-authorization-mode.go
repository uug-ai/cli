package actions

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/uug-ai/models/pkg/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// OrganisationsAuthorizationModeConfig configures organisations-authorization-mode.
type OrganisationsAuthorizationModeConfig struct {
	Mode              string // dry-run (default) or live
	MongoDBURI        string
	Database          string
	OrganisationID    string
	AuthorizationMode string // legacy, shadow or rbac
}

// The stored access collections (spec 3), as hub-api reads them.
const (
	authorizationModeOrganisationUsers = "organisation_users"
	authorizationModeOrganisationRoles = "organisation_roles"
	authorizationModeRoleAssignments   = "role_assignments"
)

var errAuthorizationModeNotFresh = errors.New("organisation is not fresh")

// authorizationModeState is what the action reads before deciding anything.
type authorizationModeState struct {
	organisationID primitive.ObjectID
	ownerID        primitive.ObjectID
	currentMode    models.AuthorizationMode
	ownerMember    bool // the owner has an effectively active membership
	otherMembers   int64
	subAccounts    int64
}

// authorizationModePlan is what the action will write.
type authorizationModePlan struct {
	mode        models.AuthorizationMode
	roles       []models.Role
	assignments []models.RoleAssignment
}

// OrganisationsAuthorizationMode sets an organisation's authorization mode
// (spec 7.2). It is a development tool for fresh organisations only:
//
//   - shadow and rbac need the stored documents, and for a fresh organisation
//     (its owner and no other members or sub-accounts) those are exactly
//     what models.MapLegacyAccess produces for the owner: the Owner role copy
//     and the Owner assignment, with the derived ids the migration command
//     will use. They are upserted, so a rerun writes the same documents.
//   - legacy only sets the mode back; the stored documents stay.
//
// Organisations with other members need the migration command, which maps
// every member; this action refuses them.
func OrganisationsAuthorizationMode(cfg OrganisationsAuthorizationModeConfig) error {
	target := models.AuthorizationMode(strings.TrimSpace(cfg.AuthorizationMode))
	if target == "" || !target.IsValid() {
		return fmt.Errorf("-authorization-mode must be legacy, shadow or rbac, got %q", cfg.AuthorizationMode)
	}
	live := strings.EqualFold(cfg.Mode, "live")
	if !live && cfg.Mode != "" && !strings.EqualFold(cfg.Mode, "dry-run") {
		return fmt.Errorf("-mode must be dry-run or live, got %q", cfg.Mode)
	}
	organisationID, err := primitive.ObjectIDFromHex(strings.TrimSpace(cfg.OrganisationID))
	if err != nil || organisationID.IsZero() {
		return fmt.Errorf("-organisation-id must be an organisation ObjectID")
	}
	if cfg.MongoDBURI == "" || cfg.Database == "" {
		return fmt.Errorf("-mongodb-uri and -mongodb-destination-database are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.MongoDBURI).SetServerSelectionTimeout(ServerSelectionTimeoutCheck*time.Second))
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}
	defer client.Disconnect(ctx)
	db := client.Database(cfg.Database)

	state, err := readAuthorizationModeState(ctx, db, organisationID)
	if err != nil {
		return err
	}
	plan, err := planAuthorizationMode(state, target, time.Now().UTC())
	if err != nil {
		return err
	}
	printAuthorizationModePlan(state, plan, live)
	if !live {
		fmt.Println("[info] dry-run: nothing written; rerun with -mode live to apply.")
		return nil
	}
	return applyAuthorizationModePlan(ctx, db, organisationID, plan)
}

func readAuthorizationModeState(ctx context.Context, db *mongo.Database, organisationID primitive.ObjectID) (authorizationModeState, error) {
	state := authorizationModeState{organisationID: organisationID}
	var organisation bson.Raw
	err := db.Collection("organisation").FindOne(ctx, bson.M{"_id": organisationID}).Decode(&organisation)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return state, fmt.Errorf("organisation %s not found; bootstrap it first (organisations-bootstrap -username <owner>)", organisationID.Hex())
	}
	if err != nil {
		return state, fmt.Errorf("read organisation: %w", err)
	}
	ownerID, ownerState := organisationsBootstrapObjectID(organisation, "ownerId")
	if ownerState != organisationsBootstrapFieldValue {
		return state, fmt.Errorf("organisation %s has no ownerId", organisationID.Hex())
	}
	state.ownerID = ownerID
	if mode, ok := organisation.Lookup("authorizationMode").StringValueOK(); ok {
		state.currentMode = models.AuthorizationMode(mode)
	}

	var membership models.OrganisationUser
	err = db.Collection(authorizationModeOrganisationUsers).FindOne(ctx, bson.M{"organisationId": organisationID, "userId": ownerID}).Decode(&membership)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return state, fmt.Errorf("read owner membership: %w", err)
	}
	state.ownerMember = err == nil && membership.IsEffectivelyActive()

	if state.otherMembers, err = db.Collection(authorizationModeOrganisationUsers).CountDocuments(ctx, bson.M{"organisationId": organisationID, "userId": bson.M{"$ne": ownerID}}); err != nil {
		return state, fmt.Errorf("count members: %w", err)
	}
	if state.subAccounts, err = db.Collection("users").CountDocuments(ctx, bson.M{"user_id": organisationID.Hex()}); err != nil {
		return state, fmt.Errorf("count sub-accounts: %w", err)
	}
	return state, nil
}

// planAuthorizationMode decides what to write, without any I/O.
func planAuthorizationMode(state authorizationModeState, target models.AuthorizationMode, now time.Time) (authorizationModePlan, error) {
	plan := authorizationModePlan{mode: target}
	if target == models.AuthorizationModeLegacy {
		return plan, nil
	}
	if state.otherMembers > 0 || state.subAccounts > 0 {
		return plan, fmt.Errorf("%w: %d other member(s) and %d sub-account(s); organisations with members need the migration command",
			errAuthorizationModeNotFresh, state.otherMembers, state.subAccounts)
	}
	if !state.ownerMember {
		return plan, fmt.Errorf("the owner %s has no active membership of %s; bootstrap it first (organisations-bootstrap)", state.ownerID.Hex(), state.organisationID.Hex())
	}
	mapped, err := models.MapLegacyAccess(models.LegacyUserAccess{
		OrganisationId: state.organisationID, UserId: state.ownerID, Role: string(models.BaseRoleOwner),
	})
	if err != nil {
		return plan, fmt.Errorf("map the owner: %w", err)
	}
	stamp := models.Audit{CreatedBy: "cli", CreatedAt: now, UpdatedBy: "cli", UpdatedAt: now, LastAction: "authorization.mode"}
	for _, role := range mapped.Roles {
		role.Audit = stamp
		plan.roles = append(plan.roles, role)
	}
	for _, assignment := range mapped.Assignments {
		assignment.Audit = stamp
		plan.assignments = append(plan.assignments, assignment)
	}
	return plan, nil
}

func printAuthorizationModePlan(state authorizationModeState, plan authorizationModePlan, live bool) {
	current := state.currentMode
	if current == "" {
		current = "(unset: legacy)"
	}
	fmt.Printf("[info] organisation %s, owner %s, mode %s -> %s (live=%v)\n", state.organisationID.Hex(), state.ownerID.Hex(), current, plan.mode, live)
	for _, role := range plan.roles {
		fmt.Printf("  upsert %s %s (%s, key %s)\n", authorizationModeOrganisationRoles, role.Id.Hex(), role.Name, role.Key)
	}
	for _, assignment := range plan.assignments {
		fmt.Printf("  upsert %s %s (%s %s, role %s)\n", authorizationModeRoleAssignments, assignment.Id.Hex(), assignment.Principal.Type, assignment.Principal.Id.Hex(), assignment.RoleId.Hex())
	}
	fmt.Printf("  set organisation.authorizationMode = %s\n", plan.mode)
}

func applyAuthorizationModePlan(ctx context.Context, db *mongo.Database, organisationID primitive.ObjectID, plan authorizationModePlan) error {
	upsert := options.Replace().SetUpsert(true)
	for _, role := range plan.roles {
		if _, err := db.Collection(authorizationModeOrganisationRoles).ReplaceOne(ctx, bson.M{"_id": role.Id}, role, upsert); err != nil {
			return fmt.Errorf("upsert role %s: %w", role.Id.Hex(), err)
		}
	}
	for _, assignment := range plan.assignments {
		if _, err := db.Collection(authorizationModeRoleAssignments).ReplaceOne(ctx, bson.M{"_id": assignment.Id}, assignment, upsert); err != nil {
			return fmt.Errorf("upsert assignment %s: %w", assignment.Id.Hex(), err)
		}
	}
	// The mode goes last: services switch only once the documents exist.
	if _, err := db.Collection("organisation").UpdateOne(ctx, bson.M{"_id": organisationID}, bson.M{"$set": bson.M{"authorizationMode": plan.mode}}); err != nil {
		return fmt.Errorf("set the mode: %w", err)
	}
	fmt.Printf("[ok] organisation %s is now in %s mode\n", organisationID.Hex(), plan.mode)
	return nil
}
