package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// organisations-cleanup-access-references removes sites and groups that no
// longer exist from what selects them for access: users' legacy allow-lists
// (sites, groups) and stored role assignments (scope.siteIds,
// scope.groupIds). Deleting a site or group used to leave its id behind.
//
// A site or group id is never reused, so a reference to a deleted one grants
// nothing and removing it changes no one's access. References to sites and
// groups that still exist, in any organisation, are kept, and device keys are
// not touched: a device can be added again with the same key.
//
// Every write pulls only ids the scan found missing, so a rerun, or an
// allow-list edited in between, is safe.

// OrganisationsCleanupAccessReferencesConfig configures the action.
type OrganisationsCleanupAccessReferencesConfig struct {
	Mode           string // dry-run (default) or live
	MongoDBURI     string
	Database       string
	OrganisationID string // optional: limit to one organisation
	ReportFile     string // optional JSON report
}

const (
	accessReferencesUsers           = "users"
	accessReferencesSites           = "sites"
	accessReferencesGroups          = "groups"
	accessReferencesRoleAssignments = "role_assignments"
)

// accessReferencesUser is the part of a user document the action reads.
// Allow-list entries are read one by one: only id strings are considered,
// and anything else (an old embedded site document) is left alone.
type accessReferencesUser struct {
	Id     primitive.ObjectID `bson:"_id"`
	Master string             `bson:"user_id"`
	Sites  []bson.RawValue    `bson:"sites"`
	Groups []bson.RawValue    `bson:"groups"`
}

// accessReferencesStrings returns the string entries and how many others
// were skipped.
func accessReferencesStrings(raw []bson.RawValue) (values []string, skipped int) {
	for _, value := range raw {
		if text, ok := value.StringValueOK(); ok {
			values = append(values, text)
		} else {
			skipped++
		}
	}
	return values, skipped
}

// organisation is the organisation the user's allow-list applies in: the
// master account of a sub-account, the account itself otherwise.
func (u accessReferencesUser) organisation() string {
	if u.Master != "" {
		return u.Master
	}
	return u.Id.Hex()
}

type accessReferencesAssignment struct {
	Id             primitive.ObjectID `bson:"_id"`
	OrganisationId primitive.ObjectID `bson:"organisationId"`
	Scope          struct {
		SiteIds  []primitive.ObjectID `bson:"siteIds"`
		GroupIds []primitive.ObjectID `bson:"groupIds"`
	} `bson:"scope"`
}

// accessReferencesUserRemoval is one user's missing sites and groups.
type accessReferencesUserRemoval struct {
	UserId         string   `json:"userId"`
	OrganisationId string   `json:"organisationId"`
	Sites          []string `json:"sites,omitempty"`
	Groups         []string `json:"groups,omitempty"`
}

// accessReferencesAssignmentRemoval is one assignment's missing sites and groups.
type accessReferencesAssignmentRemoval struct {
	AssignmentId   string               `json:"assignmentId"`
	OrganisationId string               `json:"organisationId"`
	Sites          []primitive.ObjectID `json:"sites,omitempty"`
	Groups         []primitive.ObjectID `json:"groups,omitempty"`
}

type accessReferencesReport struct {
	Mode               string                              `json:"mode"`
	Database           string                              `json:"database"`
	OrganisationId     string                              `json:"organisationId,omitempty"`
	StartedAt          string                              `json:"startedAt"`
	CompletedAt        string                              `json:"completedAt"`
	UsersScanned       int                                 `json:"usersScanned"`
	AssignmentsScanned int                                 `json:"assignmentsScanned"`
	SitesRemoved       int                                 `json:"sitesRemoved"`
	SkippedEntries     int                                 `json:"skippedEntries"` // allow-list entries that are not id strings, left alone
	GroupsRemoved      int                                 `json:"groupsRemoved"`
	Users              []accessReferencesUserRemoval       `json:"users"`
	Assignments        []accessReferencesAssignmentRemoval `json:"assignments"`
	Written            *accessReferencesWrites             `json:"written,omitempty"`
}

type accessReferencesWrites struct {
	Users       int `json:"users"`
	Assignments int `json:"assignments"`
	Errors      int `json:"errors"`
}

// OrganisationsCleanupAccessReferences runs the action.
func OrganisationsCleanupAccessReferences(cfg OrganisationsCleanupAccessReferencesConfig) error {
	live := strings.EqualFold(strings.TrimSpace(cfg.Mode), "live")
	if !live && strings.TrimSpace(cfg.Mode) != "" && !strings.EqualFold(strings.TrimSpace(cfg.Mode), "dry-run") {
		return fmt.Errorf("-mode must be dry-run or live, got %q", cfg.Mode)
	}
	if strings.TrimSpace(cfg.MongoDBURI) == "" || strings.TrimSpace(cfg.Database) == "" {
		return fmt.Errorf("-mongodb-uri and -mongodb-destination-database are required")
	}
	var organisationID primitive.ObjectID
	if value := strings.TrimSpace(cfg.OrganisationID); value != "" {
		id, err := primitive.ObjectIDFromHex(value)
		if err != nil || id.IsZero() {
			return fmt.Errorf("-organisation-id must be an organisation ObjectID")
		}
		organisationID = id
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.MongoDBURI).SetServerSelectionTimeout(ServerSelectionTimeoutCheck*time.Second))
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}
	defer client.Disconnect(ctx)
	db := client.Database(cfg.Database)

	report := accessReferencesReport{Mode: "dry-run", Database: cfg.Database, StartedAt: time.Now().UTC().Format(time.RFC3339)}
	if live {
		report.Mode = "live"
	}
	if !organisationID.IsZero() {
		report.OrganisationId = organisationID.Hex()
	}

	sites, err := accessReferencesExisting(ctx, db.Collection(accessReferencesSites))
	if err != nil {
		return fmt.Errorf("read sites: %w", err)
	}
	groups, err := accessReferencesExisting(ctx, db.Collection(accessReferencesGroups))
	if err != nil {
		return fmt.Errorf("read groups: %w", err)
	}
	users, err := accessReferencesReadUsers(ctx, db, organisationID)
	if err != nil {
		return err
	}
	assignments, err := accessReferencesReadAssignments(ctx, db, organisationID)
	if err != nil {
		return err
	}
	report.UsersScanned, report.AssignmentsScanned = len(users), len(assignments)
	report.Users, report.SkippedEntries = planUserAccessReferences(users, sites, groups)
	report.Assignments = planAssignmentAccessReferences(assignments, sites, groups)
	for _, removal := range report.Users {
		report.SitesRemoved += len(removal.Sites)
		report.GroupsRemoved += len(removal.Groups)
	}
	for _, removal := range report.Assignments {
		report.SitesRemoved += len(removal.Sites)
		report.GroupsRemoved += len(removal.Groups)
	}
	fmt.Printf("[info] %d users and %d role assignments scanned; %d users and %d assignments reference %d deleted sites and %d deleted groups; %d entries that are not ids left alone (live=%v)\n",
		report.UsersScanned, report.AssignmentsScanned, len(report.Users), len(report.Assignments), report.SitesRemoved, report.GroupsRemoved, report.SkippedEntries, live)

	if live {
		report.Written = applyAccessReferences(ctx, db, report.Users, report.Assignments)
		fmt.Printf("[ok] %d users and %d assignments updated, %d errors\n", report.Written.Users, report.Written.Assignments, report.Written.Errors)
	} else {
		fmt.Println("[info] dry-run: nothing written; rerun with -mode live to apply.")
	}
	report.CompletedAt = time.Now().UTC().Format(time.RFC3339)
	if cfg.ReportFile != "" {
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(cfg.ReportFile, data, 0o600); err != nil {
			return fmt.Errorf("write report: %w", err)
		}
	}
	if report.Written != nil && report.Written.Errors > 0 {
		return fmt.Errorf("%d writes failed; rerun to retry", report.Written.Errors)
	}
	return nil
}

// accessReferencesExisting returns the hex ids of every document in a collection.
func accessReferencesExisting(ctx context.Context, collection *mongo.Collection) (map[string]bool, error) {
	cursor, err := collection.Find(ctx, bson.M{}, options.Find().SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	existing := map[string]bool{}
	for cursor.Next(ctx) {
		var document struct {
			Id primitive.ObjectID `bson:"_id"`
		}
		if err := cursor.Decode(&document); err == nil {
			existing[document.Id.Hex()] = true
		}
	}
	return existing, cursor.Err()
}

func accessReferencesReadUsers(ctx context.Context, db *mongo.Database, organisationID primitive.ObjectID) ([]accessReferencesUser, error) {
	filter := bson.M{"$or": bson.A{bson.M{"sites.0": bson.M{"$exists": true}}, bson.M{"groups.0": bson.M{"$exists": true}}}}
	if !organisationID.IsZero() {
		filter = bson.M{"$and": bson.A{filter, bson.M{"$or": bson.A{bson.M{"user_id": organisationID.Hex()}, bson.M{"_id": organisationID}}}}}
	}
	cursor, err := db.Collection(accessReferencesUsers).Find(ctx, filter, options.Find().SetProjection(bson.M{"user_id": 1, "sites": 1, "groups": 1}))
	if err != nil {
		return nil, fmt.Errorf("read users: %w", err)
	}
	var users []accessReferencesUser
	if err := cursor.All(ctx, &users); err != nil {
		return nil, fmt.Errorf("read users: %w", err)
	}
	return users, nil
}

func accessReferencesReadAssignments(ctx context.Context, db *mongo.Database, organisationID primitive.ObjectID) ([]accessReferencesAssignment, error) {
	filter := bson.M{"$or": bson.A{bson.M{"scope.siteIds.0": bson.M{"$exists": true}}, bson.M{"scope.groupIds.0": bson.M{"$exists": true}}}}
	if !organisationID.IsZero() {
		filter = bson.M{"$and": bson.A{filter, bson.M{"organisationId": organisationID}}}
	}
	cursor, err := db.Collection(accessReferencesRoleAssignments).Find(ctx, filter, options.Find().SetProjection(bson.M{"organisationId": 1, "scope.siteIds": 1, "scope.groupIds": 1}))
	if err != nil {
		return nil, fmt.Errorf("read role assignments: %w", err)
	}
	var assignments []accessReferencesAssignment
	if err := cursor.All(ctx, &assignments); err != nil {
		return nil, fmt.Errorf("read role assignments: %w", err)
	}
	return assignments, nil
}

// planUserAccessReferences returns, per user, the allow-listed sites and
// groups that do not exist. A value that is not an id selects nothing and is
// removed as well.
func planUserAccessReferences(users []accessReferencesUser, sites, groups map[string]bool) ([]accessReferencesUserRemoval, int) {
	removals := []accessReferencesUserRemoval{}
	skipped := 0
	for _, user := range users {
		userSites, skippedSites := accessReferencesStrings(user.Sites)
		userGroups, skippedGroups := accessReferencesStrings(user.Groups)
		skipped += skippedSites + skippedGroups
		removal := accessReferencesUserRemoval{UserId: user.Id.Hex(), OrganisationId: user.organisation(),
			Sites: missingReferences(userSites, sites), Groups: missingReferences(userGroups, groups)}
		if len(removal.Sites)+len(removal.Groups) > 0 {
			removals = append(removals, removal)
		}
	}
	sort.Slice(removals, func(i, j int) bool { return removals[i].UserId < removals[j].UserId })
	return removals, skipped
}

func planAssignmentAccessReferences(assignments []accessReferencesAssignment, sites, groups map[string]bool) []accessReferencesAssignmentRemoval {
	removals := []accessReferencesAssignmentRemoval{}
	for _, assignment := range assignments {
		removal := accessReferencesAssignmentRemoval{AssignmentId: assignment.Id.Hex(), OrganisationId: assignment.OrganisationId.Hex(),
			Sites: missingObjectReferences(assignment.Scope.SiteIds, sites), Groups: missingObjectReferences(assignment.Scope.GroupIds, groups)}
		if len(removal.Sites)+len(removal.Groups) > 0 {
			removals = append(removals, removal)
		}
	}
	sort.Slice(removals, func(i, j int) bool { return removals[i].AssignmentId < removals[j].AssignmentId })
	return removals
}

func missingReferences(values []string, existing map[string]bool) []string {
	var missing []string
	for _, value := range values {
		id, err := primitive.ObjectIDFromHex(value)
		if err != nil || id.IsZero() || !existing[id.Hex()] {
			missing = append(missing, value)
		}
	}
	return missing
}

func missingObjectReferences(values []primitive.ObjectID, existing map[string]bool) []primitive.ObjectID {
	var missing []primitive.ObjectID
	for _, value := range values {
		if value.IsZero() || !existing[value.Hex()] {
			missing = append(missing, value)
		}
	}
	return missing
}

// applyAccessReferences pulls the planned ids, and only those, so an
// allow-list changed since the scan keeps its new entries.
func applyAccessReferences(ctx context.Context, db *mongo.Database, users []accessReferencesUserRemoval, assignments []accessReferencesAssignmentRemoval) *accessReferencesWrites {
	written := &accessReferencesWrites{}
	for _, removal := range users {
		id, _ := primitive.ObjectIDFromHex(removal.UserId)
		if _, err := db.Collection(accessReferencesUsers).UpdateOne(ctx, bson.M{"_id": id}, accessReferencesPull("sites", removal.Sites, "groups", removal.Groups)); err != nil {
			fmt.Printf("[error] user %s: %v\n", removal.UserId, err)
			written.Errors++
			continue
		}
		written.Users++
	}
	for _, removal := range assignments {
		id, _ := primitive.ObjectIDFromHex(removal.AssignmentId)
		if _, err := db.Collection(accessReferencesRoleAssignments).UpdateOne(ctx, bson.M{"_id": id}, accessReferencesPull("scope.siteIds", removal.Sites, "scope.groupIds", removal.Groups)); err != nil {
			fmt.Printf("[error] assignment %s: %v\n", removal.AssignmentId, err)
			written.Errors++
			continue
		}
		written.Assignments++
	}
	return written
}

func accessReferencesPull[T any](siteField string, sites []T, groupField string, groups []T) bson.M {
	pull := bson.M{}
	if len(sites) > 0 {
		pull[siteField] = bson.M{"$in": sites}
	}
	if len(groups) > 0 {
		pull[groupField] = bson.M{"$in": groups}
	}
	return bson.M{"$pull": pull}
}
