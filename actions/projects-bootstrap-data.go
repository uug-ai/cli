package actions

import (
	"context"
	"errors"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// projectsBootstrapUser extends the shared bootstrap user projection with the
// project selection this migration owns. Selection (organisationId) stays the
// source of truth: under the hidden single-project rollout the default project
// identity is the organisation identity.
type projectsBootstrapUser struct {
	organisationsBootstrapUser
	ProjectSelection      primitive.ObjectID
	ProjectSelectionState organisationsBootstrapFieldState
}

func parseProjectsBootstrapUser(document bson.Raw) (projectsBootstrapUser, error) {
	base, err := parseOrganisationsBootstrapUser(document)
	if err != nil {
		return projectsBootstrapUser{}, err
	}
	user := projectsBootstrapUser{organisationsBootstrapUser: base}
	user.ProjectSelection, user.ProjectSelectionState = organisationsBootstrapObjectID(document, "projectId")
	return user, nil
}

// projectsBootstrapProjectDocument builds the reserved default project exactly
// as Hub API's ensureDefaultProject would mint it, so a migrated document and a
// lazily-minted one are interchangeable.
func projectsBootstrapProjectDocument(organisationID, ownerID primitive.ObjectID, createdAt time.Time) bson.M {
	return bson.M{
		"_id":            organisationID,
		"organisationId": organisationID,
		"name":           projectsBootstrapDefaultName,
		"slug":           projectsBootstrapDefaultSlug,
		"isActive":       true,
		"audit": bson.M{
			"createdBy":  ownerID.Hex(),
			"createdAt":  createdAt,
			"updatedBy":  ownerID.Hex(),
			"updatedAt":  createdAt,
			"lastAction": projectsBootstrapLastAction,
		},
	}
}

// projectsBootstrapMissingProjectFields reports only absent fields. A populated
// business field is never replaced: the migration fills gaps, it does not
// rewrite tenant data.
func projectsBootstrapMissingProjectFields(document bson.Raw, organisationID, ownerID primitive.ObjectID, createdAt time.Time) bson.M {
	if existingCreatedAt, state := organisationsBootstrapTime(document, "audit", "createdAt"); state == organisationsBootstrapFieldValue {
		createdAt = existingCreatedAt
	}
	candidates := bson.M{
		"organisationId":   organisationID,
		"name":             projectsBootstrapDefaultName,
		"slug":             projectsBootstrapDefaultSlug,
		"isActive":         true,
		"audit.createdBy":  ownerID.Hex(),
		"audit.createdAt":  createdAt,
		"audit.updatedBy":  ownerID.Hex(),
		"audit.updatedAt":  createdAt,
		"audit.lastAction": projectsBootstrapLastAction,
	}

	missing := bson.M{}
	for field, value := range candidates {
		if field == "organisationId" {
			if _, state := organisationsBootstrapObjectID(document, field); state == organisationsBootstrapFieldEmpty {
				missing[field] = value
			}
			continue
		}
		if document.Lookup(splitOrganisationsBootstrapPath(field)...).Type == bsontype.Type(0) {
			missing[field] = value
		}
	}
	return missing
}

func projectsBootstrapProjectFieldsValid(document bson.Raw) bool {
	for _, path := range [][]string{
		{"name"},
		{"slug"},
		{"audit", "createdBy"},
		{"audit", "updatedBy"},
		{"audit", "lastAction"},
	} {
		if _, state := organisationsBootstrapString(document, path...); state != organisationsBootstrapFieldValue {
			return false
		}
	}
	if _, state := organisationsBootstrapBool(document, "isActive"); state != organisationsBootstrapFieldValue {
		return false
	}
	for _, path := range [][]string{{"audit", "createdAt"}, {"audit", "updatedAt"}} {
		if _, state := organisationsBootstrapTime(document, path...); state != organisationsBootstrapFieldValue {
			return false
		}
	}
	return true
}

func projectsBootstrapExistingProjectTypesValid(document bson.Raw) bool {
	checks := []struct {
		path     []string
		expected bsontype.Type
	}{
		{[]string{"name"}, bsontype.String},
		{[]string{"slug"}, bsontype.String},
		{[]string{"isActive"}, bsontype.Boolean},
		{[]string{"audit", "createdBy"}, bsontype.String},
		{[]string{"audit", "createdAt"}, bsontype.DateTime},
		{[]string{"audit", "updatedBy"}, bsontype.String},
		{[]string{"audit", "updatedAt"}, bsontype.DateTime},
		{[]string{"audit", "lastAction"}, bsontype.String},
	}
	for _, check := range checks {
		value := document.Lookup(check.path...)
		if value.Type == bsontype.Type(0) {
			continue
		}
		if value.Type != check.expected {
			return false
		}
		if check.expected == bsontype.String && value.StringValue() == "" {
			return false
		}
	}
	return true
}

// projectsBootstrapDefaultProjectValid mirrors Hub API's validateDefaultProject:
// identity, organisation link, and the reserved slug are the contract that keeps
// ensureDefaultProject from raising ErrDefaultProjectConflict.
func projectsBootstrapDefaultProjectValid(document bson.Raw, organisationID primitive.ObjectID) bool {
	id, idState := organisationsBootstrapObjectID(document, "_id")
	if idState != organisationsBootstrapFieldValue || id != organisationID {
		return false
	}
	link, linkState := organisationsBootstrapObjectID(document, "organisationId")
	if linkState != organisationsBootstrapFieldValue || link != organisationID {
		return false
	}
	slug, slugState := organisationsBootstrapString(document, "slug")
	return slugState == organisationsBootstrapFieldValue && slug == projectsBootstrapDefaultSlug
}

// organisationBootstrapReady delegates the Phase 3 green gate to the shipped
// organisations bootstrap invariants. The delegate only reads r.database and
// r.now, so the throwaway report it writes into is never observed.
func (r *projectsBootstrapRunner) organisationBootstrapReady(ctx context.Context, masterID primitive.ObjectID) (bool, error) {
	delegate := organisationsBootstrapRunner{
		database: r.database,
		now:      r.now,
		report:   &organisationsBootstrapReport{},
	}
	return delegate.ownerBootstrapReady(ctx, masterID)
}

// ensureDefaultProject materializes the reserved default project at
// _id == organisationID. It reports (ok, changed): ok is false only for a
// blocking conflict, changed is true when a write happened or would happen.
func (r *projectsBootstrapRunner) ensureDefaultProject(ctx context.Context, organisationID, actorID primitive.ObjectID) (bool, bool, error) {
	var organisation bson.Raw
	err := r.database.Collection("organisation").FindOne(ctx, bson.M{"_id": organisationID}).Decode(&organisation)
	if errors.Is(err, mongo.ErrNoDocuments) {
		r.report.Projects.Conflicted++
		r.addConflict("organisation-bootstrap-incomplete", actorID.Hex(), organisationID.Hex(), "the referenced organisation does not exist")
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	ownerID, ownerState := organisationsBootstrapObjectID(organisation, "ownerId")
	if ownerState != organisationsBootstrapFieldValue {
		r.report.Projects.Conflicted++
		r.addConflict("organisation-bootstrap-incomplete", actorID.Hex(), organisationID.Hex(), "the referenced organisation has no canonical ownerId")
		return false, false, nil
	}
	createdAt, createdState := organisationsBootstrapTime(organisation, "audit", "createdAt")
	if createdState != organisationsBootstrapFieldValue {
		createdAt = r.now
	}

	legacyDefaults, err := r.inspectLegacyDefaultProjects(ctx, organisationID, actorID)
	if err != nil {
		return false, false, err
	}
	if legacyDefaults > 0 {
		return false, false, nil
	}

	collection := r.database.Collection(projectsBootstrapCollection)
	var stored bson.Raw
	err = collection.FindOne(ctx, bson.M{"_id": organisationID}).Decode(&stored)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return r.createDefaultProject(ctx, organisationID, ownerID, actorID, createdAt)
	}
	if err != nil {
		return false, false, err
	}
	return r.completeDefaultProject(ctx, stored, organisationID, ownerID, actorID, createdAt)
}

// inspectLegacyDefaultProjects fails closed on a default project minted with a
// random _id. Hub API resolves the default by {organisationId, slug} and
// requires exactly one match, so a second document would break every metadata
// read. This tool never deletes or folds it — an operator must decide.
func (r *projectsBootstrapRunner) inspectLegacyDefaultProjects(ctx context.Context, organisationID, actorID primitive.ObjectID) (int64, error) {
	cursor, err := r.database.Collection(projectsBootstrapCollection).Find(ctx, bson.M{
		"organisationId": organisationID,
		"slug":           projectsBootstrapDefaultSlug,
		"_id":            bson.M{"$ne": organisationID},
	}, options.Find().SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return 0, err
	}
	defer cursor.Close(ctx)
	found := int64(0)
	for cursor.Next(ctx) {
		var legacy struct {
			ID primitive.ObjectID `bson:"_id"`
		}
		if err := cursor.Decode(&legacy); err != nil {
			return found, err
		}
		found++
		r.report.Projects.LegacyDefaults++
		r.report.Projects.Conflicted++
		r.addConflict("default-project-conflict", actorID.Hex(), legacy.ID.Hex(), "a default project exists outside the deterministic identity and must be resolved by an operator")
	}
	return found, cursor.Err()
}

func (r *projectsBootstrapRunner) createDefaultProject(ctx context.Context, organisationID, ownerID, actorID primitive.ObjectID, createdAt time.Time) (bool, bool, error) {
	if r.strict {
		r.report.Projects.Conflicted++
		r.addConflict("default-project-missing", actorID.Hex(), organisationID.Hex(), "the deterministic default project does not exist")
		return false, false, nil
	}
	r.report.Projects.Planned++
	if r.config.Mode == "dry-run" {
		return true, true, nil
	}

	collection := r.database.Collection(projectsBootstrapCollection)
	insert := projectsBootstrapProjectDocument(organisationID, ownerID, createdAt)
	// _id is pinned by the filter; repeating it in $setOnInsert would touch an
	// immutable path.
	delete(insert, "_id")

	r.report.Writes.Attempted++
	result, writeErr := collection.UpdateOne(ctx, bson.M{"_id": organisationID}, bson.M{"$setOnInsert": insert}, options.Update().SetUpsert(true))
	if writeErr == nil && result.UpsertedCount == 1 {
		r.report.Writes.Applied++
		r.report.Projects.Inserted++
	}

	var stored bson.Raw
	if err := collection.FindOne(ctx, bson.M{"_id": organisationID}).Decode(&stored); err != nil {
		if writeErr != nil {
			r.report.Writes.Failed++
			return false, true, errors.Join(writeErr, err)
		}
		return false, true, err
	}
	if !projectsBootstrapDefaultProjectValid(stored, organisationID) {
		if writeErr != nil {
			r.report.Writes.Failed++
		}
		r.report.Projects.Conflicted++
		r.addConflict("default-project-reconcile-failed", actorID.Hex(), organisationID.Hex(), "the default project write did not reconcile to the deterministic identity")
		return false, true, nil
	}
	if writeErr != nil || result.UpsertedCount != 1 {
		r.report.Writes.Reconciled++
		r.report.Projects.Reconciled++
	}
	return true, true, nil
}

func (r *projectsBootstrapRunner) completeDefaultProject(ctx context.Context, stored bson.Raw, organisationID, ownerID, actorID primitive.ObjectID, createdAt time.Time) (bool, bool, error) {
	if !organisationsBootstrapContainerValid(stored, "audit") {
		r.report.Projects.Conflicted++
		r.addConflict("invalid-project-document", actorID.Hex(), organisationID.Hex(), "project audit must be an embedded document")
		return false, false, nil
	}
	if !projectsBootstrapExistingProjectTypesValid(stored) {
		r.report.Projects.Conflicted++
		r.addConflict("invalid-project-document", actorID.Hex(), organisationID.Hex(), "existing project fields have unexpected BSON types")
		return false, false, nil
	}
	if link, state := organisationsBootstrapObjectID(stored, "organisationId"); state == organisationsBootstrapFieldValue && link != organisationID {
		r.report.Projects.Conflicted++
		r.addConflict("default-project-conflict", actorID.Hex(), organisationID.Hex(), "the project at the deterministic identity belongs to another organisation")
		return false, false, nil
	}
	if slug, state := organisationsBootstrapString(stored, "slug"); state == organisationsBootstrapFieldValue && slug != projectsBootstrapDefaultSlug {
		r.report.Projects.Conflicted++
		r.addConflict("default-project-conflict", actorID.Hex(), organisationID.Hex(), "the project at the deterministic identity is not the reserved default project")
		return false, false, nil
	}

	missing := projectsBootstrapMissingProjectFields(stored, organisationID, ownerID, createdAt)
	if len(missing) == 0 && projectsBootstrapDefaultProjectValid(stored, organisationID) && projectsBootstrapProjectFieldsValid(stored) {
		r.report.Projects.AlreadyPresent++
		return true, false, nil
	}
	if r.strict {
		r.report.Projects.Conflicted++
		r.addConflict("default-project-incomplete", actorID.Hex(), organisationID.Hex(), "the default project is missing required canonical fields")
		return false, false, nil
	}
	r.report.Projects.Planned++
	if r.config.Mode == "dry-run" {
		return true, true, nil
	}

	collection := r.database.Collection(projectsBootstrapCollection)
	for _, field := range sortedOrganisationsBootstrapFields(missing) {
		r.report.Writes.Attempted++
		if _, err := collection.UpdateOne(
			ctx,
			organisationsBootstrapMissingFieldFilter(organisationID, field),
			bson.M{"$set": bson.M{field: missing[field]}},
		); err != nil {
			r.report.Writes.Failed++
			return false, true, err
		}
		r.report.Writes.Applied++
	}

	var reconciled bson.Raw
	if err := collection.FindOne(ctx, bson.M{"_id": organisationID}).Decode(&reconciled); err != nil {
		return false, true, err
	}
	if !projectsBootstrapDefaultProjectValid(reconciled, organisationID) || !projectsBootstrapProjectFieldsValid(reconciled) {
		r.report.Projects.Conflicted++
		r.addConflict("default-project-reconcile-failed", actorID.Hex(), organisationID.Hex(), "the default project completion did not reconcile")
		return false, true, nil
	}
	r.report.Writes.Reconciled++
	r.report.Projects.Completed++
	return true, true, nil
}

// ensureUserProjectSelection initializes users.projectId from the user's own
// organisationId, missing-only. Under the hidden single-project rollout a
// non-zero projectId that disagrees with organisationId is a blocking conflict.
func (r *projectsBootstrapRunner) ensureUserProjectSelection(ctx context.Context, user projectsBootstrapUser, targetID, actorID primitive.ObjectID) (bool, bool, error) {
	if targetID.IsZero() {
		r.addConflict("organisation-bootstrap-incomplete", actorID.Hex(), user.ID.Hex(), "users.organisationId must be initialized before users.projectId")
		return false, false, nil
	}
	if user.ProjectSelectionState == organisationsBootstrapFieldWrong {
		r.addConflict("invalid-user-project-selection", actorID.Hex(), user.ID.Hex(), "projectId must be a BSON ObjectID")
		return false, false, nil
	}
	if user.ProjectSelectionState == organisationsBootstrapFieldValue {
		if user.ProjectSelection != targetID {
			r.report.Users.MissingSelectedScope++
			r.addConflict("invalid-user-project-selection", actorID.Hex(), user.ID.Hex(), "preserved projectId does not match the organisation default project")
			return false, false, nil
		}
		r.report.Users.SelectionsPreserved++
		return true, false, nil
	}
	if r.strict {
		r.addConflict("user-project-selection-missing", actorID.Hex(), user.ID.Hex(), "required projectId is missing")
		return false, false, nil
	}
	if r.config.Mode == "dry-run" {
		return true, true, nil
	}

	collection := r.database.Collection("users")
	filter := bson.M{
		"_id": user.ID,
		"$or": bson.A{
			bson.M{"projectId": bson.M{"$exists": false}},
			bson.M{"projectId": nil},
			bson.M{"projectId": primitive.NilObjectID},
		},
	}
	r.report.Writes.Attempted++
	result, writeErr := collection.UpdateOne(ctx, filter, bson.M{"$set": bson.M{"projectId": targetID}})
	if writeErr == nil && result.ModifiedCount == 1 {
		r.report.Writes.Applied++
	}

	var stored bson.Raw
	if err := collection.FindOne(ctx, bson.M{"_id": user.ID}).Decode(&stored); err != nil {
		if writeErr != nil {
			r.report.Writes.Failed++
			return false, true, errors.Join(writeErr, err)
		}
		return false, true, err
	}
	selection, state := organisationsBootstrapObjectID(stored, "projectId")
	if state != organisationsBootstrapFieldValue || selection != targetID {
		if writeErr != nil {
			r.report.Writes.Failed++
		}
		r.addConflict("user-project-selection-reconcile-failed", actorID.Hex(), user.ID.Hex(), "the projectId update did not reconcile to the intended value")
		return false, true, nil
	}
	if writeErr != nil || result.ModifiedCount != 1 {
		r.report.Writes.Reconciled++
	}
	return true, true, nil
}

// projectsBootstrapOwnerReady is the sub-users stage gate: the owners stage must
// be complete for this master before its sub-users are given a project
// selection.
func (r *projectsBootstrapRunner) projectsBootstrapOwnerReady(ctx context.Context, masterID primitive.ObjectID) (bool, error) {
	ready, err := r.organisationBootstrapReady(ctx, masterID)
	if err != nil || !ready {
		return false, err
	}
	var masterDocument bson.Raw
	if err := r.database.Collection("users").FindOne(ctx, bson.M{"_id": masterID}).Decode(&masterDocument); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return false, nil
		}
		return false, err
	}
	master, err := parseProjectsBootstrapUser(masterDocument)
	if err != nil {
		return false, err
	}
	if master.ProjectSelectionState != organisationsBootstrapFieldValue || master.ProjectSelection != master.Selection {
		return false, nil
	}
	targets, err := r.ownedOrganisations(ctx, masterID)
	if err != nil {
		return false, err
	}
	targets = append(targets, master.Selection)
	for _, organisationID := range targets {
		var stored bson.Raw
		err := r.database.Collection(projectsBootstrapCollection).FindOne(ctx, bson.M{"_id": organisationID}).Decode(&stored)
		if errors.Is(err, mongo.ErrNoDocuments) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if !projectsBootstrapDefaultProjectValid(stored, organisationID) || !projectsBootstrapProjectFieldsValid(stored) {
			return false, nil
		}
	}
	return true, nil
}

func (r *projectsBootstrapRunner) runSubUsers(ctx context.Context) error {
	masterFilter, err := r.masterFilter(ctx)
	if err != nil {
		return err
	}
	masterFilter = organisationsBootstrapResumeFilter(masterFilter, r.checkpointLastMaster)
	masters, err := r.database.Collection("users").Find(ctx, masterFilter, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetBatchSize(int32(r.config.BatchSize)))
	if err != nil {
		return err
	}
	defer masters.Close(ctx)
	mastersScanned := int64(0)
	for masters.Next(ctx) {
		if r.interrupted() {
			return errProjectsBootstrapInterrupted
		}
		if err := r.renewCheckpoint(ctx); err != nil {
			return err
		}
		master, parseErr := parseProjectsBootstrapUser(masters.Current)
		if parseErr != nil || master.ParentState != organisationsBootstrapFieldEmpty {
			r.addConflict("invalid-master", "", "", "master user is invalid during sub-user processing")
			if r.config.StopOnConflict {
				break
			}
			continue
		}
		mastersScanned++
		ready, readyErr := r.projectsBootstrapOwnerReady(ctx, master.ID)
		if readyErr != nil {
			return readyErr
		}
		if !ready {
			r.report.SubUsers.Conflicted++
			r.addConflict("owner-bootstrap-incomplete", master.ID.Hex(), master.ID.Hex(), "run the owners stage to green for this master before its sub-users")
			if r.config.StopOnConflict {
				break
			}
			continue
		}

		subUsers, findErr := r.database.Collection("users").Find(ctx, bson.M{"user_id": master.ID.Hex()}, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetBatchSize(int32(r.config.BatchSize)))
		if findErr != nil {
			return findErr
		}
		for subUsers.Next(ctx) {
			if err := r.renewCheckpoint(ctx); err != nil {
				subUsers.Close(ctx)
				return err
			}
			user, userErr := parseProjectsBootstrapUser(subUsers.Current)
			r.report.SubUsers.Scanned++
			if userErr != nil || user.ParentState != organisationsBootstrapFieldValue || user.ParentID != master.ID {
				r.report.SubUsers.Orphaned++
				r.addConflict("invalid-sub-user-parent", master.ID.Hex(), "", "sub-user relationship changed during processing")
				if r.config.StopOnConflict {
					break
				}
				continue
			}
			if processErr := r.processSubUser(ctx, user, master.ID); processErr != nil {
				subUsers.Close(ctx)
				return processErr
			}
			if r.hasConflict && r.config.StopOnConflict {
				break
			}
		}
		if cursorErr := subUsers.Err(); cursorErr != nil {
			subUsers.Close(ctx)
			return cursorErr
		}
		subUsers.Close(ctx)
		if r.blockingConflictCount == 0 {
			if checkpointErr := r.advanceCheckpoint(ctx, master.ID); checkpointErr != nil {
				return checkpointErr
			}
		}
		if r.interrupted() {
			return errProjectsBootstrapInterrupted
		}
		if r.hasConflict && r.config.StopOnConflict {
			break
		}
	}
	if err := masters.Err(); err != nil {
		return err
	}
	if mastersScanned == 0 && r.checkpointLastMaster.IsZero() && (r.config.Username != "" || r.config.OrganisationID != "") {
		r.addConflict("master-scope-not-found", "", "", "the requested scope does not resolve to a master user")
	}
	return nil
}

// processSubUser materializes the default project for every organisation the
// sub-user can select into — its own organisationId plus any secondary
// organisation it owns — then initializes its projectId.
func (r *projectsBootstrapRunner) processSubUser(ctx context.Context, user projectsBootstrapUser, masterID primitive.ObjectID) error {
	if user.SelectionState != organisationsBootstrapFieldValue {
		r.report.SubUsers.Conflicted++
		r.addConflict("organisation-bootstrap-incomplete", masterID.Hex(), user.ID.Hex(), "sub-user has no canonical organisationId")
		r.report.Verification.Failed++
		return nil
	}

	targetSet := map[primitive.ObjectID]struct{}{user.Selection: {}}
	ownedCursor, err := r.database.Collection("organisation").Find(ctx, bson.M{"ownerId": user.ID}, options.Find().SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return err
	}
	for ownedCursor.Next(ctx) {
		var organisation struct {
			ID primitive.ObjectID `bson:"_id"`
		}
		if err := ownedCursor.Decode(&organisation); err != nil {
			ownedCursor.Close(ctx)
			return err
		}
		targetSet[organisation.ID] = struct{}{}
		r.report.Organisations.SecondaryOwned++
	}
	if err := ownedCursor.Err(); err != nil {
		ownedCursor.Close(ctx)
		return err
	}
	ownedCursor.Close(ctx)

	targets := make([]primitive.ObjectID, 0, len(targetSet))
	for organisationID := range targetSet {
		targets = append(targets, organisationID)
	}
	sort.Slice(targets, func(left, right int) bool { return targets[left].Hex() < targets[right].Hex() })

	projectChanged := false
	for _, organisationID := range targets {
		r.report.Organisations.Scanned++
		ok, changed, ensureErr := r.ensureDefaultProject(ctx, organisationID, masterID)
		if ensureErr != nil {
			return ensureErr
		}
		if !ok {
			r.report.SubUsers.Conflicted++
			r.report.Verification.Failed++
			return nil
		}
		projectChanged = projectChanged || changed
	}

	selectionOK, selectionChanged, err := r.ensureUserProjectSelection(ctx, user, user.Selection, masterID)
	if err != nil {
		return err
	}
	if !selectionOK {
		r.report.SubUsers.Conflicted++
		r.report.Verification.Failed++
		return nil
	}
	if selectionChanged {
		if r.config.Mode == "live" {
			r.report.SubUsers.Updated++
			r.report.Users.SubUsersUpdated++
		}
	} else {
		r.report.SubUsers.AlreadySelected++
	}
	if projectChanged || selectionChanged {
		r.report.SubUsers.Planned++
	}
	r.report.Verification.Passed++
	return nil
}
