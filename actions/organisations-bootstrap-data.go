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

type organisationsBootstrapFieldState int

const (
	organisationsBootstrapFieldEmpty organisationsBootstrapFieldState = iota
	organisationsBootstrapFieldValue
	organisationsBootstrapFieldWrong
)

type organisationsBootstrapUser struct {
	ID                    primitive.ObjectID
	Username              string
	OrganisationName      string
	ParentID              primitive.ObjectID
	ParentState           organisationsBootstrapFieldState
	Selection             primitive.ObjectID
	SelectionState        organisationsBootstrapFieldState
	Timezone              string
	Domain                string
	CreatedAt             time.Time
	OrganisationCreatedAt time.Time
	OrganisationUpdatedAt time.Time
}

type organisationsBootstrapLegacyCandidate struct {
	ID        primitive.ObjectID
	Name      string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func parseOrganisationsBootstrapUser(document bson.Raw) (organisationsBootstrapUser, error) {
	user := organisationsBootstrapUser{}
	id, state := organisationsBootstrapObjectID(document, "_id")
	if state != organisationsBootstrapFieldValue {
		return user, errors.New("user _id must be a non-zero BSON ObjectID")
	}
	user.ID = id
	user.Username, _ = organisationsBootstrapString(document, "username")
	user.Timezone, _ = organisationsBootstrapString(document, "timezone")
	user.Domain, _ = organisationsBootstrapString(document, "domain")
	user.CreatedAt, _ = organisationsBootstrapTime(document, "created_at")

	parentHex, parentState := organisationsBootstrapString(document, "user_id")
	user.ParentState = parentState
	if parentState == organisationsBootstrapFieldValue {
		parentID, err := primitive.ObjectIDFromHex(parentHex)
		if err != nil {
			user.ParentState = organisationsBootstrapFieldWrong
		} else {
			user.ParentID = parentID
		}
	}
	user.Selection, user.SelectionState = organisationsBootstrapObjectID(document, "organisation_id")
	return user, nil
}

func organisationsBootstrapObjectID(document bson.Raw, path ...string) (primitive.ObjectID, organisationsBootstrapFieldState) {
	value := document.Lookup(path...)
	switch value.Type {
	case bsontype.Type(0), bsontype.Null, bsontype.Undefined:
		return primitive.NilObjectID, organisationsBootstrapFieldEmpty
	case bsontype.ObjectID:
		id := value.ObjectID()
		if id.IsZero() {
			return primitive.NilObjectID, organisationsBootstrapFieldEmpty
		}
		return id, organisationsBootstrapFieldValue
	default:
		return primitive.NilObjectID, organisationsBootstrapFieldWrong
	}
}

func organisationsBootstrapString(document bson.Raw, path ...string) (string, organisationsBootstrapFieldState) {
	value := document.Lookup(path...)
	switch value.Type {
	case bsontype.Type(0), bsontype.Null, bsontype.Undefined:
		return "", organisationsBootstrapFieldEmpty
	case bsontype.String:
		text := value.StringValue()
		if text == "" {
			return "", organisationsBootstrapFieldEmpty
		}
		return text, organisationsBootstrapFieldValue
	default:
		return "", organisationsBootstrapFieldWrong
	}
}

func organisationsBootstrapTime(document bson.Raw, path ...string) (time.Time, organisationsBootstrapFieldState) {
	value := document.Lookup(path...)
	switch value.Type {
	case bsontype.Type(0), bsontype.Null, bsontype.Undefined:
		return time.Time{}, organisationsBootstrapFieldEmpty
	case bsontype.DateTime:
		return value.Time(), organisationsBootstrapFieldValue
	default:
		return time.Time{}, organisationsBootstrapFieldWrong
	}
}

func organisationsBootstrapBool(document bson.Raw, path ...string) (bool, organisationsBootstrapFieldState) {
	value := document.Lookup(path...)
	switch value.Type {
	case bsontype.Type(0), bsontype.Null, bsontype.Undefined:
		return false, organisationsBootstrapFieldEmpty
	case bsontype.Boolean:
		return value.Boolean(), organisationsBootstrapFieldValue
	default:
		return false, organisationsBootstrapFieldWrong
	}
}

func organisationsBootstrapOrganisationDocument(user organisationsBootstrapUser, now time.Time) bson.M {
	createdAt, updatedAt := organisationsBootstrapOrganisationTimestamps(user, now)
	document := bson.M{
		"_id":      user.ID,
		"name":     organisationsBootstrapOrganisationName(user),
		"ownerId":  user.ID,
		"isActive": true,
		"audit": bson.M{
			"createdBy":  user.ID.Hex(),
			"createdAt":  createdAt,
			"updatedBy":  user.ID.Hex(),
			"updatedAt":  updatedAt,
			"lastAction": "organisation.migrated",
		},
	}
	if user.Timezone != "" {
		document["settings"] = bson.M{"timezone": user.Timezone}
	}
	return document
}

func organisationsBootstrapMissingOrganisationFields(document bson.Raw, user organisationsBootstrapUser, now time.Time) bson.M {
	createdAt, updatedAt := organisationsBootstrapOrganisationTimestamps(user, now)
	if legacyCreatedAt, state := organisationsBootstrapTime(document, "created_at"); state == organisationsBootstrapFieldValue {
		createdAt = legacyCreatedAt
	}
	if canonicalCreatedAt, state := organisationsBootstrapTime(document, "audit", "createdAt"); state == organisationsBootstrapFieldValue {
		createdAt = canonicalCreatedAt
	}
	if legacyUpdatedAt, state := organisationsBootstrapTime(document, "updated_at"); state == organisationsBootstrapFieldValue {
		updatedAt = legacyUpdatedAt
	} else if user.OrganisationUpdatedAt.IsZero() {
		updatedAt = createdAt
	}

	candidates := bson.M{
		"ownerId":          user.ID,
		"name":             organisationsBootstrapOrganisationName(user),
		"isActive":         true,
		"audit.createdBy":  user.ID.Hex(),
		"audit.createdAt":  createdAt,
		"audit.updatedBy":  user.ID.Hex(),
		"audit.updatedAt":  updatedAt,
		"audit.lastAction": "organisation.migrated",
	}
	if user.Timezone != "" {
		candidates["settings.timezone"] = user.Timezone
	}

	missing := bson.M{}
	for field, value := range candidates {
		if field == "ownerId" {
			_, state := organisationsBootstrapObjectID(document, field)
			if state == organisationsBootstrapFieldEmpty {
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

func organisationsBootstrapOrganisationName(user organisationsBootstrapUser) string {
	if user.OrganisationName != "" {
		return user.OrganisationName
	}
	return user.Username
}

func organisationsBootstrapOrganisationTimestamps(user organisationsBootstrapUser, now time.Time) (time.Time, time.Time) {
	createdAt := user.OrganisationCreatedAt
	if createdAt.IsZero() {
		createdAt = user.CreatedAt
	}
	if createdAt.IsZero() {
		createdAt = now
	}
	updatedAt := user.OrganisationUpdatedAt
	if updatedAt.IsZero() {
		updatedAt = createdAt
	}
	return createdAt, updatedAt
}

func (r *organisationsBootstrapRunner) legacyOrganisationCandidates(ctx context.Context, masterID primitive.ObjectID, report bool) ([]organisationsBootstrapLegacyCandidate, bool, error) {
	cursor, err := r.database.Collection("organisation").Find(ctx, bson.M{
		"_id":      bson.M{"$ne": masterID},
		"owner_id": masterID.Hex(),
	})
	if err != nil {
		return nil, false, err
	}
	defer cursor.Close(ctx)
	candidates := []organisationsBootstrapLegacyCandidate{}
	invalid := false
	for cursor.Next(ctx) {
		var document bson.Raw
		if err := cursor.Decode(&document); err != nil {
			return nil, false, err
		}
		documentID, idState := organisationsBootstrapObjectID(document, "_id")
		ownerID, ownerState := organisationsBootstrapObjectID(document, "ownerId")
		if idState != organisationsBootstrapFieldValue {
			invalid = true
			continue
		}
		if ownerState == organisationsBootstrapFieldValue && ownerID == masterID {
			continue
		}
		if ownerState != organisationsBootstrapFieldEmpty || document.Lookup("ownerId").Type != bsontype.Type(0) {
			invalid = true
			if report {
				r.addConflict("legacy-organisation-owner-conflict", masterID.Hex(), documentID.Hex(), "legacy organisation has an invalid or conflicting canonical owner")
			}
			continue
		}
		candidate := organisationsBootstrapLegacyCandidate{ID: documentID}
		candidate.Name, _ = organisationsBootstrapString(document, "name")
		candidate.CreatedAt, _ = organisationsBootstrapTime(document, "created_at")
		candidate.UpdatedAt, _ = organisationsBootstrapTime(document, "updated_at")
		candidates = append(candidates, candidate)
	}
	if err := cursor.Err(); err != nil {
		return nil, false, err
	}
	if report && len(candidates) > 0 {
		ids := make([]primitive.ObjectID, len(candidates))
		for index := range candidates {
			ids[index] = candidates[index].ID
		}
		referenced, err := r.database.Collection("users").CountDocuments(ctx, bson.M{"organisation_id": bson.M{"$in": ids}})
		if err != nil {
			return nil, false, err
		}
		r.report.Organisations.Referenced += referenced
	}
	return candidates, invalid, nil
}

func organisationsBootstrapContainerValid(document bson.Raw, field string) bool {
	typeOfField := document.Lookup(field).Type
	return typeOfField == bsontype.Type(0) || typeOfField == bsontype.EmbeddedDocument
}

func organisationsBootstrapOrganisationFieldsValid(document bson.Raw, requireTimezone bool) bool {
	if _, state := organisationsBootstrapString(document, "name"); state != organisationsBootstrapFieldValue {
		return false
	}
	if _, state := organisationsBootstrapBool(document, "isActive"); state != organisationsBootstrapFieldValue {
		return false
	}
	for _, path := range [][]string{
		{"audit", "createdBy"},
		{"audit", "updatedBy"},
		{"audit", "lastAction"},
	} {
		if _, state := organisationsBootstrapString(document, path...); state != organisationsBootstrapFieldValue {
			return false
		}
	}
	for _, path := range [][]string{
		{"audit", "createdAt"},
		{"audit", "updatedAt"},
	} {
		if _, state := organisationsBootstrapTime(document, path...); state != organisationsBootstrapFieldValue {
			return false
		}
	}
	if requireTimezone {
		if _, state := organisationsBootstrapString(document, "settings", "timezone"); state != organisationsBootstrapFieldValue {
			return false
		}
	}
	return true
}

func organisationsBootstrapExistingOrganisationTypesValid(document bson.Raw, requireTimezone bool) bool {
	checks := []struct {
		path     []string
		expected bsontype.Type
	}{
		{[]string{"name"}, bsontype.String},
		{[]string{"isActive"}, bsontype.Boolean},
		{[]string{"audit", "createdBy"}, bsontype.String},
		{[]string{"audit", "createdAt"}, bsontype.DateTime},
		{[]string{"audit", "updatedBy"}, bsontype.String},
		{[]string{"audit", "updatedAt"}, bsontype.DateTime},
		{[]string{"audit", "lastAction"}, bsontype.String},
	}
	if requireTimezone {
		checks = append(checks, struct {
			path     []string
			expected bsontype.Type
		}{[]string{"settings", "timezone"}, bsontype.String})
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

func organisationsBootstrapMissingFieldFilter(documentID primitive.ObjectID, field string) bson.M {
	filter := bson.M{"_id": documentID, field: bson.M{"$exists": false}}
	if field == "ownerId" {
		filter["$or"] = bson.A{
			bson.M{"ownerId": bson.M{"$exists": false}},
			bson.M{"ownerId": nil},
			bson.M{"ownerId": primitive.NilObjectID},
		}
		delete(filter, field)
	}
	return filter
}

func splitOrganisationsBootstrapPath(path string) []string {
	for index := range path {
		if path[index] == '.' {
			return []string{path[:index], path[index+1:]}
		}
	}
	return []string{path}
}

func sortedOrganisationsBootstrapFields(fields bson.M) []string {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (r *organisationsBootstrapRunner) organisationAccessibleToMaster(ctx context.Context, organisationID, masterID primitive.ObjectID) (bool, error) {
	return r.organisationAccessibleToUser(ctx, organisationID, masterID, masterID)
}

func (r *organisationsBootstrapRunner) organisationAccessibleToUser(ctx context.Context, organisationID, userID, masterID primitive.ObjectID) (bool, error) {
	var organisation bson.Raw
	err := r.database.Collection("organisation").FindOne(ctx, bson.M{"_id": organisationID}).Decode(&organisation)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	ownerID, ownerState := organisationsBootstrapObjectID(organisation, "ownerId")
	if ownerState == organisationsBootstrapFieldValue {
		if ownerID == masterID || ownerID == userID {
			return true, nil
		}
		return organisationsBootstrapMembershipActive(ctx, r.database.Collection("organisation_users"), bson.M{
			"userId":         userID,
			"organisationId": organisationID,
		}, r.now)
	}
	if ownerState == organisationsBootstrapFieldWrong {
		return false, nil
	}
	legacyOwner, legacyState := organisationsBootstrapString(organisation, "owner_id")
	if legacyState == organisationsBootstrapFieldValue && (legacyOwner == masterID.Hex() || legacyOwner == userID.Hex()) {
		return true, nil
	}
	return organisationsBootstrapMembershipActive(ctx, r.database.Collection("organisation_users"), bson.M{
		"userId":         userID,
		"organisationId": organisationID,
	}, r.now)
}

func (r *organisationsBootstrapRunner) ensureMembership(ctx context.Context, userID, organisationID, actorID primitive.ObjectID) (bool, error) {
	collection := r.database.Collection("organisation_users")
	filter := bson.M{"userId": userID, "organisationId": organisationID}
	count, err := collection.CountDocuments(ctx, filter, options.Count().SetLimit(2))
	if err != nil {
		return false, err
	}
	if count > 1 {
		r.report.Memberships.Conflicted++
		r.addConflict("duplicate-membership", actorID.Hex(), userID.Hex(), "more than one membership exists for the user and organisation pair")
		return false, nil
	}
	if count == 1 {
		active, activeErr := organisationsBootstrapMembershipActive(ctx, collection, filter, r.now)
		if activeErr != nil {
			return false, activeErr
		}
		if !active {
			r.report.Memberships.Conflicted++
			r.addConflict("inactive-membership", actorID.Hex(), userID.Hex(), "existing membership is not effectively active")
			return false, nil
		}
		r.report.Memberships.AlreadyPresent++
		return true, nil
	}

	if r.strict {
		r.report.Memberships.Conflicted++
		r.addConflict("membership-missing", actorID.Hex(), userID.Hex(), "required active membership is missing")
		return false, nil
	}
	r.report.Memberships.Planned++
	if r.config.Mode == "dry-run" {
		return true, nil
	}
	membership := bson.M{
		"_id":            primitive.NewObjectID(),
		"userId":         userID,
		"organisationId": organisationID,
		"status":         "active",
		"joinedAt":       r.now,
		"audit": bson.M{
			"createdBy":  actorID.Hex(),
			"createdAt":  r.now,
			"updatedBy":  actorID.Hex(),
			"updatedAt":  r.now,
			"lastAction": "organisation.membership.created",
		},
	}
	r.report.Writes.Attempted++
	result, writeErr := collection.UpdateOne(ctx, filter, bson.M{"$setOnInsert": membership}, options.Update().SetUpsert(true))
	if writeErr != nil && !mongo.IsDuplicateKeyError(writeErr) {
		r.report.Writes.Failed++
		return false, writeErr
	}
	if writeErr == nil && result.UpsertedCount == 1 {
		r.report.Writes.Applied++
		r.report.Memberships.Inserted++
	}
	active, reconcileErr := organisationsBootstrapMembershipActive(ctx, collection, filter, r.now)
	if reconcileErr != nil {
		return false, reconcileErr
	}
	if !active {
		r.report.Memberships.Conflicted++
		r.addConflict("membership-reconcile-failed", actorID.Hex(), userID.Hex(), "membership write did not reconcile as active")
		return false, nil
	}
	r.report.Memberships.Reconciled++
	r.report.Writes.Reconciled++
	return true, nil
}

func organisationsBootstrapMembershipActive(ctx context.Context, collection *mongo.Collection, filter bson.M, now time.Time) (bool, error) {
	var membership bson.Raw
	if err := collection.FindOne(ctx, filter).Decode(&membership); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return false, nil
		}
		return false, err
	}
	status, state := organisationsBootstrapString(membership, "status")
	if state != organisationsBootstrapFieldValue || status != "active" {
		return false, nil
	}
	expiresAt, expiryState := organisationsBootstrapTime(membership, "expiresAt")
	if expiryState == organisationsBootstrapFieldWrong {
		return false, nil
	}
	return expiryState != organisationsBootstrapFieldValue || expiresAt.After(now), nil
}

func (r *organisationsBootstrapRunner) ensureUserSelection(ctx context.Context, user organisationsBootstrapUser, targetID, masterID primitive.ObjectID) (bool, bool, error) {
	if user.SelectionState == organisationsBootstrapFieldWrong {
		r.addConflict("invalid-user-selection", masterID.Hex(), user.ID.Hex(), "organisation_id must be a BSON ObjectID")
		return false, false, nil
	}
	if user.SelectionState == organisationsBootstrapFieldValue {
		accessible, err := r.organisationAccessibleToUser(ctx, user.Selection, user.ID, masterID)
		if err != nil {
			return false, false, err
		}
		if !accessible {
			r.report.Users.MissingSelectedOrganisation++
			r.addConflict("invalid-user-selection", masterID.Hex(), user.ID.Hex(), "preserved organisation_id is missing or not owned by the master")
			return false, false, nil
		}
		return true, false, nil
	}
	if r.strict {
		r.addConflict("user-selection-missing", masterID.Hex(), user.ID.Hex(), "required organisation_id is missing")
		return false, false, nil
	}
	if r.config.Mode == "dry-run" {
		return true, false, nil
	}

	collection := r.database.Collection("users")
	filter := bson.M{
		"_id": user.ID,
		"$or": bson.A{
			bson.M{"organisation_id": bson.M{"$exists": false}},
			bson.M{"organisation_id": nil},
			bson.M{"organisation_id": primitive.NilObjectID},
		},
	}
	r.report.Writes.Attempted++
	result, err := collection.UpdateOne(ctx, filter, bson.M{"$set": bson.M{"organisation_id": targetID}})
	if err != nil {
		r.report.Writes.Failed++
		return false, false, err
	}
	if result.ModifiedCount == 1 {
		r.report.Writes.Applied++
	}

	var stored bson.Raw
	if err := collection.FindOne(ctx, bson.M{"_id": user.ID}).Decode(&stored); err != nil {
		return false, false, err
	}
	selection, state := organisationsBootstrapObjectID(stored, "organisation_id")
	if state != organisationsBootstrapFieldValue {
		r.addConflict("user-selection-reconcile-failed", masterID.Hex(), user.ID.Hex(), "organisation_id update did not reconcile")
		return false, true, nil
	}
	accessible, err := r.organisationAccessibleToUser(ctx, selection, user.ID, masterID)
	if err != nil {
		return false, true, err
	}
	if !accessible {
		r.addConflict("user-selection-concurrent-conflict", masterID.Hex(), user.ID.Hex(), "concurrent organisation selection is not owned by the master")
		return false, true, nil
	}
	r.report.Writes.Reconciled++
	return true, result.ModifiedCount == 1, nil
}

func (r *organisationsBootstrapRunner) runSubUsers(ctx context.Context) error {
	conflictsBeforePreflight := r.conflictCount
	if err := r.inspectSubUserOrphans(ctx); err != nil {
		return err
	}
	if r.config.Mode == "live" && r.conflictCount > conflictsBeforePreflight {
		return nil
	}
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
		if err := r.renewCheckpoint(ctx); err != nil {
			return err
		}
		master, parseErr := parseOrganisationsBootstrapUser(masters.Current)
		if parseErr != nil || master.ParentState != organisationsBootstrapFieldEmpty {
			r.addConflict("invalid-master", "", "", "master user is invalid during sub-user processing")
			if r.config.StopOnConflict {
				break
			}
			continue
		}
		mastersScanned++
		conflictsBefore := r.conflictCount
		ready, readyErr := r.ownerBootstrapReady(ctx, master.ID)
		if readyErr != nil {
			return readyErr
		}
		if !ready {
			r.report.SubUsers.Conflicted++
			r.addConflict("owner-bootstrap-incomplete", master.ID.Hex(), master.ID.Hex(), "owner stage invariants are not complete")
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
			user, userErr := parseOrganisationsBootstrapUser(subUsers.Current)
			r.report.SubUsers.Scanned++
			if userErr != nil || user.ParentState != organisationsBootstrapFieldValue || user.ParentID != master.ID {
				r.report.SubUsers.Orphaned++
				r.addConflict("invalid-sub-user-parent", master.ID.Hex(), "", "sub-user relationship changed during processing")
				if r.config.StopOnConflict {
					break
				}
				continue
			}
			if processErr := r.processSubUser(ctx, user); processErr != nil {
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
		if conflictsBefore == r.conflictCount && !r.hasConflict {
			if checkpointErr := r.advanceCheckpoint(ctx, master.ID); checkpointErr != nil {
				return checkpointErr
			}
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

func (r *organisationsBootstrapRunner) inspectSubUserOrphans(ctx context.Context) error {
	filter := bson.M{"user_id": bson.M{"$exists": true, "$nin": bson.A{nil, ""}}}
	if masterID, scoped, err := r.scopedMasterID(ctx); err != nil {
		return err
	} else if scoped {
		filter["user_id"] = masterID.Hex()
	}
	cursor, err := r.database.Collection("users").Find(ctx, filter, options.Find().SetProjection(bson.M{"_id": 1, "user_id": 1}))
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)
	masterExists := map[primitive.ObjectID]bool{}
	masterChecked := map[primitive.ObjectID]bool{}
	for cursor.Next(ctx) {
		user, parseErr := parseOrganisationsBootstrapUser(cursor.Current)
		if parseErr != nil || user.ParentState != organisationsBootstrapFieldValue {
			r.report.SubUsers.Scanned++
			r.report.SubUsers.Orphaned++
			documentID := ""
			if parseErr == nil {
				documentID = user.ID.Hex()
			}
			r.addConflict("invalid-sub-user-parent", "", documentID, "sub-user user_id must contain a valid master ObjectID hex")
			continue
		}
		if !masterChecked[user.ParentID] {
			count, countErr := r.database.Collection("users").CountDocuments(ctx, bson.M{
				"_id": user.ParentID,
				"$or": bson.A{
					bson.M{"user_id": bson.M{"$exists": false}},
					bson.M{"user_id": nil},
					bson.M{"user_id": ""},
				},
			}, options.Count().SetLimit(1))
			if countErr != nil {
				return countErr
			}
			masterExists[user.ParentID] = count == 1
			masterChecked[user.ParentID] = true
		}
		if !masterExists[user.ParentID] {
			r.report.SubUsers.Scanned++
			r.report.SubUsers.Orphaned++
			r.addConflict("orphan-sub-user", user.ParentID.Hex(), user.ID.Hex(), "sub-user user_id does not resolve to a master user")
		}
	}
	return cursor.Err()
}

func (r *organisationsBootstrapRunner) ownerBootstrapReady(ctx context.Context, masterID primitive.ObjectID) (bool, error) {
	var masterDocument bson.Raw
	if err := r.database.Collection("users").FindOne(ctx, bson.M{"_id": masterID}).Decode(&masterDocument); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return false, nil
		}
		return false, err
	}
	master, err := parseOrganisationsBootstrapUser(masterDocument)
	if err != nil || master.ParentState != organisationsBootstrapFieldEmpty {
		return false, nil
	}
	var organisation bson.Raw
	if err := r.database.Collection("organisation").FindOne(ctx, bson.M{"_id": masterID}).Decode(&organisation); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return false, nil
		}
		return false, err
	}
	ownerID, state := organisationsBootstrapObjectID(organisation, "ownerId")
	if state != organisationsBootstrapFieldValue || ownerID != masterID {
		return false, nil
	}
	if len(organisationsBootstrapMissingOrganisationFields(organisation, master, r.now)) != 0 || !organisationsBootstrapOrganisationFieldsValid(organisation, master.Timezone != "") {
		return false, nil
	}
	legacyCandidates, legacyInvalid, err := r.legacyOrganisationCandidates(ctx, masterID, false)
	if err != nil || legacyInvalid || len(legacyCandidates) != 0 {
		return false, err
	}
	if master.SelectionState != organisationsBootstrapFieldValue {
		return false, nil
	}
	accessible, err := r.organisationAccessibleToMaster(ctx, master.Selection, masterID)
	if err != nil || !accessible {
		return false, err
	}
	canonicalMembership, err := organisationsBootstrapMembershipActive(ctx, r.database.Collection("organisation_users"), bson.M{"userId": masterID, "organisationId": masterID}, r.now)
	if err != nil || !canonicalMembership {
		return false, err
	}
	ownedOrganisations, err := r.database.Collection("organisation").Find(ctx, bson.M{"ownerId": masterID}, options.Find().SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return false, err
	}
	defer ownedOrganisations.Close(ctx)
	for ownedOrganisations.Next(ctx) {
		var owned struct {
			ID primitive.ObjectID `bson:"_id"`
		}
		if err := ownedOrganisations.Decode(&owned); err != nil {
			return false, err
		}
		active, err := organisationsBootstrapMembershipActive(ctx, r.database.Collection("organisation_users"), bson.M{"userId": masterID, "organisationId": owned.ID}, r.now)
		if err != nil || !active {
			return false, err
		}
	}
	if err := ownedOrganisations.Err(); err != nil {
		return false, err
	}
	selectedMembership, err := organisationsBootstrapMembershipActive(ctx, r.database.Collection("organisation_users"), bson.M{"userId": masterID, "organisationId": master.Selection}, r.now)
	return selectedMembership, err
}

func (r *organisationsBootstrapRunner) processSubUser(ctx context.Context, user organisationsBootstrapUser) error {
	masterID := user.ParentID
	targetSet := map[primitive.ObjectID]struct{}{masterID: {}}
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
	if user.SelectionState == organisationsBootstrapFieldWrong {
		r.report.SubUsers.Conflicted++
		r.addConflict("invalid-user-selection", masterID.Hex(), user.ID.Hex(), "organisation_id must be a BSON ObjectID")
		r.report.Verification.Failed++
		return nil
	}
	if user.SelectionState == organisationsBootstrapFieldValue {
		accessible, err := r.organisationAccessibleToUser(ctx, user.Selection, user.ID, masterID)
		if err != nil {
			return err
		}
		if !accessible {
			r.report.SubUsers.Conflicted++
			r.report.Users.MissingSelectedOrganisation++
			r.addConflict("invalid-user-selection", masterID.Hex(), user.ID.Hex(), "preserved organisation_id is missing or not owned by the master")
			r.report.Verification.Failed++
			return nil
		}
		targetSet[user.Selection] = struct{}{}
		r.report.SubUsers.AlreadySelected++
		r.report.Users.SelectionsPreserved++
	} else {
		r.report.SubUsers.Planned++
	}
	targets := make([]primitive.ObjectID, 0, len(targetSet))
	for organisationID := range targetSet {
		targets = append(targets, organisationID)
	}
	sort.Slice(targets, func(left, right int) bool { return targets[left].Hex() < targets[right].Hex() })
	for _, organisationID := range targets {
		ok, err := r.ensureMembership(ctx, user.ID, organisationID, masterID)
		if err != nil {
			return err
		}
		if !ok {
			r.report.SubUsers.Conflicted++
			r.report.Verification.Failed++
			return nil
		}
	}
	selectionOK, updated, err := r.ensureUserSelection(ctx, user, masterID, masterID)
	if err != nil {
		return err
	}
	if !selectionOK {
		r.report.SubUsers.Conflicted++
		r.report.Verification.Failed++
		return nil
	}
	if updated {
		r.report.SubUsers.Updated++
	}
	r.report.Verification.Passed++
	return nil
}
