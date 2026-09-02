package actions

import (
	"context"
	"sort"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	organisationsBackfillNotificationMailbox = "mailbox"
	organisationsBackfillNotificationFlat    = "flat-event"
	organisationsBackfillNotificationUnknown = "unknown"
)

type organisationsBackfillNotificationShapes struct {
	Total                        int64 `json:"total"`
	Mailbox                      int64 `json:"mailbox"`
	FlatEvents                   int64 `json:"flatEvents"`
	Unknown                      int64 `json:"unknown"`
	MailboxExcluded              bool  `json:"mailboxExcludedFromOwnershipBackfill"`
	ScopeAppliedToFlatEventsOnly bool  `json:"scopeAppliedToFlatEventsOnly,omitempty"`
}

func organisationsBackfillNotificationShape(document bson.Raw) string {
	if document.Lookup("data").Type == bsontype.Array {
		return organisationsBackfillNotificationMailbox
	}
	for _, field := range []string{"organisationId", "projectId", "alert_master_user", "userid", "alert_user", "media_key", "notification_type", "device_id", "timestamp"} {
		if document.Lookup(field).Type != 0 {
			return organisationsBackfillNotificationFlat
		}
	}
	return organisationsBackfillNotificationUnknown
}

func inspectOrganisationsBackfillNotifications(
	ctx context.Context,
	database *mongo.Database,
	adapter organisationsBackfillAdapter,
	config OrganisationsBackfillConfig,
	report organisationsBackfillCollection,
) (organisationsBackfillCollection, error) {
	documents, err := findOrganisationsBackfillDocuments(ctx, database.Collection(adapter.Collection), config)
	if err != nil {
		return report, err
	}
	shapes := organisationsBackfillNotificationShapes{
		MailboxExcluded:              true,
		ScopeAppliedToFlatEventsOnly: config.OrganisationID != "",
	}
	flatDocuments := make([]bson.Raw, 0, len(documents))
	unknownDocuments := make([]bson.Raw, 0)
	for _, document := range documents {
		shapes.Total++
		switch organisationsBackfillNotificationShape(document) {
		case organisationsBackfillNotificationMailbox:
			shapes.Mailbox++
		case organisationsBackfillNotificationFlat:
			shapes.FlatEvents++
			flatDocuments = append(flatDocuments, document)
		default:
			shapes.Unknown++
			unknownDocuments = append(unknownDocuments, document)
		}
	}
	report.NotificationShapes = &shapes
	resetOrganisationsBackfillScopedInventory(&report)

	var scopeID primitive.ObjectID
	if config.OrganisationID != "" {
		scopeID, _ = primitive.ObjectIDFromHex(config.OrganisationID)
	}
	legacyUserIDs := make(map[primitive.ObjectID]struct{})
	organisationIDs := make(map[primitive.ObjectID]struct{})
	projectIDs := make(map[primitive.ObjectID]struct{})
	for _, document := range flatDocuments {
		if projectID, state := organisationsBootstrapObjectID(document, "projectId"); state == organisationsBootstrapFieldValue {
			projectIDs[projectID] = struct{}{}
		}
		canonicalID, canonicalState, _ := organisationsBackfillAlertCanonicalOrganisation(document)
		if canonicalState == organisationsBootstrapFieldValue {
			organisationIDs[canonicalID] = struct{}{}
			continue
		}
		if canonicalState == organisationsBootstrapFieldWrong {
			continue
		}
		masterID, masterState := organisationsBackfillStringObjectIDField(document, "alert_master_user")
		if masterState == organisationsBootstrapFieldValue {
			organisationIDs[masterID] = struct{}{}
			continue
		}
		if masterState == organisationsBootstrapFieldWrong {
			continue
		}
		if userID, userState := organisationsBackfillStringObjectIDField(document, "userid"); userState == organisationsBootstrapFieldValue {
			legacyUserIDs[userID] = struct{}{}
		}
	}
	users, err := findOrganisationsBackfillUsers(ctx, database.Collection("users"), legacyUserIDs)
	if err != nil {
		return report, err
	}
	for _, user := range users {
		resolved := resolveOrganisationsBackfillUser(user)
		if resolved.code == "" {
			organisationIDs[resolved.organisationID] = struct{}{}
		}
	}
	if !scopeID.IsZero() {
		organisationIDs[scopeID] = struct{}{}
	}
	organisations, err := findOrganisationsBackfillOrganisations(ctx, database.Collection("organisation"), organisationIDs)
	if err != nil {
		return report, err
	}
	projects, err := findOrganisationsBackfillProjects(ctx, database.Collection("project"), projectIDs)
	if err != nil {
		return report, err
	}

	resolution := organisationsBackfillResolution{
		ObservedFieldTypes: make(map[string]map[string]int64),
		ObservedShapes:     make(map[string]int64),
	}
	if !scopeID.IsZero() && !organisations[scopeID] {
		resolution.OrphanOrganisations++
		resolution.Conflicts++
		resolution.ConflictEntries = append(resolution.ConflictEntries, organisationsBackfillConflict{
			Code:                  "scope-organisation-not-found",
			CanonicalOrganisation: scopeID.Hex(),
			ResolvedOrganisations: []string{scopeID.Hex()},
			Message:               "requested organisation does not exist",
		})
	}
	for _, document := range flatDocuments {
		outcome := resolveOrganisationsBackfillNotification(document, users, organisations, projects)
		if !organisationsBackfillAlertInScope(outcome, scopeID) {
			continue
		}
		observeOrganisationsBackfillDocument(&resolution, document)
		addOrganisationsBackfillAlertOutcome(&resolution, outcome)
		addOrganisationsBackfillNotificationInventory(&report, outcome)
	}
	if scopeID.IsZero() {
		for _, document := range unknownDocuments {
			resolution.Conflicts++
			resolution.ConflictEntries = append(resolution.ConflictEntries, organisationsBackfillConflict{
				Code:       "unknown-notification-shape",
				DocumentID: organisationsBackfillDocumentID(document),
				Message:    "document is neither a personal mailbox nor a flat notification event",
			})
		}
	}
	sortOrganisationsBackfillNotificationConflicts(resolution.ConflictEntries)
	if len(resolution.ConflictEntries) > organisationsBackfillConflictLimit {
		resolution.ConflictEntries = resolution.ConflictEntries[:organisationsBackfillConflictLimit]
	}
	report.Resolution = &resolution

	contracts, err := inspectOrganisationsBackfillNotificationIndexes(ctx, database.Collection(adapter.Collection))
	if err != nil {
		return report, err
	}
	report.IndexContracts = contracts
	return report, nil
}

func resolveOrganisationsBackfillNotification(
	document bson.Raw,
	users map[primitive.ObjectID]bson.Raw,
	organisations map[primitive.ObjectID]bool,
	projects map[primitive.ObjectID]primitive.ObjectID,
) (outcome organisationsBackfillAlertOutcome) {
	outcome.documentID = organisationsBackfillDocumentID(document)
	defer outcome.enrichConflicts()
	defer outcome.resolveProject(projects)

	masterID, masterState := organisationsBackfillStringObjectIDField(document, "alert_master_user")
	if masterState != organisationsBootstrapFieldEmpty {
		outcome.legacyMasterPresent = true
	}
	if masterState == organisationsBootstrapFieldValue {
		outcome.legacyMasterID = masterID
	}
	userID, userState := organisationsBackfillStringObjectIDField(document, "userid")
	if userState != organisationsBootstrapFieldEmpty {
		outcome.legacyUserPresent = true
	}
	if userState == organisationsBootstrapFieldValue {
		outcome.legacyUserID = userID
	}
	projectID, projectState := organisationsBootstrapObjectID(document, "projectId")
	switch projectState {
	case organisationsBootstrapFieldValue:
		outcome.projectPresent = true
		outcome.resolvedProjectID = projectID
	case organisationsBootstrapFieldEmpty:
		outcome.projectMissing = true
	default:
		outcome.projectWrong = true
		outcome.addConflict("invalid-project-id", "projectId must be a non-zero BSON ObjectID or null")
	}

	canonicalID, canonicalState, canonicalWrongType := organisationsBackfillAlertCanonicalOrganisation(document)
	switch canonicalState {
	case organisationsBootstrapFieldValue:
		outcome.canonicalID = canonicalID
		outcome.canonicalValid = !canonicalWrongType
		outcome.canonicalWrong = canonicalWrongType
		if canonicalWrongType {
			outcome.addConflict("invalid-canonical-organisation", "organisationId must contain an ObjectID hex string")
		}
		if !organisations[canonicalID] {
			outcome.orphanOrganisation = true
			outcome.addConflict("orphan-organisation", "canonical organisation does not exist")
		}
		return outcome
	case organisationsBootstrapFieldWrong:
		outcome.canonicalWrong = true
		outcome.addConflict("invalid-canonical-organisation", "organisationId must contain an ObjectID hex string")
		return outcome
	default:
		outcome.canonicalMissing = true
	}

	switch masterState {
	case organisationsBootstrapFieldValue:
		outcome.resolvedID = masterID
		if !organisations[masterID] {
			outcome.orphanOrganisation = true
			outcome.addConflict("orphan-organisation", "alert_master_user organisation does not exist")
			return outcome
		}
		outcome.resolved = true
		outcome.proposedWrite = true
		return outcome
	case organisationsBootstrapFieldWrong:
		outcome.invalidLegacy = true
		outcome.addConflict("invalid-legacy-master-id", "alert_master_user must contain an ObjectID hex string")
		return outcome
	}

	switch userState {
	case organisationsBootstrapFieldEmpty:
		outcome.zeroCandidate = true
		outcome.addConflict("zero-candidate", "flat notification has no canonical organisationId, alert_master_user, or userid")
		return outcome
	case organisationsBootstrapFieldWrong:
		outcome.invalidLegacy = true
		outcome.addConflict("invalid-legacy-user-id", "userid must contain an ObjectID hex string")
		return outcome
	}
	user, exists := users[userID]
	if !exists {
		outcome.orphanUser = true
		outcome.addConflict("orphan-user", "recipient userid does not resolve to a persisted user")
		return outcome
	}
	userResolution := resolveOrganisationsBackfillUser(user)
	if userResolution.code != "" {
		outcome.addConflict(userResolution.code, userResolution.message)
		return outcome
	}
	if !organisations[userResolution.organisationID] {
		outcome.orphanOrganisation = true
		outcome.addConflict("orphan-organisation", "organisation resolved from recipient userid does not exist")
		return outcome
	}
	outcome.resolvedID = userResolution.organisationID
	outcome.resolved = true
	outcome.proposedWrite = true
	return outcome
}

func addOrganisationsBackfillNotificationInventory(report *organisationsBackfillCollection, outcome organisationsBackfillAlertOutcome) {
	report.Total++
	if outcome.canonicalValid {
		report.CanonicalPresent++
	}
	if outcome.canonicalMissing {
		report.CanonicalMissing++
	}
	if outcome.canonicalWrong {
		report.CanonicalWrongType++
	}
	if outcome.projectPresent {
		report.ProjectPresent++
	}
	if outcome.projectMissing {
		report.ProjectMissing++
	}
	if outcome.projectWrong {
		report.ProjectWrongType++
	}
	if outcome.legacyMasterPresent {
		report.LegacyCandidateCount["alert_master_user"]++
	}
	if outcome.legacyUserPresent {
		report.LegacyCandidateCount["userid"]++
	}
}

func inspectOrganisationsBackfillNotificationIndexes(ctx context.Context, collection *mongo.Collection) ([]organisationsBackfillIndexContract, error) {
	cursor, err := collection.Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var indexes []struct {
		Name string `bson:"name"`
		Key  bson.D `bson:"key"`
	}
	if err := cursor.All(ctx, &indexes); err != nil {
		return nil, err
	}
	contracts := []organisationsBackfillIndexContract{
		organisationsBackfillNewIndexContract("mailbox-owner", bson.D{{Key: "user_id", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("canonical-project-timeline", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "timestamp", Value: int32(-1)}}),
		organisationsBackfillNewIndexContract("legacy-master-project-timeline", bson.D{{Key: "alert_master_user", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "timestamp", Value: int32(-1)}}),
		organisationsBackfillNewIndexContract("legacy-recipient-project-timeline", bson.D{{Key: "userid", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "timestamp", Value: int32(-1)}}),
		organisationsBackfillNewIndexContract("canonical-project-recipient-media", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "userid", Value: int32(1)}, {Key: "media_key", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("legacy-master-project-recipient-media", bson.D{{Key: "alert_master_user", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "userid", Value: int32(1)}, {Key: "media_key", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("global-retention", bson.D{{Key: "timestamp", Value: int32(1)}}),
	}
	for contractIndex := range contracts {
		contracts[contractIndex].Status = "missing"
		for _, candidate := range indexes {
			status := organisationsBackfillOrderedIndexCoverage(candidate.Key, contracts[contractIndex].Keys)
			if status == "exact" || (status == "prefix" && contracts[contractIndex].Status == "missing") {
				contracts[contractIndex].Status = status
				contracts[contractIndex].IndexName = candidate.Name
			}
			if status == "exact" {
				break
			}
		}
	}
	return contracts, nil
}

func sortOrganisationsBackfillNotificationConflicts(conflicts []organisationsBackfillConflict) {
	sort.Slice(conflicts, func(left, right int) bool {
		if conflicts[left].DocumentID != conflicts[right].DocumentID {
			return conflicts[left].DocumentID < conflicts[right].DocumentID
		}
		return conflicts[left].Code < conflicts[right].Code
	})
}
