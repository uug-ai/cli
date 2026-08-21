package actions

import (
	"context"
	"sort"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type organisationsBackfillAlertOutcome struct {
	documentID          string
	canonicalID         primitive.ObjectID
	canonicalValid      bool
	canonicalMissing    bool
	canonicalWrong      bool
	legacyMasterID      primitive.ObjectID
	legacyMasterPresent bool
	legacyUserID        primitive.ObjectID
	legacyUserPresent   bool
	invalidLegacy       bool
	resolvedID          primitive.ObjectID
	resolved            bool
	zeroCandidate       bool
	multipleCandidates  bool
	orphanUser          bool
	orphanOrganisation  bool
	proposedWrite       bool
	conflicts           []organisationsBackfillConflict
}

func inspectOrganisationsBackfillAlerts(
	ctx context.Context,
	database *mongo.Database,
	adapter organisationsBackfillAdapter,
	config OrganisationsBackfillConfig,
	report organisationsBackfillCollection,
) (organisationsBackfillCollection, error) {
	var scopeID primitive.ObjectID
	if config.OrganisationID != "" {
		scopeID, _ = primitive.ObjectIDFromHex(config.OrganisationID)
	}
	documents, err := findOrganisationsBackfillDocuments(ctx, database.Collection(adapter.Collection), config)
	if err != nil {
		return report, err
	}

	legacyUserIDs := make(map[primitive.ObjectID]struct{})
	organisationIDs := make(map[primitive.ObjectID]struct{})
	for _, document := range documents {
		canonicalID, canonicalState, _ := organisationsBackfillAlertCanonicalOrganisation(document)
		if canonicalState == organisationsBootstrapFieldValue {
			organisationIDs[canonicalID] = struct{}{}
			continue
		}
		if canonicalState == organisationsBootstrapFieldWrong {
			continue
		}
		masterID, masterState := organisationsBackfillStringObjectIDField(document, "master_user_id")
		if masterState == organisationsBootstrapFieldValue {
			organisationIDs[masterID] = struct{}{}
			continue
		}
		if masterState == organisationsBootstrapFieldWrong {
			continue
		}
		userID, userState := organisationsBackfillStringObjectIDField(document, "user_id")
		if userState == organisationsBootstrapFieldValue {
			legacyUserIDs[userID] = struct{}{}
			organisationIDs[userID] = struct{}{}
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

	resolution := organisationsBackfillResolution{
		ObservedFieldTypes: make(map[string]map[string]int64),
		ObservedShapes:     make(map[string]int64),
	}
	if config.OrganisationID != "" {
		resetOrganisationsBackfillScopedInventory(&report)
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
	for _, document := range documents {
		outcome := resolveOrganisationsBackfillAlert(document, users, organisations)
		if !organisationsBackfillAlertInScope(outcome, scopeID) {
			continue
		}
		observeOrganisationsBackfillDocument(&resolution, document)
		addOrganisationsBackfillAlertOutcome(&resolution, outcome)
		if config.OrganisationID != "" {
			addOrganisationsBackfillAlertScopedInventory(&report, outcome)
		}
	}
	sort.Slice(resolution.ConflictEntries, func(left, right int) bool {
		first := resolution.ConflictEntries[left]
		second := resolution.ConflictEntries[right]
		if first.DocumentID != second.DocumentID {
			return first.DocumentID < second.DocumentID
		}
		return first.Code < second.Code
	})
	if len(resolution.ConflictEntries) > organisationsBackfillConflictLimit {
		resolution.ConflictEntries = resolution.ConflictEntries[:organisationsBackfillConflictLimit]
	}
	report.Resolution = &resolution

	contracts, err := inspectOrganisationsBackfillAlertIndexes(ctx, database.Collection(adapter.Collection))
	if err != nil {
		return report, err
	}
	report.IndexContracts = contracts
	return report, nil
}

func resolveOrganisationsBackfillAlert(
	document bson.Raw,
	users map[primitive.ObjectID]bson.Raw,
	organisations map[primitive.ObjectID]bool,
) (outcome organisationsBackfillAlertOutcome) {
	outcome.documentID = organisationsBackfillDocumentID(document)
	defer outcome.enrichConflicts()
	masterID, masterState := organisationsBackfillStringObjectIDField(document, "master_user_id")
	if masterState != organisationsBootstrapFieldEmpty {
		outcome.legacyMasterPresent = true
	}
	if masterState == organisationsBootstrapFieldValue {
		outcome.legacyMasterID = masterID
	}
	userID, userState := organisationsBackfillStringObjectIDField(document, "user_id")
	if userState != organisationsBootstrapFieldEmpty {
		outcome.legacyUserPresent = true
	}
	if userState == organisationsBootstrapFieldValue {
		outcome.legacyUserID = userID
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
			outcome.addConflict("orphan-organisation", "master_user_id organisation does not exist")
			return outcome
		}
		outcome.resolved = true
		outcome.proposedWrite = true
		return outcome
	case organisationsBootstrapFieldWrong:
		outcome.invalidLegacy = true
		outcome.addConflict("invalid-legacy-master-id", "master_user_id must contain an ObjectID hex string")
		return outcome
	}

	switch userState {
	case organisationsBootstrapFieldEmpty:
		outcome.zeroCandidate = true
		outcome.addConflict("zero-candidate", "alert has no canonical organisationId, master_user_id, or user_id")
		return outcome
	case organisationsBootstrapFieldWrong:
		outcome.invalidLegacy = true
		outcome.addConflict("invalid-legacy-user-id", "user_id must contain an ObjectID hex string")
		return outcome
	}
	candidates := make(map[primitive.ObjectID]struct{})
	if organisations[userID] {
		candidates[userID] = struct{}{}
	}
	user, userExists := users[userID]
	if userExists {
		userResolution := resolveOrganisationsBackfillUser(user)
		if userResolution.code != "" {
			outcome.addConflict(userResolution.code, userResolution.message)
			return outcome
		}
		if !organisations[userResolution.organisationID] {
			outcome.orphanOrganisation = true
			outcome.addConflict("orphan-organisation", "organisation resolved from legacy creator does not exist")
			return outcome
		}
		candidates[userResolution.organisationID] = struct{}{}
	} else if len(candidates) == 0 {
		outcome.orphanUser = true
		outcome.addConflict("orphan-user", "legacy user_id resolves to neither an organisation nor a user")
		return outcome
	}

	resolved := sortedOrganisationsBackfillObjectIDs(candidates)
	if len(resolved) > 1 {
		outcome.multipleCandidates = true
		outcome.addConflict("multiple-candidates", "legacy user_id resolves to multiple organisations")
		for _, id := range resolved {
			outcome.conflicts[len(outcome.conflicts)-1].ResolvedOrganisations = append(outcome.conflicts[len(outcome.conflicts)-1].ResolvedOrganisations, id.Hex())
		}
		return outcome
	}
	outcome.resolvedID = resolved[0]
	outcome.resolved = true
	outcome.proposedWrite = true
	return outcome
}

func organisationsBackfillAlertCanonicalOrganisation(document bson.Raw) (primitive.ObjectID, organisationsBootstrapFieldState, bool) {
	value := document.Lookup("organisationId")
	if value.Type == bsontype.ObjectID {
		id := value.ObjectID()
		if id.IsZero() {
			return primitive.NilObjectID, organisationsBootstrapFieldWrong, true
		}
		return id, organisationsBootstrapFieldValue, true
	}
	id, state := organisationsBackfillStringObjectIDField(document, "organisationId")
	return id, state, false
}

func (outcome *organisationsBackfillAlertOutcome) addConflict(code, message string) {
	outcome.conflicts = append(outcome.conflicts, organisationsBackfillConflict{
		Code:       code,
		DocumentID: outcome.documentID,
		Message:    message,
	})
}

func (outcome *organisationsBackfillAlertOutcome) enrichConflicts() {
	resolved := make(map[string]struct{})
	if !outcome.canonicalID.IsZero() {
		resolved[outcome.canonicalID.Hex()] = struct{}{}
	}
	if !outcome.resolvedID.IsZero() {
		resolved[outcome.resolvedID.Hex()] = struct{}{}
	}
	resolvedOrganisations := make([]string, 0, len(resolved))
	for id := range resolved {
		resolvedOrganisations = append(resolvedOrganisations, id)
	}
	sort.Strings(resolvedOrganisations)
	for index := range outcome.conflicts {
		if !outcome.canonicalID.IsZero() {
			outcome.conflicts[index].CanonicalOrganisation = outcome.canonicalID.Hex()
		}
		if !outcome.legacyMasterID.IsZero() {
			outcome.conflicts[index].LegacyMaster = outcome.legacyMasterID.Hex()
		}
		if !outcome.legacyUserID.IsZero() {
			outcome.conflicts[index].LegacyUser = outcome.legacyUserID.Hex()
		}
		if len(outcome.conflicts[index].ResolvedOrganisations) == 0 {
			outcome.conflicts[index].ResolvedOrganisations = append([]string(nil), resolvedOrganisations...)
		}
	}
}

func organisationsBackfillAlertInScope(outcome organisationsBackfillAlertOutcome, scopeID primitive.ObjectID) bool {
	if scopeID.IsZero() {
		return true
	}
	if !outcome.canonicalID.IsZero() {
		return outcome.canonicalID == scopeID
	}
	return outcome.resolvedID == scopeID
}

func addOrganisationsBackfillAlertOutcome(report *organisationsBackfillResolution, outcome organisationsBackfillAlertOutcome) {
	report.Scanned++
	if outcome.canonicalValid {
		report.CanonicalValid++
	}
	if outcome.canonicalMissing {
		report.CanonicalMissing++
	}
	if outcome.resolved {
		report.Resolved++
	}
	if outcome.zeroCandidate {
		report.ZeroCandidate++
	}
	if outcome.multipleCandidates {
		report.MultipleCandidates++
	}
	if outcome.invalidLegacy {
		report.InvalidLegacy++
	}
	if outcome.orphanUser {
		report.OrphanUsers++
	}
	if outcome.orphanOrganisation {
		report.OrphanOrganisations++
	}
	if outcome.proposedWrite {
		report.ProposedWrites++
	}
	report.Conflicts += int64(len(outcome.conflicts))
	report.ConflictEntries = append(report.ConflictEntries, outcome.conflicts...)
}

func addOrganisationsBackfillAlertScopedInventory(report *organisationsBackfillCollection, outcome organisationsBackfillAlertOutcome) {
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
	if outcome.legacyMasterPresent {
		report.LegacyCandidateCount["master_user_id"]++
	}
	if outcome.legacyUserPresent {
		report.LegacyCandidateCount["user_id"]++
	}
}

func inspectOrganisationsBackfillAlertIndexes(ctx context.Context, collection *mongo.Collection) ([]organisationsBackfillIndexContract, error) {
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
		organisationsBackfillNewIndexContract("canonical-enabled-lookup", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "enabled", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("legacy-master-rollback", bson.D{{Key: "master_user_id", Value: int32(1)}, {Key: "enabled", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("legacy-creator-rollback", bson.D{{Key: "user_id", Value: int32(1)}, {Key: "enabled", Value: int32(1)}}),
	}
	for index := range contracts {
		contracts[index].Status = "missing"
		for _, candidate := range indexes {
			status := organisationsBackfillOrderedIndexCoverage(candidate.Key, contracts[index].Keys)
			if status == "exact" || (status == "prefix" && contracts[index].Status == "missing") {
				contracts[index].Status = status
				contracts[index].IndexName = candidate.Name
			}
			if status == "exact" {
				break
			}
		}
	}
	return contracts, nil
}
