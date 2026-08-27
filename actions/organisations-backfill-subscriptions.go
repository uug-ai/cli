package actions

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const organisationsBackfillConflictLimit = 100

type organisationsBackfillResolution struct {
	Scanned               int64                           `json:"scanned"`
	CanonicalValid        int64                           `json:"canonicalValid"`
	CanonicalMissing      int64                           `json:"canonicalMissing"`
	Resolved              int64                           `json:"resolved"`
	ZeroCandidate         int64                           `json:"zeroCandidate"`
	MultipleCandidates    int64                           `json:"multipleCandidates"`
	Conflicts             int64                           `json:"conflicts"`
	InvalidLegacy         int64                           `json:"invalidLegacy"`
	OrphanUsers           int64                           `json:"orphanUsers"`
	OrphanOrganisations   int64                           `json:"orphanOrganisations"`
	ProposedWrites        int64                           `json:"proposedWrites"`
	ProjectResolved       int64                           `json:"projectResolved,omitempty"`
	ProposedProjectWrites int64                           `json:"proposedProjectWrites,omitempty"`
	ObservedFieldTypes    map[string]map[string]int64     `json:"observedFieldTypes"`
	ObservedShapes        map[string]int64                `json:"observedShapes"`
	ConflictEntries       []organisationsBackfillConflict `json:"conflictEntries"`
}

type organisationsBackfillConflict struct {
	Code                  string   `json:"code"`
	DocumentID            string   `json:"documentId"`
	CanonicalOrganisation string   `json:"canonicalOrganisation,omitempty"`
	LegacyMaster          string   `json:"legacyMaster,omitempty"`
	LegacyUser            string   `json:"legacyUser,omitempty"`
	ResolvedOrganisations []string `json:"resolvedOrganisations,omitempty"`
	Message               string   `json:"message"`
}

type organisationsBackfillIndexContract struct {
	Name                    string                          `json:"name"`
	Keys                    []organisationsBackfillIndexKey `json:"keys"`
	Unique                  bool                            `json:"unique,omitempty"`
	PartialFilterExpression bson.M                          `json:"partialFilterExpression,omitempty"`
	Status                  string                          `json:"status"`
	IndexName               string                          `json:"indexName,omitempty"`
}

type organisationsBackfillIndexKey struct {
	Field     string `json:"field"`
	Direction int32  `json:"direction"`
}

type organisationsBackfillSubscriptionOutcome struct {
	documentID         string
	canonicalID        primitive.ObjectID
	canonicalValid     bool
	canonicalMissing   bool
	canonicalWrong     bool
	legacyUserID       primitive.ObjectID
	legacyPresent      bool
	invalidLegacy      bool
	resolvedID         primitive.ObjectID
	resolved           bool
	zeroCandidate      bool
	orphanUser         bool
	orphanOrganisation bool
	proposedWrite      bool
	conflicts          []organisationsBackfillConflict
}

type organisationsBackfillUserResolution struct {
	organisationID primitive.ObjectID
	code           string
	message        string
}

func inspectOrganisationsBackfillSubscriptions(
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
		if id, state := organisationsBackfillSubscriptionCanonicalOrganisation(document); state == organisationsBootstrapFieldValue {
			organisationIDs[id] = struct{}{}
		}
		if id, state := organisationsBackfillSubscriptionLegacyUser(document); state == organisationsBootstrapFieldValue {
			legacyUserIDs[id] = struct{}{}
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
		outcome := resolveOrganisationsBackfillSubscription(document, users, organisations)
		if !organisationsBackfillSubscriptionInScope(outcome, scopeID) {
			continue
		}
		observeOrganisationsBackfillDocument(&resolution, document)
		addOrganisationsBackfillSubscriptionOutcome(&resolution, outcome)
		if config.OrganisationID != "" {
			addOrganisationsBackfillScopedInventory(&report, outcome)
		}
	}
	sort.Slice(resolution.ConflictEntries, func(left, right int) bool {
		first := resolution.ConflictEntries[left]
		second := resolution.ConflictEntries[right]
		if first.DocumentID != second.DocumentID {
			return first.DocumentID < second.DocumentID
		}
		if first.Code != second.Code {
			return first.Code < second.Code
		}
		return first.Message < second.Message
	})
	if len(resolution.ConflictEntries) > organisationsBackfillConflictLimit {
		resolution.ConflictEntries = resolution.ConflictEntries[:organisationsBackfillConflictLimit]
	}
	report.Resolution = &resolution

	contracts, err := inspectOrganisationsBackfillSubscriptionIndexes(ctx, database.Collection(adapter.Collection))
	if err != nil {
		return report, err
	}
	report.IndexContracts = contracts
	return report, nil
}

func observeOrganisationsBackfillDocument(report *organisationsBackfillResolution, document bson.Raw) {
	elements, err := document.Elements()
	if err != nil {
		return
	}
	shape := make([]string, 0, len(elements))
	for _, element := range elements {
		field := element.Key()
		typeName := element.Value().Type.String()
		if report.ObservedFieldTypes[field] == nil {
			report.ObservedFieldTypes[field] = make(map[string]int64)
		}
		report.ObservedFieldTypes[field][typeName]++
		shape = append(shape, field+":"+typeName)
	}
	sort.Strings(shape)
	report.ObservedShapes[strings.Join(shape, ",")]++
}

func findOrganisationsBackfillDocuments(
	ctx context.Context,
	collection *mongo.Collection,
	config OrganisationsBackfillConfig,
) ([]bson.Raw, error) {
	filter := bson.M{}
	if config.DocumentID != "" {
		id, _ := primitive.ObjectIDFromHex(config.DocumentID)
		filter["_id"] = id
	}
	cursor, err := collection.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetBatchSize(int32(config.BatchSize)))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	documents := []bson.Raw{}
	for cursor.Next(ctx) {
		var document bson.Raw
		if err := cursor.Decode(&document); err != nil {
			return nil, err
		}
		documents = append(documents, document)
	}
	return documents, cursor.Err()
}

func findOrganisationsBackfillUsers(
	ctx context.Context,
	collection *mongo.Collection,
	ids map[primitive.ObjectID]struct{},
) (map[primitive.ObjectID]bson.Raw, error) {
	users := make(map[primitive.ObjectID]bson.Raw, len(ids))
	if len(ids) == 0 {
		return users, nil
	}
	cursor, err := collection.Find(ctx, bson.M{"_id": bson.M{"$in": sortedOrganisationsBackfillObjectIDs(ids)}}, options.Find().SetProjection(bson.M{
		"_id": 1, "organisationId": 1, "user_id": 1,
	}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var document bson.Raw
		if err := cursor.Decode(&document); err != nil {
			return nil, err
		}
		id, state := organisationsBootstrapObjectID(document, "_id")
		if state == organisationsBootstrapFieldValue {
			users[id] = document
		}
	}
	return users, cursor.Err()
}

func findOrganisationsBackfillOrganisations(
	ctx context.Context,
	collection *mongo.Collection,
	ids map[primitive.ObjectID]struct{},
) (map[primitive.ObjectID]bool, error) {
	organisations := make(map[primitive.ObjectID]bool, len(ids))
	if len(ids) == 0 {
		return organisations, nil
	}
	cursor, err := collection.Find(ctx, bson.M{"_id": bson.M{"$in": sortedOrganisationsBackfillObjectIDs(ids)}}, options.Find().SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var document bson.Raw
		if err := cursor.Decode(&document); err != nil {
			return nil, err
		}
		id, state := organisationsBootstrapObjectID(document, "_id")
		if state == organisationsBootstrapFieldValue {
			organisations[id] = true
		}
	}
	return organisations, cursor.Err()
}

func sortedOrganisationsBackfillObjectIDs(ids map[primitive.ObjectID]struct{}) []primitive.ObjectID {
	result := make([]primitive.ObjectID, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Hex() < result[right].Hex() })
	return result
}

func resolveOrganisationsBackfillSubscription(
	document bson.Raw,
	users map[primitive.ObjectID]bson.Raw,
	organisations map[primitive.ObjectID]bool,
) (outcome organisationsBackfillSubscriptionOutcome) {
	outcome.documentID = organisationsBackfillDocumentID(document)
	defer outcome.enrichConflicts()
	legacyUserID, legacyState := organisationsBackfillSubscriptionLegacyUser(document)
	if legacyState != organisationsBootstrapFieldEmpty {
		outcome.legacyPresent = true
	}
	if legacyState == organisationsBootstrapFieldValue {
		outcome.legacyUserID = legacyUserID
	} else if legacyState == organisationsBootstrapFieldWrong {
		outcome.invalidLegacy = true
		outcome.addConflict("invalid-legacy-user-id", "user_id must contain an ObjectID hex string")
	}

	var canonicalState organisationsBootstrapFieldState
	outcome.canonicalID, canonicalState = organisationsBackfillSubscriptionCanonicalOrganisation(document)
	switch canonicalState {
	case organisationsBootstrapFieldValue:
		outcome.canonicalValid = true
		if !organisations[outcome.canonicalID] {
			outcome.orphanOrganisation = true
			outcome.addConflict("orphan-organisation", "canonical organisation does not exist")
		}
		// Once canonical ownership exists, user_id is payer/creator provenance.
		// It may legitimately identify a user whose primary organisation differs
		// from this subscription's organisation, so it must not be reinterpreted
		// as a competing tenant candidate.
		return outcome
	case organisationsBootstrapFieldEmpty:
		outcome.canonicalMissing = true
	default:
		outcome.canonicalWrong = true
		outcome.addConflict("invalid-canonical-organisation", "organisation_id must be a non-zero BSON ObjectID or null")
	}

	if legacyState == organisationsBootstrapFieldEmpty {
		if outcome.canonicalMissing {
			outcome.zeroCandidate = true
			outcome.addConflict("zero-candidate", "subscription has neither canonical organisation_id nor legacy user_id")
		}
		return outcome
	}
	if legacyState == organisationsBootstrapFieldWrong {
		return outcome
	}
	user, exists := users[legacyUserID]
	if !exists {
		outcome.orphanUser = true
		outcome.addConflict("orphan-user", "legacy user_id does not resolve to a user")
		return outcome
	}
	userResolution := resolveOrganisationsBackfillUser(user)
	if userResolution.code != "" {
		outcome.addConflict(userResolution.code, userResolution.message)
		return outcome
	}
	outcome.resolvedID = userResolution.organisationID
	if !organisations[outcome.resolvedID] {
		outcome.orphanOrganisation = true
		outcome.addConflict("orphan-organisation", "organisation resolved from legacy user does not exist")
		return outcome
	}
	outcome.resolved = true
	if outcome.canonicalMissing {
		outcome.proposedWrite = true
	}
	return outcome
}

func resolveOrganisationsBackfillUser(user bson.Raw) organisationsBackfillUserResolution {
	userID, userIDState := organisationsBootstrapObjectID(user, "_id")
	if userIDState != organisationsBootstrapFieldValue {
		return organisationsBackfillUserResolution{code: "invalid-user-identity", message: "persisted user _id must be a non-zero BSON ObjectID"}
	}
	// users.organisationId is mutable active-selection state and is not evidence
	// of which organisation owned a historical legacy subscription. The stable
	// legacy master relationship, or the master's own deterministic primary
	// organisation identity, is the only safe fallback for canonical-missing rows.
	parentID, parentState := organisationsBackfillStringObjectIDField(user, "user_id")
	if parentState == organisationsBootstrapFieldValue {
		return organisationsBackfillUserResolution{organisationID: parentID}
	}
	if parentState == organisationsBootstrapFieldWrong {
		return organisationsBackfillUserResolution{code: "invalid-user-parent", message: "persisted users.user_id must contain an ObjectID hex string"}
	}
	return organisationsBackfillUserResolution{organisationID: userID}
}

func organisationsBackfillSubscriptionLegacyUser(document bson.Raw) (primitive.ObjectID, organisationsBootstrapFieldState) {
	return organisationsBackfillStringObjectIDField(document, "user_id")
}

func organisationsBackfillSubscriptionCanonicalOrganisation(document bson.Raw) (primitive.ObjectID, organisationsBootstrapFieldState) {
	value := document.Lookup("organisation_id")
	switch value.Type {
	case bsontype.Type(0), bsontype.Null, bsontype.Undefined:
		return primitive.NilObjectID, organisationsBootstrapFieldEmpty
	case bsontype.ObjectID:
		id := value.ObjectID()
		if id.IsZero() {
			return primitive.NilObjectID, organisationsBootstrapFieldWrong
		}
		return id, organisationsBootstrapFieldValue
	default:
		return primitive.NilObjectID, organisationsBootstrapFieldWrong
	}
}

func organisationsBackfillStringObjectIDField(document bson.Raw, field string) (primitive.ObjectID, organisationsBootstrapFieldState) {
	value := document.Lookup(field)
	switch value.Type {
	case bsontype.Type(0), bsontype.Null, bsontype.Undefined:
		return primitive.NilObjectID, organisationsBootstrapFieldEmpty
	case bsontype.String:
		text := value.StringValue()
		if text == "" {
			return primitive.NilObjectID, organisationsBootstrapFieldEmpty
		}
		id, err := primitive.ObjectIDFromHex(text)
		if err != nil || id.IsZero() {
			return primitive.NilObjectID, organisationsBootstrapFieldWrong
		}
		return id, organisationsBootstrapFieldValue
	default:
		return primitive.NilObjectID, organisationsBootstrapFieldWrong
	}
}

func organisationsBackfillDocumentID(document bson.Raw) string {
	value := document.Lookup("_id")
	if value.Type == bsontype.ObjectID {
		return value.ObjectID().Hex()
	}
	if value.Type == bsontype.String {
		return value.StringValue()
	}
	return fmt.Sprintf("<%s>", value.Type)
}

func (outcome *organisationsBackfillSubscriptionOutcome) addConflict(code, message string) {
	outcome.conflicts = append(outcome.conflicts, organisationsBackfillConflict{
		Code:       code,
		DocumentID: outcome.documentID,
		Message:    message,
	})
}

func (outcome *organisationsBackfillSubscriptionOutcome) enrichConflicts() {
	resolved := map[string]struct{}{}
	if outcome.canonicalValid {
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
		if outcome.canonicalValid {
			outcome.conflicts[index].CanonicalOrganisation = outcome.canonicalID.Hex()
		}
		if !outcome.legacyUserID.IsZero() {
			outcome.conflicts[index].LegacyUser = outcome.legacyUserID.Hex()
		}
		outcome.conflicts[index].ResolvedOrganisations = append([]string(nil), resolvedOrganisations...)
	}
}

func organisationsBackfillSubscriptionInScope(outcome organisationsBackfillSubscriptionOutcome, scopeID primitive.ObjectID) bool {
	if scopeID.IsZero() {
		return true
	}
	if outcome.canonicalValid {
		return outcome.canonicalID == scopeID
	}
	return (outcome.canonicalMissing || outcome.canonicalWrong) && outcome.resolvedID == scopeID
}

func addOrganisationsBackfillSubscriptionOutcome(
	report *organisationsBackfillResolution,
	outcome organisationsBackfillSubscriptionOutcome,
) {
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

func resetOrganisationsBackfillScopedInventory(report *organisationsBackfillCollection) {
	report.Total = 0
	report.CanonicalPresent = 0
	report.CanonicalMissing = 0
	report.CanonicalWrongType = 0
	report.CanonicalInvalidHex = 0
	report.ProjectPresent = 0
	report.ProjectMissing = 0
	report.ProjectWrongType = 0
	for candidate := range report.LegacyCandidateCount {
		report.LegacyCandidateCount[candidate] = 0
	}
}

func addOrganisationsBackfillScopedInventory(report *organisationsBackfillCollection, outcome organisationsBackfillSubscriptionOutcome) {
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
	if outcome.legacyPresent {
		report.LegacyCandidateCount["user_id"]++
	}
}

func inspectOrganisationsBackfillSubscriptionIndexes(ctx context.Context, collection *mongo.Collection) ([]organisationsBackfillIndexContract, error) {
	cursor, err := collection.Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	type indexDocument struct {
		Name string `bson:"name"`
		Key  bson.D `bson:"key"`
	}
	indexes := []indexDocument{}
	for cursor.Next(ctx) {
		var index indexDocument
		if err := cursor.Decode(&index); err != nil {
			return nil, err
		}
		indexes = append(indexes, index)
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}
	sort.Slice(indexes, func(left, right int) bool { return indexes[left].Name < indexes[right].Name })

	contracts := []organisationsBackfillIndexContract{
		organisationsBackfillNewIndexContract("active-subscription-scan", bson.D{{Key: "ends_at", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("canonical-active-lookup", bson.D{{Key: "organisation_id", Value: int32(1)}, {Key: "ends_at", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("legacy-rollback", bson.D{{Key: "user_id", Value: int32(1)}, {Key: "ends_at", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("cleanup", bson.D{{Key: "organisation_id", Value: int32(1)}, {Key: "updated_at", Value: int32(-1)}, {Key: "created_at", Value: int32(-1)}, {Key: "_id", Value: int32(-1)}}),
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

func organisationsBackfillNewIndexContract(name string, keys bson.D) organisationsBackfillIndexContract {
	contract := organisationsBackfillIndexContract{Name: name}
	for _, key := range keys {
		contract.Keys = append(contract.Keys, organisationsBackfillIndexKey{Field: key.Key, Direction: organisationsBackfillIndexDirection(key.Value)})
	}
	return contract
}

func organisationsBackfillOrderedIndexCoverage(actual bson.D, required []organisationsBackfillIndexKey) string {
	if len(actual) < len(required) {
		return "missing"
	}
	for index, key := range required {
		if actual[index].Key != key.Field || organisationsBackfillIndexDirection(actual[index].Value) != key.Direction {
			return "missing"
		}
	}
	if len(actual) == len(required) {
		return "exact"
	}
	return "prefix"
}

func organisationsBackfillIndexDirection(value any) int32 {
	switch direction := value.(type) {
	case int:
		return int32(direction)
	case int32:
		return direction
	case int64:
		return int32(direction)
	case float64:
		return int32(direction)
	default:
		return 0
	}
}
