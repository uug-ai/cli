package actions

import (
	"context"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type organisationsBackfillMarkerParent struct {
	organisationID primitive.ObjectID
	projectID      primitive.ObjectID
	valid          bool
}

type organisationsBackfillMarkerParents struct {
	byKey map[string][]organisationsBackfillMarkerParent
}

type organisationsBackfillMarkerLinks struct {
	keys       []string
	legacyKeys []string
	usesKeys   bool
	malformed  bool
}

type organisationsBackfillMarkerResolvedDocument struct {
	document bson.Raw
	outcome  organisationsBackfillProjectResourceOutcome
}

func inspectOrganisationsBackfillMarkers(
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
	mediaKeys := make(map[string]struct{})
	deviceKeys := make(map[string]struct{})
	for _, document := range documents {
		links := organisationsBackfillMarkerLinksFromDocument(document)
		organisationID, _ := organisationsBackfillStringObjectIDField(document, "organisationId")
		for _, mediaKey := range organisationsBackfillMarkerLookupKeys(links.keys, organisationID) {
			mediaKeys[mediaKey] = struct{}{}
		}
		for _, mediaKey := range organisationsBackfillMarkerLookupKeys(links.legacyKeys, organisationID) {
			mediaKeys[mediaKey] = struct{}{}
		}
		if deviceKey := organisationsBackfillMarkerDeviceKey(document); deviceKey != "" {
			deviceKeys[deviceKey] = struct{}{}
		}
	}
	parents, err := findOrganisationsBackfillMarkerParents(ctx, database.Collection("media"), mediaKeys)
	if err != nil {
		return report, err
	}
	devices, err := findOrganisationsBackfillMarkerDevices(ctx, database.Collection("devices"), deviceKeys)
	if err != nil {
		return report, err
	}

	organisationIDs := make(map[primitive.ObjectID]struct{})
	projectIDs := make(map[primitive.ObjectID]struct{})
	for _, document := range documents {
		if organisationID, state := organisationsBackfillStringObjectIDField(document, "organisationId"); state == organisationsBootstrapFieldValue {
			organisationIDs[organisationID] = struct{}{}
		}
		if projectID, state := organisationsBootstrapObjectID(document, "projectId"); state == organisationsBootstrapFieldValue {
			projectIDs[projectID] = struct{}{}
		}
	}
	for _, matches := range parents.byKey {
		for _, parent := range matches {
			if parent.valid {
				organisationIDs[parent.organisationID] = struct{}{}
				projectIDs[parent.projectID] = struct{}{}
			}
		}
	}
	for _, matches := range devices.byKey {
		for _, parent := range matches {
			if parent.valid {
				organisationIDs[parent.organisationID] = struct{}{}
				projectIDs[parent.projectID] = struct{}{}
			}
		}
	}
	scopeID := primitive.NilObjectID
	if config.OrganisationID != "" {
		scopeID, _ = primitive.ObjectIDFromHex(config.OrganisationID)
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

	resolution := newOrganisationsBackfillResolution()
	if !scopeID.IsZero() {
		resetOrganisationsBackfillScopedInventory(&report)
		addOrganisationsBackfillMissingScope(&resolution, scopeID, organisations)
	}
	for _, document := range documents {
		outcome := resolveOrganisationsBackfillMarker(document, parents, devices, organisations, projects)
		if !organisationsBackfillSiteInScope(outcome, scopeID) {
			continue
		}
		observeOrganisationsBackfillDocument(&resolution, document)
		addOrganisationsBackfillSiteOutcome(&resolution, outcome)
		if !scopeID.IsZero() {
			addOrganisationsBackfillSiteScopedInventory(&report, outcome, "")
			if organisationsBackfillMarkerFieldPresent(document, "mediaKeys") {
				report.LegacyCandidateCount["mediaKeys"]++
			}
			if organisationsBackfillMarkerFieldPresent(document, "mediaIds") {
				report.LegacyCandidateCount["mediaIds"]++
			}
		}
	}
	finalizeOrganisationsBackfillResolution(&resolution)
	report.Resolution = &resolution
	report.IndexContracts, err = inspectOrganisationsBackfillOrderedIndexes(ctx, database.Collection(adapter.Collection), organisationsBackfillMarkerIndexContracts())
	return report, err
}

func findOrganisationsBackfillMarkerParents(
	ctx context.Context,
	collection *mongo.Collection,
	keys map[string]struct{},
) (organisationsBackfillMarkerParents, error) {
	parents := organisationsBackfillMarkerParents{
		byKey: make(map[string][]organisationsBackfillMarkerParent, len(keys)),
	}
	if len(keys) == 0 {
		return parents, nil
	}
	cursor, err := collection.Find(ctx, bson.M{"videoFile": bson.M{"$in": sortedOrganisationsBackfillStrings(keys)}}, options.Find().SetProjection(bson.M{
		"_id": 1, "videoFile": 1, "organisationId": 1, "projectId": 1,
	}))
	if err != nil {
		return parents, err
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var document bson.Raw
		if err := cursor.Decode(&document); err != nil {
			return parents, err
		}
		organisationID, organisationState := organisationsBackfillStringObjectIDField(document, "organisationId")
		parent := organisationsBackfillMarkerParent{}
		projectID, projectState := organisationsBootstrapObjectID(document, "projectId")
		if organisationState == organisationsBootstrapFieldValue {
			if projectState == organisationsBootstrapFieldEmpty {
				projectID = organisationID
			}
			if projectState == organisationsBootstrapFieldValue || projectID == organisationID {
				parent = organisationsBackfillMarkerParent{organisationID: organisationID, projectID: projectID, valid: true}
			}
		}
		videoFile := document.Lookup("videoFile")
		if videoFile.Type == bsontype.String {
			key := videoFile.StringValue()
			if _, requested := keys[key]; requested {
				parents.byKey[key] = append(parents.byKey[key], parent)
			}
		}
	}
	return parents, cursor.Err()
}

func findOrganisationsBackfillMarkerDevices(
	ctx context.Context,
	collection *mongo.Collection,
	keys map[string]struct{},
) (organisationsBackfillMarkerParents, error) {
	devices := organisationsBackfillMarkerParents{byKey: make(map[string][]organisationsBackfillMarkerParent, len(keys))}
	if len(keys) == 0 {
		return devices, nil
	}
	cursor, err := collection.Find(ctx, bson.M{"key": bson.M{"$in": sortedOrganisationsBackfillStrings(keys)}}, options.Find().SetProjection(bson.M{
		"key": 1, "organisationId": 1, "projectId": 1, "user_id": 1,
	}))
	if err != nil {
		return devices, err
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var document bson.Raw
		if err := cursor.Decode(&document); err != nil {
			return devices, err
		}
		key := document.Lookup("key")
		if key.Type != bsontype.String {
			continue
		}
		organisationID, organisationState := organisationsBackfillStringObjectIDField(document, "organisationId")
		parent := organisationsBackfillMarkerParent{}
		projectID, projectState := organisationsBootstrapObjectID(document, "projectId")
		if organisationState == organisationsBootstrapFieldValue {
			if projectState == organisationsBootstrapFieldEmpty {
				projectID = organisationID
			}
			if projectState == organisationsBootstrapFieldValue || projectID == organisationID {
				parent = organisationsBackfillMarkerParent{organisationID: organisationID, projectID: projectID, valid: true}
			}
		}
		devices.byKey[key.StringValue()] = append(devices.byKey[key.StringValue()], parent)
	}
	return devices, cursor.Err()
}

func resolveOrganisationsBackfillMarker(
	document bson.Raw,
	parents organisationsBackfillMarkerParents,
	devices organisationsBackfillMarkerParents,
	organisations map[primitive.ObjectID]bool,
	projects map[primitive.ObjectID]primitive.ObjectID,
) (outcome organisationsBackfillProjectResourceOutcome) {
	outcome.documentID = organisationsBackfillDocumentID(document)
	defer outcome.enrichConflicts()

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

	canonicalID, canonicalState := organisationsBackfillStringObjectIDField(document, "organisationId")
	switch canonicalState {
	case organisationsBootstrapFieldValue:
		outcome.canonicalID = canonicalID
		outcome.canonicalValid = true
		if !organisations[canonicalID] {
			outcome.orphanOrganisation = true
			outcome.addConflict("orphan-organisation", "canonical organisation does not exist")
		}
	case organisationsBootstrapFieldWrong:
		outcome.canonicalWrong = true
		outcome.addConflict("invalid-canonical-organisation", "organisationId must contain an ObjectID hex string")
		return outcome
	default:
		outcome.canonicalMissing = true
	}

	links := organisationsBackfillMarkerLinksFromDocument(document)
	links.keys = organisationsBackfillMarkerLookupKeys(links.keys, outcome.canonicalID)
	links.legacyKeys = organisationsBackfillMarkerLookupKeys(links.legacyKeys, outcome.canonicalID)
	if outcome.canonicalValid {
		outcome.resolveMarkerProject(outcome.canonicalID, projects)
		if len(links.keys) == 0 && len(links.legacyKeys) == 0 {
			return outcome
		}
		parent, code := organisationsBackfillResolveMarkerParent(links, parents, projects, outcome.canonicalID, outcome.resolvedProjectID, outcome.projectPresent)
		if code != "" {
			parent, code = organisationsBackfillResolveMarkerParent(links, parents, projects, primitive.NilObjectID, primitive.NilObjectID, false)
		}
		if code == "" {
			return outcome.resolveMarkerParent(parent, organisations, projects, "parent")
		}
		return outcome
	}
	if len(links.keys) == 0 && len(links.legacyKeys) == 0 {
		device, code := organisationsBackfillResolveMarkerDevice(document, devices, projects)
		if code != "" {
			if outcome.canonicalMissing {
				outcome.zeroCandidate = code == "zero-device"
				outcome.multipleCandidates = code == "ambiguous-device"
				outcome.addConflict(code, "marker has no unique stored device ownership source")
			} else {
				outcome.resolveProject(projects)
			}
			return outcome
		}
		return outcome.resolveMarkerParent(device, organisations, projects, "device")
	}
	parent, code := organisationsBackfillResolveMarkerParent(links, parents, projects, outcome.canonicalID, outcome.resolvedProjectID, outcome.projectPresent)
	if code != "" {
		if code == "ambiguous-parent" {
			outcome.multipleCandidates = true
		}
		outcome.addConflict(code, "linked media does not provide one authoritative organisation and project")
		return outcome
	}

	return outcome.resolveMarkerParent(parent, organisations, projects, "parent")
}

func (outcome organisationsBackfillProjectResourceOutcome) resolveMarkerParent(
	parent organisationsBackfillMarkerParent,
	organisations map[primitive.ObjectID]bool,
	projects map[primitive.ObjectID]primitive.ObjectID,
	source string,
) organisationsBackfillProjectResourceOutcome {
	if outcome.canonicalValid {
		outcome.resolvedID = parent.organisationID
		if outcome.canonicalID != parent.organisationID {
			outcome.addConflict("canonical-"+source+"-organisation-mismatch", "canonical organisationId differs from stored "+source+" ownership")
		}
		outcome.resolveMarkerProject(outcome.canonicalID, projects)
		if !outcome.projectWrong && outcome.resolvedProjectID != parent.projectID {
			outcome.addConflict("canonical-"+source+"-project-mismatch", "marker projectId differs from stored "+source+" ownership")
		}
		return outcome
	}
	if !organisations[parent.organisationID] {
		outcome.orphanOrganisation = true
		outcome.addConflict("orphan-organisation", "linked media organisation does not exist")
		return outcome
	}
	outcome.resolvedID = parent.organisationID
	outcome.resolved = true
	outcome.proposedWrite = true
	if outcome.projectPresent && outcome.resolvedProjectID != parent.projectID {
		outcome.addConflict("parent-project-mismatch", "marker projectId differs from linked media ownership")
		return outcome
	}
	if outcome.projectMissing {
		outcome.resolvedProjectID = parent.projectID
		outcome.projectResolved = true
		outcome.proposedProjectWrite = true
		return outcome
	}
	outcome.resolveMarkerProject(parent.organisationID, projects)
	return outcome
}

func organisationsBackfillResolveMarkerParent(
	links organisationsBackfillMarkerLinks,
	parents organisationsBackfillMarkerParents,
	projects map[primitive.ObjectID]primitive.ObjectID,
	expectedOrganisation primitive.ObjectID,
	expectedProject primitive.ObjectID,
	hasExpectedProject bool,
) (organisationsBackfillMarkerParent, string) {
	if links.malformed {
		return organisationsBackfillMarkerParent{}, "orphan-parent"
	}
	candidates := make([]organisationsBackfillMarkerParent, 0, len(links.keys)+len(links.legacyKeys))
	if links.usesKeys {
		for _, mediaKey := range links.keys {
			matches := organisationsBackfillMarkerParentsInScope(parents.byKey[mediaKey], expectedOrganisation, expectedProject, hasExpectedProject)
			if len(matches) == 0 {
				return organisationsBackfillMarkerParent{}, "orphan-parent"
			}
			if len(matches) != 1 {
				return organisationsBackfillMarkerParent{}, "ambiguous-parent"
			}
			candidates = append(candidates, matches[0])
		}
	} else {
		for _, mediaKey := range links.legacyKeys {
			matches := organisationsBackfillMarkerParentsInScope(parents.byKey[mediaKey], expectedOrganisation, expectedProject, hasExpectedProject)
			if len(matches) == 0 {
				return organisationsBackfillMarkerParent{}, "orphan-parent"
			}
			if len(matches) != 1 {
				return organisationsBackfillMarkerParent{}, "ambiguous-parent"
			}
			candidates = append(candidates, matches[0])
		}
	}
	var resolved organisationsBackfillMarkerParent
	for _, parent := range candidates {
		if !parent.valid {
			return organisationsBackfillMarkerParent{}, "unresolved-parent-ownership"
		}
		if parent.projectID != parent.organisationID {
			projectOrganisationID, exists := projects[parent.projectID]
			if !exists || projectOrganisationID != parent.organisationID {
				return organisationsBackfillMarkerParent{}, "orphan-parent"
			}
		}
		if !resolved.valid {
			resolved = parent
			continue
		}
		if resolved.organisationID != parent.organisationID || resolved.projectID != parent.projectID {
			return organisationsBackfillMarkerParent{}, "ambiguous-parent"
		}
	}
	return resolved, ""
}

func organisationsBackfillMarkerParentsInScope(
	parents []organisationsBackfillMarkerParent,
	organisationID primitive.ObjectID,
	projectID primitive.ObjectID,
	hasProject bool,
) []organisationsBackfillMarkerParent {
	if organisationID.IsZero() {
		return parents
	}
	filtered := make([]organisationsBackfillMarkerParent, 0, len(parents))
	for _, parent := range parents {
		if parent.organisationID != organisationID || (hasProject && parent.projectID != projectID) {
			continue
		}
		filtered = append(filtered, parent)
	}
	return filtered
}

func organisationsBackfillResolveMarkerDevice(
	document bson.Raw,
	devices organisationsBackfillMarkerParents,
	projects map[primitive.ObjectID]primitive.ObjectID,
) (organisationsBackfillMarkerParent, string) {
	deviceKey := organisationsBackfillMarkerDeviceKey(document)
	if deviceKey == "" {
		return organisationsBackfillMarkerParent{}, "zero-device"
	}
	matches := devices.byKey[deviceKey]
	if len(matches) == 0 {
		return organisationsBackfillMarkerParent{}, "orphan-device"
	}
	if len(matches) != 1 {
		return organisationsBackfillMarkerParent{}, "ambiguous-device"
	}
	device := matches[0]
	if !device.valid {
		return organisationsBackfillMarkerParent{}, "unresolved-device-ownership"
	}
	if device.projectID != device.organisationID {
		organisationID, exists := projects[device.projectID]
		if !exists || organisationID != device.organisationID {
			return organisationsBackfillMarkerParent{}, "orphan-device-project"
		}
	}
	return device, ""
}

func organisationsBackfillMarkerDeviceKey(document bson.Raw) string {
	for _, field := range []string{"deviceId", "deviceKey"} {
		value := document.Lookup(field)
		if value.Type == bsontype.String && value.StringValue() != "" {
			return value.StringValue()
		}
	}
	return ""
}

func (outcome *organisationsBackfillProjectResourceOutcome) resolveMarkerProject(
	organisationID primitive.ObjectID,
	projects map[primitive.ObjectID]primitive.ObjectID,
) {
	if outcome.projectWrong || organisationID.IsZero() {
		return
	}
	if outcome.projectMissing {
		outcome.resolvedProjectID = organisationID
		outcome.projectResolved = true
		outcome.proposedProjectWrite = true
		return
	}
	if outcome.resolvedProjectID == organisationID {
		outcome.projectResolved = true
		return
	}
	projectOrganisationID, exists := projects[outcome.resolvedProjectID]
	if !exists {
		outcome.addConflict("orphan-project", "projectId does not resolve to a project")
		return
	}
	if projectOrganisationID != organisationID {
		outcome.addConflict("project-organisation-mismatch", "projectId belongs to a different organisation")
		return
	}
	outcome.projectResolved = true
}

func organisationsBackfillMarkerLinksFromDocument(document bson.Raw) organisationsBackfillMarkerLinks {
	if keys, present, malformed := organisationsBackfillMarkerStringArray(document, "mediaKeys"); present {
		if len(keys) > 0 || malformed {
			return organisationsBackfillMarkerLinks{keys: keys, usesKeys: true, malformed: malformed}
		}
	}
	values, present, malformed := organisationsBackfillMarkerStringArray(document, "mediaIds")
	links := organisationsBackfillMarkerLinks{legacyKeys: values, malformed: malformed}
	if !present {
		return links
	}
	return links
}

func organisationsBackfillMarkerLookupKeys(keys []string, organisationID primitive.ObjectID) []string {
	seen := make(map[string]struct{}, len(keys)*2)
	result := make([]string, 0, len(keys)*2)
	for _, key := range keys {
		candidates := []string{key}
		if !organisationID.IsZero() && !strings.Contains(key, "/") {
			candidates = append(candidates, organisationID.Hex()+"/"+key)
		}
		for _, candidate := range candidates {
			if _, exists := seen[candidate]; exists {
				continue
			}
			seen[candidate] = struct{}{}
			result = append(result, candidate)
		}
	}
	sort.Strings(result)
	return result
}

func organisationsBackfillMarkerStringArray(document bson.Raw, field string) ([]string, bool, bool) {
	value := document.Lookup(field)
	if value.Type == bsontype.Type(0) || value.Type == bsontype.Null || value.Type == bsontype.Undefined {
		return nil, false, false
	}
	if value.Type != bsontype.Array {
		return nil, true, true
	}
	values, err := value.Array().Values()
	if err != nil {
		return nil, true, true
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	malformed := false
	for _, item := range values {
		if item.Type != bsontype.String || item.StringValue() == "" {
			malformed = true
			continue
		}
		text := item.StringValue()
		if _, exists := seen[text]; !exists {
			seen[text] = struct{}{}
			result = append(result, text)
		}
	}
	sort.Strings(result)
	return result, true, malformed
}

func organisationsBackfillMarkerFieldPresent(document bson.Raw, field string) bool {
	_, present, _ := organisationsBackfillMarkerStringArray(document, field)
	return present
}

func sortedOrganisationsBackfillStrings(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func organisationsBackfillMarkerIndexContracts() []organisationsBackfillIndexContract {
	return []organisationsBackfillIndexContract{
		organisationsBackfillNewIndexContract("project-time-list", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "startTimestamp", Value: int32(-1)}, {Key: "_id", Value: int32(-1)}}),
		organisationsBackfillNewIndexContract("project-device-name-time", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "deviceId", Value: int32(1)}, {Key: "name", Value: int32(1)}, {Key: "startTimestamp", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("project-media-keys", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "mediaKeys", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("project-device-time", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "deviceId", Value: int32(1)}, {Key: "startTimestamp", Value: int32(1)}}),
	}
}

func inspectOrganisationsBackfillMarkerCanonical(
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
	organisationIDs := make(map[primitive.ObjectID]struct{})
	projectIDs := make(map[primitive.ObjectID]struct{})
	for _, document := range documents {
		if organisationID, state := organisationsBackfillStringObjectIDField(document, "organisationId"); state == organisationsBootstrapFieldValue {
			organisationIDs[organisationID] = struct{}{}
		}
		if projectID, state := organisationsBootstrapObjectID(document, "projectId"); state == organisationsBootstrapFieldValue {
			projectIDs[projectID] = struct{}{}
		}
	}
	scopeID := primitive.NilObjectID
	if config.OrganisationID != "" {
		scopeID, _ = primitive.ObjectIDFromHex(config.OrganisationID)
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

	resolution := newOrganisationsBackfillResolution()
	if !scopeID.IsZero() {
		resetOrganisationsBackfillScopedInventory(&report)
		addOrganisationsBackfillMissingScope(&resolution, scopeID, organisations)
	}
	resolvedDocuments := make([]organisationsBackfillMarkerResolvedDocument, 0, len(documents))
	for _, document := range documents {
		outcome := resolveOrganisationsBackfillCanonicalOnly(document, adapter.Collection, organisations, projects)
		if organisationsBackfillMarkerOptionCollection(adapter.Collection) {
			value := document.Lookup("value")
			if value.Type != bsontype.String || value.StringValue() == "" {
				outcome.addConflict("invalid-option-value", "option value must be a non-empty string before a unique index can be created")
			}
		}
		if organisationsBackfillMarkerRangeCollection(adapter.Collection) && !organisationsBackfillValidMarkerRangeIdentity(document) {
			outcome.addConflict("invalid-range-identity", "range value, deviceId, and start must be valid before a unique index can be created")
		}
		if !organisationsBackfillSiteInScope(outcome, scopeID) {
			continue
		}
		if adapter.LifecycleStatus == "inactive" || adapter.OperationalUse == "retention-only" {
			outcome.proposedProjectWrite = false
		}
		observeOrganisationsBackfillDocument(&resolution, document)
		addOrganisationsBackfillSiteOutcome(&resolution, outcome)
		if !scopeID.IsZero() {
			addOrganisationsBackfillSiteScopedInventory(&report, outcome, "")
		}
		resolvedDocuments = append(resolvedDocuments, organisationsBackfillMarkerResolvedDocument{document: document, outcome: outcome})
	}
	if organisationsBackfillMarkerOptionCollection(adapter.Collection) && config.DocumentID == "" {
		addOrganisationsBackfillMarkerOptionDuplicates(&resolution, resolvedDocuments)
	}
	if organisationsBackfillMarkerRangeCollection(adapter.Collection) && config.DocumentID == "" {
		addOrganisationsBackfillMarkerRangeDuplicates(&resolution, resolvedDocuments)
	}
	finalizeOrganisationsBackfillResolution(&resolution)
	report.Resolution = &resolution
	report.IndexContracts, err = inspectOrganisationsBackfillMarkerCanonicalIndexes(ctx, database.Collection(adapter.Collection), adapter.Collection)
	return report, err
}

func resolveOrganisationsBackfillCanonicalOnly(
	document bson.Raw,
	resourceName string,
	organisations map[primitive.ObjectID]bool,
	projects map[primitive.ObjectID]primitive.ObjectID,
) (outcome organisationsBackfillProjectResourceOutcome) {
	outcome.documentID = organisationsBackfillDocumentID(document)
	defer outcome.enrichConflicts()
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
	organisationID, organisationState := organisationsBackfillStringObjectIDField(document, "organisationId")
	switch organisationState {
	case organisationsBootstrapFieldValue:
		outcome.canonicalID = organisationID
		outcome.canonicalValid = true
		if !organisations[organisationID] {
			outcome.orphanOrganisation = true
			outcome.addConflict("orphan-organisation", "canonical organisation does not exist")
			return outcome
		}
	case organisationsBootstrapFieldWrong:
		outcome.canonicalWrong = true
		outcome.addConflict("invalid-canonical-organisation", "organisationId must contain an ObjectID hex string")
		return outcome
	default:
		outcome.canonicalMissing = true
		outcome.zeroCandidate = true
		outcome.addConflict("missing-canonical-organisation", resourceName+" has no trustworthy ownership fallback")
		return outcome
	}
	outcome.resolveMarkerProject(organisationID, projects)
	return outcome
}

func addOrganisationsBackfillMarkerOptionDuplicates(report *organisationsBackfillResolution, documents []organisationsBackfillMarkerResolvedDocument) {
	counts := make(map[string]int64)
	for _, resolved := range documents {
		if !resolved.outcome.canonicalValid || !resolved.outcome.projectResolved || len(resolved.outcome.conflicts) > 0 {
			continue
		}
		value := resolved.document.Lookup("value")
		if value.Type != bsontype.String || value.StringValue() == "" {
			continue
		}
		key := resolved.outcome.canonicalID.Hex() + "\x00" + resolved.outcome.resolvedProjectID.Hex() + "\x00" + value.StringValue()
		counts[key]++
	}
	for _, count := range counts {
		if count < 2 {
			continue
		}
		report.MultipleCandidates += count
		report.Conflicts++
		report.ConflictEntries = append(report.ConflictEntries, organisationsBackfillConflict{
			Code:    "duplicate-option-value",
			Message: "a redacted option value is duplicated within one organisation and project",
		})
	}
}

func organisationsBackfillMarkerOptionCollection(collection string) bool {
	switch collection {
	case "marker_options", "marker_tag_options", "marker_event_options", "marker_category_options":
		return true
	default:
		return false
	}
}

func organisationsBackfillMarkerRangeCollection(collection string) bool {
	switch collection {
	case "marker_option_ranges", "marker_tag_option_ranges", "marker_event_option_ranges":
		return true
	default:
		return false
	}
}

func organisationsBackfillValidMarkerRangeIdentity(document bson.Raw) bool {
	value := document.Lookup("value")
	deviceID := document.Lookup("deviceId")
	start := document.Lookup("start")
	if value.Type != bsontype.String || value.StringValue() == "" || deviceID.Type != bsontype.String || deviceID.StringValue() == "" {
		return false
	}
	return start.Type == bsontype.Int32 || start.Type == bsontype.Int64 || start.Type == bsontype.Double
}

func addOrganisationsBackfillMarkerRangeDuplicates(report *organisationsBackfillResolution, documents []organisationsBackfillMarkerResolvedDocument) {
	counts := make(map[string]int64)
	for _, resolved := range documents {
		if !resolved.outcome.canonicalValid || !resolved.outcome.projectResolved || len(resolved.outcome.conflicts) > 0 || !organisationsBackfillValidMarkerRangeIdentity(resolved.document) {
			continue
		}
		value := resolved.document.Lookup("value").StringValue()
		deviceID := resolved.document.Lookup("deviceId").StringValue()
		start, ok := organisationsBackfillMarkerNumericIdentity(resolved.document.Lookup("start"))
		if !ok {
			continue
		}
		key := resolved.outcome.canonicalID.Hex() + "\x00" + resolved.outcome.resolvedProjectID.Hex() + "\x00" + value + "\x00" + deviceID + "\x00" + start
		counts[key]++
	}
	for _, count := range counts {
		if count < 2 {
			continue
		}
		report.MultipleCandidates += count
		report.Conflicts++
		report.ConflictEntries = append(report.ConflictEntries, organisationsBackfillConflict{
			Code:    "duplicate-range-identity",
			Message: "a redacted range identity is duplicated within one organisation and project",
		})
	}
}

func organisationsBackfillMarkerNumericIdentity(value bson.RawValue) (string, bool) {
	switch value.Type {
	case bsontype.Int32:
		return strconv.FormatInt(int64(value.Int32()), 10), true
	case bsontype.Int64:
		return strconv.FormatInt(value.Int64(), 10), true
	case bsontype.Double:
		return strconv.FormatFloat(value.Double(), 'g', -1, 64), true
	default:
		return "", false
	}
}

func organisationsBackfillMarkerCanonicalIndexContracts(collection string) []organisationsBackfillIndexContract {
	if organisationsBackfillMarkerOptionCollection(collection) {
		contracts := []organisationsBackfillIndexContract{
			organisationsBackfillNewIndexContract("project-value-unique", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "value", Value: int32(1)}}),
			organisationsBackfillNewIndexContract("project-updated-list", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "updatedAt", Value: int32(-1)}, {Key: "_id", Value: int32(-1)}}),
		}
		contracts[0].Unique = true
		return contracts
	}
	contracts := []organisationsBackfillIndexContract{
		organisationsBackfillNewIndexContract("project-text-range", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "text", Value: int32(1)}, {Key: "start", Value: int32(1)}, {Key: "end", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("project-value-device-start-unique", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "value", Value: int32(1)}, {Key: "deviceId", Value: int32(1)}, {Key: "start", Value: int32(1)}}),
	}
	if collection == "marker_option_ranges" {
		contracts = append(contracts,
			organisationsBackfillNewIndexContract("project-value-device-key-range", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "value", Value: int32(1)}, {Key: "deviceKey", Value: int32(1)}, {Key: "start", Value: int32(1)}, {Key: "end", Value: int32(1)}}),
			organisationsBackfillNewIndexContract("project-range", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "start", Value: int32(1)}, {Key: "end", Value: int32(1)}}),
		)
	}
	contracts[1].Unique = true
	contracts[1].PartialFilterExpression = markerRangePartialFilter()
	return contracts
}

func markerRangePartialFilter() bson.M {
	return bson.M{
		"organisationId": bson.M{"$type": "string"},
		"projectId":      bson.M{"$type": "objectId"},
		"value":          bson.M{"$type": "string"},
		"deviceId":       bson.M{"$type": "string"},
		"start":          bson.M{"$type": "number"},
	}
}

func inspectOrganisationsBackfillMarkerCanonicalIndexes(
	ctx context.Context,
	collection *mongo.Collection,
	collectionName string,
) ([]organisationsBackfillIndexContract, error) {
	cursor, err := collection.Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var indexes []struct {
		Name                    string `bson:"name"`
		Key                     bson.D `bson:"key"`
		Unique                  bool   `bson:"unique"`
		PartialFilterExpression bson.M `bson:"partialFilterExpression"`
	}
	if err := cursor.All(ctx, &indexes); err != nil {
		return nil, err
	}
	contracts := organisationsBackfillMarkerCanonicalIndexContracts(collectionName)
	for index := range contracts {
		contracts[index].Status = "missing"
		for _, candidate := range indexes {
			status := organisationsBackfillOrderedIndexCoverage(candidate.Key, contracts[index].Keys)
			if contracts[index].Unique != candidate.Unique || !reflect.DeepEqual(contracts[index].PartialFilterExpression, candidate.PartialFilterExpression) {
				continue
			}
			if status == "exact" || (status == "prefix" && contracts[index].Status == "missing") {
				contracts[index].Status = status
				contracts[index].IndexName = candidate.Name
			}
			if status == "exact" {
				break
			}
		}
	}
	if organisationsBackfillMarkerOptionCollection(collectionName) {
		obsoleteKeys := []organisationsBackfillIndexKey{{Field: "organisationId", Direction: 1}, {Field: "value", Direction: 1}}
		for _, candidate := range indexes {
			if candidate.Unique && organisationsBackfillOrderedIndexCoverage(candidate.Key, obsoleteKeys) == "exact" {
				obsolete := organisationsBackfillIndexContract{Name: "obsolete-organisation-value-unique", Keys: obsoleteKeys, Status: "obsolete", IndexName: candidate.Name}
				contracts = append(contracts, obsolete)
				break
			}
		}
	}
	return contracts, nil
}

func newOrganisationsBackfillResolution() organisationsBackfillResolution {
	return organisationsBackfillResolution{ObservedFieldTypes: make(map[string]map[string]int64), ObservedShapes: make(map[string]int64)}
}

func addOrganisationsBackfillMissingScope(report *organisationsBackfillResolution, scopeID primitive.ObjectID, organisations map[primitive.ObjectID]bool) {
	if scopeID.IsZero() || organisations[scopeID] {
		return
	}
	report.OrphanOrganisations++
	report.Conflicts++
	report.ConflictEntries = append(report.ConflictEntries, organisationsBackfillConflict{
		Code:                  "scope-organisation-not-found",
		CanonicalOrganisation: scopeID.Hex(),
		ResolvedOrganisations: []string{scopeID.Hex()},
		Message:               "requested organisation does not exist",
	})
}

func finalizeOrganisationsBackfillResolution(report *organisationsBackfillResolution) {
	sort.Slice(report.ConflictEntries, func(left, right int) bool {
		first := report.ConflictEntries[left]
		second := report.ConflictEntries[right]
		if first.DocumentID != second.DocumentID {
			return first.DocumentID < second.DocumentID
		}
		return first.Code < second.Code
	})
	if len(report.ConflictEntries) > organisationsBackfillConflictLimit {
		report.ConflictEntries = report.ConflictEntries[:organisationsBackfillConflictLimit]
	}
}
