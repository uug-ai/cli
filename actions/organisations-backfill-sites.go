package actions

import (
	"context"
	"sort"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type organisationsBackfillProjectResourceOutcome struct {
	documentID           string
	canonicalID          primitive.ObjectID
	canonicalValid       bool
	canonicalMissing     bool
	canonicalWrong       bool
	projectPresent       bool
	projectMissing       bool
	projectWrong         bool
	resolvedProjectID    primitive.ObjectID
	projectResolved      bool
	proposedProjectWrite bool
	legacyUserID         primitive.ObjectID
	legacyPresent        bool
	invalidLegacy        bool
	resolvedID           primitive.ObjectID
	resolved             bool
	zeroCandidate        bool
	multipleCandidates   bool
	orphanOrganisation   bool
	proposedWrite        bool
	conflicts            []organisationsBackfillConflict
}

func inspectOrganisationsBackfillSites(
	ctx context.Context,
	database *mongo.Database,
	adapter organisationsBackfillAdapter,
	config OrganisationsBackfillConfig,
	report organisationsBackfillCollection,
) (organisationsBackfillCollection, error) {
	return inspectOrganisationsBackfillProjectResource(
		ctx,
		database,
		adapter,
		config,
		report,
		"site",
		inspectOrganisationsBackfillSiteIndexes,
	)
}

func inspectOrganisationsBackfillProjectResource(
	ctx context.Context,
	database *mongo.Database,
	adapter organisationsBackfillAdapter,
	config OrganisationsBackfillConfig,
	report organisationsBackfillCollection,
	resourceName string,
	inspectIndexes func(context.Context, *mongo.Collection) ([]organisationsBackfillIndexContract, error),
) (organisationsBackfillCollection, error) {
	var scopeID primitive.ObjectID
	legacyField := adapter.LegacyCandidates[0]
	if config.OrganisationID != "" {
		scopeID, _ = primitive.ObjectIDFromHex(config.OrganisationID)
	}
	documents, err := findOrganisationsBackfillDocuments(ctx, database.Collection(adapter.Collection), config)
	if err != nil {
		return report, err
	}

	organisationIDs := make(map[primitive.ObjectID]struct{})
	projectIDs := make(map[primitive.ObjectID]struct{})
	for _, document := range documents {
		if id, state := organisationsBackfillStringObjectIDField(document, "organisationId"); state == organisationsBootstrapFieldValue {
			organisationIDs[id] = struct{}{}
		} else if state == organisationsBootstrapFieldEmpty {
			if legacyID, legacyState := organisationsBackfillStringObjectIDField(document, legacyField); legacyState == organisationsBootstrapFieldValue {
				organisationIDs[legacyID] = struct{}{}
			}
		}
		if projectID, state := organisationsBootstrapObjectID(document, "projectId"); state == organisationsBootstrapFieldValue {
			projectIDs[projectID] = struct{}{}
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
		outcome := resolveOrganisationsBackfillProjectResource(document, resourceName, legacyField, organisations, projects)
		if !organisationsBackfillSiteInScope(outcome, scopeID) {
			continue
		}
		observeOrganisationsBackfillDocument(&resolution, document)
		addOrganisationsBackfillSiteOutcome(&resolution, outcome)
		if config.OrganisationID != "" {
			addOrganisationsBackfillSiteScopedInventory(&report, outcome, legacyField)
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
	report.IndexContracts, err = inspectIndexes(ctx, database.Collection(adapter.Collection))
	return report, err
}

func resolveOrganisationsBackfillSite(
	document bson.Raw,
	organisations map[primitive.ObjectID]bool,
	projects map[primitive.ObjectID]primitive.ObjectID,
) (outcome organisationsBackfillProjectResourceOutcome) {
	return resolveOrganisationsBackfillProjectResource(document, "site", "user_id", organisations, projects)
}

func resolveOrganisationsBackfillProjectResource(
	document bson.Raw,
	resourceName string,
	legacyField string,
	organisations map[primitive.ObjectID]bool,
	projects map[primitive.ObjectID]primitive.ObjectID,
) (outcome organisationsBackfillProjectResourceOutcome) {
	outcome.documentID = organisationsBackfillDocumentID(document)
	defer outcome.enrichConflicts()
	defer outcome.resolveProject(projects)

	legacyID, legacyState := organisationsBackfillStringObjectIDField(document, legacyField)
	if legacyState != organisationsBootstrapFieldEmpty {
		outcome.legacyPresent = true
	}
	if legacyState == organisationsBootstrapFieldValue {
		outcome.legacyUserID = legacyID
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

	canonicalID, canonicalState := organisationsBackfillStringObjectIDField(document, "organisationId")
	switch canonicalState {
	case organisationsBootstrapFieldValue:
		outcome.canonicalID = canonicalID
		outcome.canonicalValid = true
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

	switch legacyState {
	case organisationsBootstrapFieldEmpty:
		outcome.zeroCandidate = true
		outcome.addConflict("zero-candidate", resourceName+" has neither canonical organisationId nor legacy "+legacyField)
	case organisationsBootstrapFieldWrong:
		outcome.invalidLegacy = true
		outcome.addConflict("invalid-legacy-owner-id", legacyField+" must contain an ObjectID hex string")
	case organisationsBootstrapFieldValue:
		outcome.resolvedID = legacyID
		if !organisations[legacyID] {
			outcome.orphanOrganisation = true
			outcome.addConflict("orphan-organisation", "legacy "+legacyField+" organisation does not exist")
			return outcome
		}
		outcome.resolved = true
		outcome.proposedWrite = true
	}
	return outcome
}

func (outcome *organisationsBackfillProjectResourceOutcome) resolveProject(projects map[primitive.ObjectID]primitive.ObjectID) {
	if outcome.projectWrong || len(outcome.conflicts) > 0 {
		return
	}
	organisationID := outcome.resolvedID
	if outcome.canonicalValid {
		organisationID = outcome.canonicalID
	}
	if organisationID.IsZero() {
		return
	}
	if outcome.projectPresent {
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
		return
	}
	if outcome.projectMissing {
		outcome.resolvedProjectID = organisationID
		outcome.projectResolved = true
		outcome.proposedProjectWrite = true
	}
}

func (outcome *organisationsBackfillProjectResourceOutcome) addConflict(code, message string) {
	outcome.conflicts = append(outcome.conflicts, organisationsBackfillConflict{Code: code, DocumentID: outcome.documentID, Message: message})
}

func (outcome *organisationsBackfillProjectResourceOutcome) enrichConflicts() {
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
		if !outcome.legacyUserID.IsZero() {
			outcome.conflicts[index].LegacyUser = outcome.legacyUserID.Hex()
		}
		outcome.conflicts[index].ResolvedOrganisations = append([]string(nil), resolvedOrganisations...)
	}
}

func organisationsBackfillSiteInScope(outcome organisationsBackfillProjectResourceOutcome, scopeID primitive.ObjectID) bool {
	if scopeID.IsZero() {
		return true
	}
	if !outcome.canonicalID.IsZero() {
		return outcome.canonicalID == scopeID
	}
	return outcome.resolvedID == scopeID
}

func addOrganisationsBackfillSiteOutcome(report *organisationsBackfillResolution, outcome organisationsBackfillProjectResourceOutcome) {
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
	if outcome.orphanOrganisation {
		report.OrphanOrganisations++
	}
	if outcome.proposedWrite {
		report.ProposedWrites++
	}
	if outcome.projectResolved {
		report.ProjectResolved++
	}
	if outcome.proposedProjectWrite {
		report.ProposedProjectWrites++
	}
	report.Conflicts += int64(len(outcome.conflicts))
	report.ConflictEntries = append(report.ConflictEntries, outcome.conflicts...)
}

func addOrganisationsBackfillSiteScopedInventory(report *organisationsBackfillCollection, outcome organisationsBackfillProjectResourceOutcome, legacyField string) {
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
	if outcome.legacyPresent && legacyField != "" {
		report.LegacyCandidateCount[legacyField]++
	}
}

func inspectOrganisationsBackfillSiteIndexes(ctx context.Context, collection *mongo.Collection) ([]organisationsBackfillIndexContract, error) {
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
		organisationsBackfillNewIndexContract("canonical-project-list", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("legacy-project-list", bson.D{{Key: "user_id", Value: int32(1)}, {Key: "projectId", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("device-membership-lookup", bson.D{{Key: "devices", Value: int32(1)}}),
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
