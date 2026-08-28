package actions

import (
	"context"
	"sort"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type organisationsBackfillWorkflowOutcome struct {
	organisationsBackfillProjectResourceOutcome
	legacyOrganisationPresent bool
	creatorPresent            bool
	orphanUser                bool
}

func inspectOrganisationsBackfillWorkflows(ctx context.Context, database *mongo.Database, adapter organisationsBackfillAdapter, config OrganisationsBackfillConfig, report organisationsBackfillCollection) (organisationsBackfillCollection, error) {
	var scopeID primitive.ObjectID
	if config.OrganisationID != "" {
		scopeID, _ = primitive.ObjectIDFromHex(config.OrganisationID)
	}
	documents, err := findOrganisationsBackfillDocuments(ctx, database.Collection(adapter.Collection), config)
	if err != nil {
		return report, err
	}
	creatorIDs := make(map[primitive.ObjectID]struct{})
	organisationIDs := make(map[primitive.ObjectID]struct{})
	projectIDs := make(map[primitive.ObjectID]struct{})
	for _, document := range documents {
		canonicalID, canonicalState := organisationsBackfillStringObjectIDField(document, "organisationId")
		legacyID, legacyState := organisationsBackfillStringObjectIDField(document, "organisation_id")
		if canonicalState == organisationsBootstrapFieldValue {
			organisationIDs[canonicalID] = struct{}{}
		} else if canonicalState == organisationsBootstrapFieldEmpty && legacyState == organisationsBootstrapFieldValue {
			organisationIDs[legacyID] = struct{}{}
		} else if canonicalState == organisationsBootstrapFieldEmpty && legacyState == organisationsBootstrapFieldEmpty {
			if creatorID, state := organisationsBackfillStringObjectIDField(document, "user_id"); state == organisationsBootstrapFieldValue {
				creatorIDs[creatorID] = struct{}{}
			}
		}
		if projectID, state := organisationsBootstrapObjectID(document, "projectId"); state == organisationsBootstrapFieldValue {
			projectIDs[projectID] = struct{}{}
		}
	}
	users, err := findOrganisationsBackfillUsers(ctx, database.Collection("users"), creatorIDs)
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
	resolution := organisationsBackfillResolution{ObservedFieldTypes: make(map[string]map[string]int64), ObservedShapes: make(map[string]int64)}
	if config.OrganisationID != "" {
		resetOrganisationsBackfillScopedInventory(&report)
	}
	addOrganisationsBackfillMissingScopeConflict(&resolution, scopeID, organisations)
	for _, document := range documents {
		outcome := resolveOrganisationsBackfillWorkflow(document, users, organisations, projects)
		if !organisationsBackfillSiteInScope(outcome.organisationsBackfillProjectResourceOutcome, scopeID) {
			continue
		}
		observeOrganisationsBackfillDocument(&resolution, document)
		addOrganisationsBackfillSiteOutcome(&resolution, outcome.organisationsBackfillProjectResourceOutcome)
		if outcome.orphanUser {
			resolution.OrphanUsers++
		}
		if config.OrganisationID != "" {
			addOrganisationsBackfillWorkflowInventory(&report, outcome)
		}
	}
	sortOrganisationsBackfillConflicts(&resolution)
	report.Resolution = &resolution
	report.IndexContracts, err = inspectOrganisationsBackfillWorkflowIndexes(ctx, database.Collection(adapter.Collection))
	return report, err
}

func resolveOrganisationsBackfillWorkflow(document bson.Raw, users map[primitive.ObjectID]bson.Raw, organisations map[primitive.ObjectID]bool, projects map[primitive.ObjectID]primitive.ObjectID) (outcome organisationsBackfillWorkflowOutcome) {
	resource := &outcome.organisationsBackfillProjectResourceOutcome
	resource.documentID = organisationsBackfillDocumentID(document)
	defer resource.enrichConflicts()
	defer resource.resolveProject(projects)

	projectID, projectState := organisationsBootstrapObjectID(document, "projectId")
	switch projectState {
	case organisationsBootstrapFieldValue:
		resource.projectPresent = true
		resource.resolvedProjectID = projectID
	case organisationsBootstrapFieldEmpty:
		resource.projectMissing = true
	default:
		resource.projectWrong = true
		resource.addConflict("invalid-project-id", "projectId must be a non-zero BSON ObjectID or null")
	}
	legacyID, legacyState := organisationsBackfillStringObjectIDField(document, "organisation_id")
	outcome.legacyOrganisationPresent = legacyState != organisationsBootstrapFieldEmpty
	creatorID, creatorState := organisationsBackfillStringObjectIDField(document, "user_id")
	outcome.creatorPresent = creatorState != organisationsBootstrapFieldEmpty

	canonicalID, canonicalState := organisationsBackfillStringObjectIDField(document, "organisationId")
	switch canonicalState {
	case organisationsBootstrapFieldValue:
		resource.canonicalID = canonicalID
		resource.canonicalValid = true
		if !organisations[canonicalID] {
			resource.orphanOrganisation = true
			resource.addConflict("orphan-organisation", "canonical organisation does not exist")
		}
		return outcome
	case organisationsBootstrapFieldWrong:
		resource.canonicalWrong = true
		resource.addConflict("invalid-canonical-organisation", "organisationId must contain an ObjectID hex string")
		return outcome
	default:
		resource.canonicalMissing = true
	}

	if legacyState == organisationsBootstrapFieldValue {
		resource.legacyPresent = true
		resource.legacyUserID = legacyID
		resource.resolvedID = legacyID
		if !organisations[legacyID] {
			resource.orphanOrganisation = true
			resource.addConflict("orphan-organisation", "legacy organisation_id does not resolve to an organisation")
			return outcome
		}
		resource.resolved = true
		resource.proposedWrite = true
		return outcome
	}
	if legacyState == organisationsBootstrapFieldWrong {
		resource.invalidLegacy = true
		resource.addConflict("invalid-legacy-organisation-id", "organisation_id must contain an ObjectID hex string")
		return outcome
	}

	switch creatorState {
	case organisationsBootstrapFieldEmpty:
		resource.zeroCandidate = true
		resource.addConflict("zero-candidate", "workflow has no canonical, legacy, or stable creator ownership")
	case organisationsBootstrapFieldWrong:
		resource.invalidLegacy = true
		resource.addConflict("invalid-creator-user-id", "user_id must contain an ObjectID hex string")
	case organisationsBootstrapFieldValue:
		resource.legacyPresent = true
		resource.legacyUserID = creatorID
		user, exists := users[creatorID]
		if !exists {
			outcome.orphanUser = true
			resource.addConflict("orphan-user", "creator user_id does not resolve to a user")
			return outcome
		}
		resolved := resolveOrganisationsBackfillUser(user)
		if resolved.code != "" {
			resource.addConflict(resolved.code, resolved.message)
			return outcome
		}
		resource.resolvedID = resolved.organisationID
		if !organisations[resource.resolvedID] {
			resource.orphanOrganisation = true
			resource.addConflict("orphan-organisation", "creator's stable organisation does not exist")
			return outcome
		}
		resource.resolved = true
		resource.proposedWrite = true
	}
	return outcome
}

func inspectOrganisationsBackfillWorkflowIndexes(ctx context.Context, collection *mongo.Collection) ([]organisationsBackfillIndexContract, error) {
	return inspectOrganisationsBackfillOrderedIndexes(ctx, collection, []organisationsBackfillIndexContract{
		organisationsBackfillNewIndexContract("canonical-enabled", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "enabled", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("legacy-organisation-enabled", bson.D{{Key: "organisation_id", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "enabled", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("legacy-creator-enabled", bson.D{{Key: "user_id", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "enabled", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("canonical-name", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "name", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("legacy-organisation-name", bson.D{{Key: "organisation_id", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "name", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("legacy-creator-name", bson.D{{Key: "user_id", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "name", Value: int32(1)}}),
	})
}

func addOrganisationsBackfillWorkflowInventory(report *organisationsBackfillCollection, outcome organisationsBackfillWorkflowOutcome) {
	addOrganisationsBackfillWorkflowBaseInventory(report, outcome.organisationsBackfillProjectResourceOutcome)
	if outcome.legacyOrganisationPresent {
		report.LegacyCandidateCount["organisation_id"]++
	}
	if outcome.creatorPresent {
		report.LegacyCandidateCount["user_id"]++
	}
}

func addOrganisationsBackfillWorkflowBaseInventory(report *organisationsBackfillCollection, outcome organisationsBackfillProjectResourceOutcome) {
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
}

func addOrganisationsBackfillMissingScopeConflict(resolution *organisationsBackfillResolution, scopeID primitive.ObjectID, organisations map[primitive.ObjectID]bool) {
	if scopeID.IsZero() || organisations[scopeID] {
		return
	}
	resolution.OrphanOrganisations++
	resolution.Conflicts++
	resolution.ConflictEntries = append(resolution.ConflictEntries, organisationsBackfillConflict{Code: "scope-organisation-not-found", CanonicalOrganisation: scopeID.Hex(), ResolvedOrganisations: []string{scopeID.Hex()}, Message: "requested organisation does not exist"})
}

func sortOrganisationsBackfillConflicts(resolution *organisationsBackfillResolution) {
	sort.Slice(resolution.ConflictEntries, func(left, right int) bool {
		if resolution.ConflictEntries[left].DocumentID != resolution.ConflictEntries[right].DocumentID {
			return resolution.ConflictEntries[left].DocumentID < resolution.ConflictEntries[right].DocumentID
		}
		return resolution.ConflictEntries[left].Code < resolution.ConflictEntries[right].Code
	})
	if len(resolution.ConflictEntries) > organisationsBackfillConflictLimit {
		resolution.ConflictEntries = resolution.ConflictEntries[:organisationsBackfillConflictLimit]
	}
}

func inspectOrganisationsBackfillOrderedIndexes(ctx context.Context, collection *mongo.Collection, contracts []organisationsBackfillIndexContract) ([]organisationsBackfillIndexContract, error) {
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
