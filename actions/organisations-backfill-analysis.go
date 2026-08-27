package actions

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

func inspectOrganisationsBackfillAnalysis(ctx context.Context, database *mongo.Database, adapter organisationsBackfillAdapter, config OrganisationsBackfillConfig, report organisationsBackfillCollection) (organisationsBackfillCollection, error) {
	return inspectOrganisationsBackfillAliasedProjectResource(ctx, database, adapter, config, report, "analysis", inspectOrganisationsBackfillAnalysisIndexes)
}

func inspectOrganisationsBackfillAliasedProjectResource(ctx context.Context, database *mongo.Database, adapter organisationsBackfillAdapter, config OrganisationsBackfillConfig, report organisationsBackfillCollection, resourceName string, inspectIndexes func(context.Context, *mongo.Collection) ([]organisationsBackfillIndexContract, error)) (organisationsBackfillCollection, error) {
	var scopeID primitive.ObjectID
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
		if id, state := preferredOrganisationsBackfillTenant(document, adapter.LegacyCandidates); state == organisationsBootstrapFieldValue {
			organisationIDs[id] = struct{}{}
		}
		if id, state := organisationsBootstrapObjectID(document, "projectId"); state == organisationsBootstrapFieldValue {
			projectIDs[id] = struct{}{}
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
		outcome := resolveOrganisationsBackfillAliasedProjectResource(document, resourceName, adapter.LegacyCandidates, organisations, projects)
		if !organisationsBackfillSiteInScope(outcome, scopeID) {
			continue
		}
		observeOrganisationsBackfillDocument(&resolution, document)
		addOrganisationsBackfillSiteOutcome(&resolution, outcome)
		if config.OrganisationID != "" {
			addOrganisationsBackfillSiteScopedInventory(&report, outcome)
		}
	}
	sortOrganisationsBackfillConflicts(&resolution)
	report.Resolution = &resolution
	report.IndexContracts, err = inspectIndexes(ctx, database.Collection(adapter.Collection))
	return report, err
}

func resolveOrganisationsBackfillAliasedProjectResource(document bson.Raw, resourceName string, aliases []string, organisations map[primitive.ObjectID]bool, projects map[primitive.ObjectID]primitive.ObjectID) (outcome organisationsBackfillProjectResourceOutcome) {
	outcome.documentID = organisationsBackfillDocumentID(document)
	defer outcome.enrichConflicts()
	defer outcome.resolveProject(projects)

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
	if canonicalState == organisationsBootstrapFieldValue {
		outcome.canonicalID = canonicalID
		outcome.canonicalValid = true
		if !organisations[canonicalID] {
			outcome.orphanOrganisation = true
			outcome.addConflict("orphan-organisation", "canonical organisation does not exist")
		}
		return outcome
	}
	if canonicalState == organisationsBootstrapFieldWrong {
		outcome.canonicalWrong = true
		outcome.addConflict("invalid-canonical-organisation", "organisationId must contain an ObjectID hex string")
		return outcome
	}
	outcome.canonicalMissing = true

	legacyID, legacyState := preferredOrganisationsBackfillTenant(document, aliases)
	outcome.legacyPresent = legacyState != organisationsBootstrapFieldEmpty
	if legacyState == organisationsBootstrapFieldValue {
		outcome.legacyUserID = legacyID
		outcome.resolvedID = legacyID
		if !organisations[legacyID] {
			outcome.orphanOrganisation = true
			outcome.addConflict("orphan-organisation", "legacy tenant does not resolve to an organisation")
			return outcome
		}
		outcome.resolved = true
		outcome.proposedWrite = true
		return outcome
	}
	if legacyState == organisationsBootstrapFieldWrong {
		outcome.invalidLegacy = true
		outcome.addConflict("invalid-legacy-tenant", "highest-precedence legacy tenant must contain an ObjectID hex string")
		return outcome
	}
	outcome.zeroCandidate = true
	outcome.addConflict("zero-candidate", resourceName+" has no canonical or stable legacy tenant")
	return outcome
}

func preferredOrganisationsBackfillTenant(document bson.Raw, aliases []string) (primitive.ObjectID, organisationsBootstrapFieldState) {
	if id, state := organisationsBackfillStringObjectIDField(document, "organisationId"); state != organisationsBootstrapFieldEmpty {
		return id, state
	}
	for _, field := range aliases {
		if id, state := organisationsBackfillStringObjectIDField(document, field); state != organisationsBootstrapFieldEmpty {
			return id, state
		}
	}
	return primitive.NilObjectID, organisationsBootstrapFieldEmpty
}

func inspectOrganisationsBackfillAnalysisIndexes(ctx context.Context, collection *mongo.Collection) ([]organisationsBackfillIndexContract, error) {
	return inspectOrganisationsBackfillOrderedIndexes(ctx, collection, []organisationsBackfillIndexContract{
		organisationsBackfillNewIndexContract("canonical-key", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "key", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("legacy-userid-key", bson.D{{Key: "userid", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "key", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("legacy-user-id-key", bson.D{{Key: "user_id", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "key", Value: int32(1)}}),
	})
}
