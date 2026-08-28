package actions

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

func inspectOrganisationsBackfillDetections(ctx context.Context, database *mongo.Database, adapter organisationsBackfillAdapter, config OrganisationsBackfillConfig, report organisationsBackfillCollection) (organisationsBackfillCollection, error) {
	documents, err := findOrganisationsBackfillDocuments(ctx, database.Collection(adapter.Collection), config)
	if err != nil {
		return report, err
	}
	keys := make(map[string]struct{})
	for _, document := range documents {
		if key := organisationsBackfillRawString(document, "key"); key != "" {
			keys[key] = struct{}{}
		}
	}
	sources, err := findOrganisationsBackfillRawByString(ctx, database.Collection("analysis"), "key", keys)
	if err != nil {
		return report, err
	}
	organisationIDs := make(map[primitive.ObjectID]struct{})
	projectIDs := make(map[primitive.ObjectID]struct{})
	for _, document := range documents {
		if id, state := organisationsBackfillStringObjectIDField(document, "organisationId"); state == organisationsBootstrapFieldValue {
			organisationIDs[id] = struct{}{}
		}
		if id, state := organisationsBootstrapObjectID(document, "projectId"); state == organisationsBootstrapFieldValue {
			projectIDs[id] = struct{}{}
		}
	}
	for _, source := range sources {
		if id, state := preferredOrganisationsBackfillTenant(source, []string{"userid", "user_id"}); state == organisationsBootstrapFieldValue {
			organisationIDs[id] = struct{}{}
		}
		if id, state := organisationsBootstrapObjectID(source, "projectId"); state == organisationsBootstrapFieldValue {
			projectIDs[id] = struct{}{}
		}
	}
	var scopeID primitive.ObjectID
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
	resolution := organisationsBackfillResolution{ObservedFieldTypes: make(map[string]map[string]int64), ObservedShapes: make(map[string]int64)}
	if config.OrganisationID != "" {
		resetOrganisationsBackfillScopedInventory(&report)
	}
	addOrganisationsBackfillMissingScopeConflict(&resolution, scopeID, organisations)
	for _, document := range documents {
		outcome := resolveOrganisationsBackfillDetection(document, sources, organisations, projects)
		if !organisationsBackfillSiteInScope(outcome, scopeID) {
			continue
		}
		observeOrganisationsBackfillDocument(&resolution, document)
		addOrganisationsBackfillSiteOutcome(&resolution, outcome)
		if config.OrganisationID != "" {
			addOrganisationsBackfillSiteScopedInventory(&report, outcome, "")
		}
	}
	sortOrganisationsBackfillConflicts(&resolution)
	report.Resolution = &resolution
	report.IndexContracts, err = inspectOrganisationsBackfillDetectionIndexes(ctx, database.Collection(adapter.Collection))
	return report, err
}

func resolveOrganisationsBackfillDetection(document bson.Raw, sources map[string]bson.Raw, organisations map[primitive.ObjectID]bool, projects map[primitive.ObjectID]primitive.ObjectID) (outcome organisationsBackfillProjectResourceOutcome) {
	outcome.documentID = organisationsBackfillDocumentID(document)
	defer outcome.enrichConflicts()

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
	default:
		outcome.canonicalMissing = true
	}

	storedProjectID, storedProjectState := organisationsBootstrapObjectID(document, "projectId")
	switch storedProjectState {
	case organisationsBootstrapFieldValue:
		outcome.projectPresent = true
		outcome.resolvedProjectID = storedProjectID
	case organisationsBootstrapFieldEmpty:
		outcome.projectMissing = true
	default:
		outcome.projectWrong = true
		outcome.addConflict("invalid-project-id", "projectId must be a non-zero BSON ObjectID or null")
	}

	key := organisationsBackfillRawString(document, "key")
	if key == "" {
		outcome.zeroCandidate = outcome.canonicalMissing
		outcome.addConflict("unresolved-source", "detection has no recording key")
		return outcome
	}
	source, exists := sources[key]
	if !exists {
		outcome.zeroCandidate = outcome.canonicalMissing
		outcome.addConflict("unresolved-source", "detection key does not resolve to analysis")
		return outcome
	}
	sourceID, sourceState := preferredOrganisationsBackfillTenant(source, []string{"userid", "user_id"})
	if sourceState != organisationsBootstrapFieldValue {
		outcome.zeroCandidate = outcome.canonicalMissing
		outcome.addConflict("unresolved-source", "analysis source ownership cannot be resolved")
		return outcome
	}
	if !organisations[sourceID] {
		outcome.orphanOrganisation = true
		outcome.addConflict("orphan-organisation", "analysis source organisation does not exist")
	}
	if outcome.canonicalMissing {
		outcome.resolvedID = sourceID
		outcome.resolved = true
		outcome.proposedWrite = true
	} else if outcome.canonicalValid && canonicalID != sourceID {
		outcome.addConflict("source-organisation-mismatch", "detection belongs to a different organisation than its analysis source")
	}

	sourceProjectID, sourceProjectState := organisationsBootstrapObjectID(source, "projectId")
	if sourceProjectState == organisationsBootstrapFieldWrong {
		outcome.addConflict("invalid-source-project", "analysis source projectId must be a non-zero BSON ObjectID or null")
		return outcome
	}
	if sourceProjectState == organisationsBootstrapFieldEmpty {
		sourceProjectID = sourceID
	}
	if sourceProjectID != sourceID {
		projectOrganisationID, exists := projects[sourceProjectID]
		if !exists {
			outcome.addConflict("orphan-project", "analysis source projectId does not resolve to a project")
		} else if projectOrganisationID != sourceID {
			outcome.addConflict("project-organisation-mismatch", "analysis source project belongs to a different organisation")
		}
	}
	if outcome.projectMissing {
		outcome.resolvedProjectID = sourceProjectID
		outcome.projectResolved = true
		outcome.proposedProjectWrite = true
	} else if outcome.projectPresent && storedProjectID != sourceProjectID {
		outcome.addConflict("source-project-mismatch", "detection belongs to a different project than its analysis source")
	} else if outcome.projectPresent {
		outcome.projectResolved = true
	}
	return outcome
}

func inspectOrganisationsBackfillDetectionIndexes(ctx context.Context, collection *mongo.Collection) ([]organisationsBackfillIndexContract, error) {
	return inspectOrganisationsBackfillOrderedIndexes(ctx, collection, []organisationsBackfillIndexContract{
		organisationsBackfillNewIndexContract("canonical-key", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "key", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("canonical-run", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "source.runId", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("identity", bson.D{{Key: "key", Value: int32(1)}, {Key: "source.runId", Value: int32(1)}}),
	})
}
