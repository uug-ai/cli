package actions

import (
	"context"
	"sort"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type organisationsBackfillIOOutcome struct {
	organisationsBackfillProjectResourceOutcome
	orphanUser bool
}

func inspectOrganisationsBackfillIO(
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
	projectIDs := make(map[primitive.ObjectID]struct{})
	for _, document := range documents {
		if organisationID, state := organisationsBackfillStringObjectIDField(document, "organisationId"); state == organisationsBootstrapFieldValue {
			organisationIDs[organisationID] = struct{}{}
		} else if state == organisationsBootstrapFieldEmpty {
			if userID, userState := organisationsBackfillStringObjectIDField(document, "user_id"); userState == organisationsBootstrapFieldValue {
				legacyUserIDs[userID] = struct{}{}
			}
		}
		if projectID, state := organisationsBootstrapObjectID(document, "projectId"); state == organisationsBootstrapFieldValue {
			projectIDs[projectID] = struct{}{}
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
		outcome := resolveOrganisationsBackfillIO(document, users, organisations, projects)
		if !organisationsBackfillIOInScope(outcome, scopeID) {
			continue
		}
		observeOrganisationsBackfillDocument(&resolution, document)
		addOrganisationsBackfillSiteOutcome(&resolution, outcome.organisationsBackfillProjectResourceOutcome)
		if outcome.orphanUser {
			resolution.OrphanUsers++
		}
		if config.OrganisationID != "" {
			addOrganisationsBackfillSiteScopedInventory(&report, outcome.organisationsBackfillProjectResourceOutcome)
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
	report.IndexContracts, err = inspectOrganisationsBackfillIOIndexes(ctx, database.Collection(adapter.Collection))
	return report, err
}

func resolveOrganisationsBackfillIO(
	document bson.Raw,
	users map[primitive.ObjectID]bson.Raw,
	organisations map[primitive.ObjectID]bool,
	projects map[primitive.ObjectID]primitive.ObjectID,
) (outcome organisationsBackfillIOOutcome) {
	resource := &outcome.organisationsBackfillProjectResourceOutcome
	resource.documentID = organisationsBackfillDocumentID(document)
	defer resource.enrichConflicts()
	defer resource.resolveProject(projects)

	legacyID, legacyState := organisationsBackfillStringObjectIDField(document, "user_id")
	if legacyState != organisationsBootstrapFieldEmpty {
		resource.legacyPresent = true
	}
	if legacyState == organisationsBootstrapFieldValue {
		resource.legacyUserID = legacyID
	}

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

	switch legacyState {
	case organisationsBootstrapFieldEmpty:
		resource.zeroCandidate = true
		resource.addConflict("zero-candidate", "IO has neither canonical organisationId nor legacy user_id actor")
	case organisationsBootstrapFieldWrong:
		resource.invalidLegacy = true
		resource.addConflict("invalid-legacy-user-id", "user_id must contain an ObjectID hex string")
	case organisationsBootstrapFieldValue:
		user, exists := users[legacyID]
		if !exists {
			outcome.orphanUser = true
			resource.addConflict("orphan-user", "legacy user_id actor does not resolve to a user")
			return outcome
		}
		userResolution := resolveOrganisationsBackfillUser(user)
		if userResolution.code != "" {
			resource.addConflict(userResolution.code, userResolution.message)
			return outcome
		}
		resource.resolvedID = userResolution.organisationID
		if !organisations[resource.resolvedID] {
			resource.orphanOrganisation = true
			resource.addConflict("orphan-organisation", "organisation resolved from legacy actor does not exist")
			return outcome
		}
		resource.resolved = true
		resource.proposedWrite = true
	}
	return outcome
}

func organisationsBackfillIOInScope(outcome organisationsBackfillIOOutcome, scopeID primitive.ObjectID) bool {
	if scopeID.IsZero() {
		return true
	}
	if outcome.canonicalValid {
		return outcome.canonicalID == scopeID
	}
	return outcome.resolvedID == scopeID
}

func inspectOrganisationsBackfillIOIndexes(ctx context.Context, collection *mongo.Collection) ([]organisationsBackfillIndexContract, error) {
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
		organisationsBackfillNewIndexContract("canonical-project-device-list", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "device", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("legacy-project-device-list", bson.D{{Key: "user_id", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "device", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("canonical-project-hash-mutation", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "hash", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("legacy-project-hash-mutation", bson.D{{Key: "user_id", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "hash", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("heartbeat-device-hash-candidate", bson.D{{Key: "device", Value: int32(1)}, {Key: "hash", Value: int32(1)}}),
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
