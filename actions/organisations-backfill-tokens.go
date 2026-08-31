package actions

import (
	"context"
	"sort"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type organisationsBackfillTokenOutcome struct {
	organisationsBackfillProjectResourceOutcome
	orphanUser bool
}

func inspectOrganisationsBackfillTokens(
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
			if userID, userState := organisationsBackfillStringObjectIDField(document, "userId"); userState == organisationsBootstrapFieldValue {
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
		outcome := resolveOrganisationsBackfillToken(document, users, organisations, projects)
		if !organisationsBackfillTokenInScope(outcome, scopeID) {
			continue
		}
		observeOrganisationsBackfillDocument(&resolution, document)
		addOrganisationsBackfillSiteOutcome(&resolution, outcome.organisationsBackfillProjectResourceOutcome)
		if outcome.orphanUser {
			resolution.OrphanUsers++
		}
		if config.OrganisationID != "" {
			addOrganisationsBackfillTokenScopedInventory(&report, outcome.organisationsBackfillProjectResourceOutcome)
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
	report.IndexContracts, err = inspectOrganisationsBackfillTokenIndexes(ctx, database.Collection(adapter.Collection))
	return report, err
}

func resolveOrganisationsBackfillToken(
	document bson.Raw,
	users map[primitive.ObjectID]bson.Raw,
	organisations map[primitive.ObjectID]bool,
	projects map[primitive.ObjectID]primitive.ObjectID,
) (outcome organisationsBackfillTokenOutcome) {
	resource := &outcome.organisationsBackfillProjectResourceOutcome
	resource.documentID = organisationsBackfillDocumentID(document)
	defer resource.enrichConflicts()
	defer resource.resolveProject(projects)

	legacyID, legacyState := organisationsBackfillStringObjectIDField(document, "userId")
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
		resource.addConflict("zero-candidate", "token has neither canonical organisationId nor legacy userId creator")
	case organisationsBootstrapFieldWrong:
		resource.invalidLegacy = true
		resource.addConflict("invalid-legacy-user-id", "userId must contain an ObjectID hex string")
	case organisationsBootstrapFieldValue:
		user, exists := users[legacyID]
		if !exists {
			outcome.orphanUser = true
			resource.addConflict("orphan-user", "legacy userId creator does not resolve to a user")
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
			resource.addConflict("orphan-organisation", "organisation resolved from legacy creator does not exist")
			return outcome
		}
		resource.resolved = true
		resource.proposedWrite = true
	}
	return outcome
}

func organisationsBackfillTokenInScope(outcome organisationsBackfillTokenOutcome, scopeID primitive.ObjectID) bool {
	if scopeID.IsZero() {
		return true
	}
	if outcome.canonicalValid {
		return outcome.canonicalID == scopeID
	}
	return outcome.resolvedID == scopeID
}

func addOrganisationsBackfillTokenScopedInventory(report *organisationsBackfillCollection, outcome organisationsBackfillProjectResourceOutcome) {
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
	if outcome.legacyPresent {
		report.LegacyCandidateCount["userId"]++
	}
}

func inspectOrganisationsBackfillTokenIndexes(ctx context.Context, collection *mongo.Collection) ([]organisationsBackfillIndexContract, error) {
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
		organisationsBackfillNewIndexContract("canonical-project-token", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "_id", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("legacy-project-token", bson.D{{Key: "userId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "_id", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("primary-token-id", bson.D{{Key: "_id", Value: int32(1)}}),
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
