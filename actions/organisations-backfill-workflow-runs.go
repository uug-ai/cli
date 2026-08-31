package actions

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type organisationsBackfillWorkflowRunOutcome struct {
	organisationsBackfillProjectResourceOutcome
	accidentalUserPresent bool
}

type organisationsBackfillRelatedOwnership struct {
	organisationID primitive.ObjectID
	projectID      primitive.ObjectID
	resolved       bool
}

func inspectOrganisationsBackfillWorkflowRuns(ctx context.Context, database *mongo.Database, adapter organisationsBackfillAdapter, config OrganisationsBackfillConfig, report organisationsBackfillCollection) (organisationsBackfillCollection, error) {
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
	taskIDs := make(map[primitive.ObjectID]struct{})
	mediaKeys := make(map[string]struct{})
	workflowIDs := make(map[primitive.ObjectID]struct{})
	for _, document := range documents {
		if id, state := organisationsBackfillStringObjectIDField(document, "organisationId"); state == organisationsBootstrapFieldValue {
			organisationIDs[id] = struct{}{}
		} else if state == organisationsBootstrapFieldEmpty {
			if id, legacyState := organisationsBackfillStringObjectIDField(document, "userid"); legacyState == organisationsBootstrapFieldValue {
				organisationIDs[id] = struct{}{}
			}
		}
		if id, state := organisationsBootstrapObjectID(document, "projectId"); state == organisationsBootstrapFieldValue {
			projectIDs[id] = struct{}{}
		}
		if sourceRef := organisationsBackfillRawString(document, "sourceref"); sourceRef != "" {
			if id, parseErr := primitive.ObjectIDFromHex(sourceRef); parseErr == nil {
				taskIDs[id] = struct{}{}
			}
		} else if key := organisationsBackfillRawString(document, "key"); key != "" {
			mediaKeys[key] = struct{}{}
		}
		if id, parseErr := primitive.ObjectIDFromHex(organisationsBackfillRawString(document, "workflowid")); parseErr == nil {
			workflowIDs[id] = struct{}{}
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
	tasks, err := findOrganisationsBackfillRawByObjectID(ctx, database.Collection("tasks"), taskIDs)
	if err != nil {
		return report, err
	}
	media, err := findOrganisationsBackfillRawByString(ctx, database.Collection("media"), "key", mediaKeys)
	if err != nil {
		return report, err
	}
	workflows, err := findOrganisationsBackfillRawByObjectID(ctx, database.Collection("workflows"), workflowIDs)
	if err != nil {
		return report, err
	}
	definitionCreatorIDs := make(map[primitive.ObjectID]struct{})
	for _, definition := range workflows {
		canonicalID, canonicalState := organisationsBackfillStringObjectIDField(definition, "organisationId")
		_, legacyState := organisationsBackfillStringObjectIDField(definition, "organisation_id")
		if canonicalState == organisationsBootstrapFieldValue {
			organisationIDs[canonicalID] = struct{}{}
		} else if canonicalState == organisationsBootstrapFieldEmpty && legacyState == organisationsBootstrapFieldEmpty {
			if creatorID, state := organisationsBackfillStringObjectIDField(definition, "user_id"); state == organisationsBootstrapFieldValue {
				definitionCreatorIDs[creatorID] = struct{}{}
			}
		}
	}
	definitionUsers, err := findOrganisationsBackfillUsers(ctx, database.Collection("users"), definitionCreatorIDs)
	if err != nil {
		return report, err
	}

	resolution := organisationsBackfillResolution{ObservedFieldTypes: make(map[string]map[string]int64), ObservedShapes: make(map[string]int64)}
	if config.OrganisationID != "" {
		resetOrganisationsBackfillScopedInventory(&report)
	}
	addOrganisationsBackfillMissingScopeConflict(&resolution, scopeID, organisations)
	for _, document := range documents {
		outcome := resolveOrganisationsBackfillWorkflowRun(document, organisations, projects, tasks, media, workflows, definitionUsers)
		if !organisationsBackfillSiteInScope(outcome.organisationsBackfillProjectResourceOutcome, scopeID) {
			continue
		}
		observeOrganisationsBackfillDocument(&resolution, document)
		addOrganisationsBackfillSiteOutcome(&resolution, outcome.organisationsBackfillProjectResourceOutcome)
		if config.OrganisationID != "" {
			addOrganisationsBackfillWorkflowBaseInventory(&report, outcome.organisationsBackfillProjectResourceOutcome)
			if outcome.legacyPresent {
				report.LegacyCandidateCount["userid"]++
			}
			if outcome.accidentalUserPresent {
				report.LegacyCandidateCount["user_id"]++
			}
		}
	}
	sortOrganisationsBackfillConflicts(&resolution)
	report.Resolution = &resolution
	report.IndexContracts, err = inspectOrganisationsBackfillWorkflowRunIndexes(ctx, database.Collection(adapter.Collection))
	return report, err
}

func resolveOrganisationsBackfillWorkflowRun(document bson.Raw, organisations map[primitive.ObjectID]bool, projects map[primitive.ObjectID]primitive.ObjectID, tasks map[primitive.ObjectID]bson.Raw, media map[string]bson.Raw, workflows map[primitive.ObjectID]bson.Raw, definitionUsers map[primitive.ObjectID]bson.Raw) (outcome organisationsBackfillWorkflowRunOutcome) {
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
	legacyID, legacyState := organisationsBackfillStringObjectIDField(document, "userid")
	resource.legacyPresent = legacyState != organisationsBootstrapFieldEmpty
	if legacyState == organisationsBootstrapFieldValue {
		resource.legacyUserID = legacyID
	}
	_, accidentalState := organisationsBackfillStringObjectIDField(document, "user_id")
	outcome.accidentalUserPresent = accidentalState != organisationsBootstrapFieldEmpty

	canonicalID, canonicalState := organisationsBackfillStringObjectIDField(document, "organisationId")
	switch canonicalState {
	case organisationsBootstrapFieldValue:
		resource.canonicalID = canonicalID
		resource.canonicalValid = true
		if !organisations[canonicalID] {
			resource.orphanOrganisation = true
			resource.addConflict("orphan-organisation", "canonical organisation does not exist")
		}
	case organisationsBootstrapFieldWrong:
		resource.canonicalWrong = true
		resource.addConflict("invalid-canonical-organisation", "organisationId must contain an ObjectID hex string")
	default:
		resource.canonicalMissing = true
		switch legacyState {
		case organisationsBootstrapFieldEmpty:
			resource.zeroCandidate = true
			resource.addConflict("zero-candidate", "workflow run has neither canonical organisationId nor legacy userid")
		case organisationsBootstrapFieldWrong:
			resource.invalidLegacy = true
			resource.addConflict("invalid-legacy-userid", "userid must contain an ObjectID hex string")
		case organisationsBootstrapFieldValue:
			resource.resolvedID = legacyID
			if !organisations[legacyID] {
				resource.orphanOrganisation = true
				resource.addConflict("orphan-organisation", "legacy userid organisation does not exist")
			} else {
				resource.resolved = true
				resource.proposedWrite = true
			}
		}
	}
	organisationID := resource.resolvedID
	if resource.canonicalValid {
		organisationID = resource.canonicalID
	}
	if organisationID.IsZero() || len(resource.conflicts) > 0 {
		return outcome
	}
	validateOrganisationsBackfillRunSource(resource, document, organisationID, tasks, media)
	validateOrganisationsBackfillRunWorkflow(resource, document, organisationID, workflows, definitionUsers)
	return outcome
}

func validateOrganisationsBackfillRunSource(resource *organisationsBackfillProjectResourceOutcome, document bson.Raw, organisationID primitive.ObjectID, tasks map[primitive.ObjectID]bson.Raw, media map[string]bson.Raw) {
	sourceRef := organisationsBackfillRawString(document, "sourceref")
	if sourceRef != "" {
		taskID, err := primitive.ObjectIDFromHex(sourceRef)
		if err != nil {
			resource.addConflict("unresolved-source", "sourceref is not a case ObjectID")
			return
		}
		task, exists := tasks[taskID]
		if !exists {
			resource.addConflict("unresolved-source", "sourceref does not resolve to a case")
			return
		}
		validateOrganisationsBackfillRelatedOwnership(resource, "source", organisationID, relatedOrganisationsBackfillOwnership(task, nil))
		return
	}
	key := organisationsBackfillRawString(document, "key")
	if key == "" {
		resource.addConflict("unresolved-source", "workflow run has neither sourceref nor media key")
		return
	}
	source, exists := media[key]
	if !exists {
		resource.addConflict("unresolved-source", "key does not resolve to media")
		return
	}
	validateOrganisationsBackfillRelatedOwnership(resource, "source", organisationID, relatedOrganisationsBackfillOwnership(source, nil))
}

func validateOrganisationsBackfillRunWorkflow(resource *organisationsBackfillProjectResourceOutcome, document bson.Raw, organisationID primitive.ObjectID, workflows map[primitive.ObjectID]bson.Raw, users map[primitive.ObjectID]bson.Raw) {
	workflowID, err := primitive.ObjectIDFromHex(organisationsBackfillRawString(document, "workflowid"))
	if err != nil {
		return
	}
	definition, exists := workflows[workflowID]
	if !exists {
		return
	}
	validateOrganisationsBackfillRelatedOwnership(resource, "definition", organisationID, relatedOrganisationsBackfillOwnership(definition, users))
}

func relatedOrganisationsBackfillOwnership(document bson.Raw, users map[primitive.ObjectID]bson.Raw) organisationsBackfillRelatedOwnership {
	result := organisationsBackfillRelatedOwnership{}
	if id, state := organisationsBackfillStringObjectIDField(document, "organisationId"); state == organisationsBootstrapFieldValue {
		result.organisationID = id
		result.resolved = true
	} else if state == organisationsBootstrapFieldEmpty {
		if id, legacyState := organisationsBackfillStringObjectIDField(document, "organisation_id"); legacyState == organisationsBootstrapFieldValue {
			result.organisationID = id
			result.resolved = true
		} else if legacyState == organisationsBootstrapFieldEmpty {
			if id, tenantState := organisationsBackfillStringObjectIDField(document, "userid"); tenantState == organisationsBootstrapFieldValue {
				result.organisationID = id
				result.resolved = true
			} else if creatorID, creatorState := organisationsBackfillStringObjectIDField(document, "user_id"); creatorState == organisationsBootstrapFieldValue && users != nil {
				if user, exists := users[creatorID]; exists {
					resolved := resolveOrganisationsBackfillUser(user)
					if resolved.code == "" {
						result.organisationID = resolved.organisationID
						result.resolved = true
					}
				}
			}
		}
	}
	if projectID, state := organisationsBootstrapObjectID(document, "projectId"); state == organisationsBootstrapFieldValue {
		result.projectID = projectID
	}
	return result
}

func validateOrganisationsBackfillRelatedOwnership(resource *organisationsBackfillProjectResourceOutcome, relationship string, organisationID primitive.ObjectID, related organisationsBackfillRelatedOwnership) {
	if !related.resolved {
		resource.addConflict("unresolved-"+relationship, relationship+" ownership cannot be resolved")
		return
	}
	if related.organisationID != organisationID {
		resource.addConflict(relationship+"-organisation-mismatch", relationship+" belongs to a different organisation")
		return
	}
	runProjectID := resource.resolvedProjectID
	if resource.projectMissing {
		runProjectID = organisationID
	}
	relatedProjectID := related.projectID
	if relatedProjectID.IsZero() {
		relatedProjectID = related.organisationID
	}
	if runProjectID != relatedProjectID {
		resource.addConflict(relationship+"-project-mismatch", relationship+" belongs to a different project")
	}
}

func inspectOrganisationsBackfillWorkflowRunIndexes(ctx context.Context, collection *mongo.Collection) ([]organisationsBackfillIndexContract, error) {
	return inspectOrganisationsBackfillOrderedIndexes(ctx, collection, []organisationsBackfillIndexContract{
		organisationsBackfillNewIndexContract("canonical-status", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "sourceref", Value: int32(1)}, {Key: "origin", Value: int32(1)}, {Key: "start", Value: int32(-1)}}),
		organisationsBackfillNewIndexContract("legacy-userid-status", bson.D{{Key: "userid", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "sourceref", Value: int32(1)}, {Key: "origin", Value: int32(1)}, {Key: "start", Value: int32(-1)}}),
		organisationsBackfillNewIndexContract("canonical-recording-retention", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "recordingtimestamp", Value: int32(1)}, {Key: "_id", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("canonical-start-retention", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "start", Value: int32(1)}, {Key: "_id", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("legacy-userid-recording-retention", bson.D{{Key: "userid", Value: int32(1)}, {Key: "recordingtimestamp", Value: int32(1)}, {Key: "_id", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("legacy-userid-start-retention", bson.D{{Key: "userid", Value: int32(1)}, {Key: "start", Value: int32(1)}, {Key: "_id", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("global-recording-retention", bson.D{{Key: "recordingtimestamp", Value: int32(1)}, {Key: "_id", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("global-start-retention", bson.D{{Key: "start", Value: int32(1)}, {Key: "_id", Value: int32(1)}}),
	})
}

func organisationsBackfillRawString(document bson.Raw, field string) string {
	value := document.Lookup(field)
	if value.Type != bsontype.String {
		return ""
	}
	return value.StringValue()
}

func findOrganisationsBackfillRawByObjectID(ctx context.Context, collection *mongo.Collection, ids map[primitive.ObjectID]struct{}) (map[primitive.ObjectID]bson.Raw, error) {
	result := make(map[primitive.ObjectID]bson.Raw)
	if len(ids) == 0 {
		return result, nil
	}
	values := make([]primitive.ObjectID, 0, len(ids))
	for id := range ids {
		values = append(values, id)
	}
	cursor, err := collection.Find(ctx, bson.M{"_id": bson.M{"$in": values}})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var documents []bson.Raw
	if err := cursor.All(ctx, &documents); err != nil {
		return nil, err
	}
	for _, document := range documents {
		if id, state := organisationsBootstrapObjectID(document, "_id"); state == organisationsBootstrapFieldValue {
			result[id] = document
		}
	}
	return result, nil
}

func findOrganisationsBackfillRawByString(ctx context.Context, collection *mongo.Collection, field string, valuesSet map[string]struct{}) (map[string]bson.Raw, error) {
	result := make(map[string]bson.Raw)
	if len(valuesSet) == 0 {
		return result, nil
	}
	values := make([]string, 0, len(valuesSet))
	for value := range valuesSet {
		values = append(values, value)
	}
	cursor, err := collection.Find(ctx, bson.M{field: bson.M{"$in": values}})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var documents []bson.Raw
	if err := cursor.All(ctx, &documents); err != nil {
		return nil, err
	}
	for _, document := range documents {
		if value := organisationsBackfillRawString(document, field); value != "" {
			result[value] = document
		}
	}
	return result, nil
}
