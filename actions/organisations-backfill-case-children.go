package actions

import (
	"context"
	"fmt"
	"sort"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type organisationsBackfillCaseChildOutcome struct {
	organisationsBackfillProjectResourceOutcome
	taskID       primitive.ObjectID
	orphanParent bool
}

func inspectOrganisationsBackfillTasks(
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
		"task",
		inspectOrganisationsBackfillTaskIndexes,
	)
}

func inspectOrganisationsBackfillCaseChildren(
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
	taskIDs := make(map[primitive.ObjectID]struct{})
	for _, document := range documents {
		if taskID, state := organisationsBackfillCaseChildTaskID(document, adapter.Collection); state == organisationsBootstrapFieldValue {
			taskIDs[taskID] = struct{}{}
		}
	}
	tasks, err := findOrganisationsBackfillCaseParentTasks(ctx, database.Collection("tasks"), taskIDs)
	if err != nil {
		return report, err
	}

	organisationIDs := make(map[primitive.ObjectID]struct{})
	projectIDs := make(map[primitive.ObjectID]struct{})
	for _, task := range tasks {
		if id, state := organisationsBackfillStringObjectIDField(task, "organisationId"); state == organisationsBootstrapFieldValue {
			organisationIDs[id] = struct{}{}
		} else if state == organisationsBootstrapFieldEmpty {
			if id, legacyState := organisationsBackfillStringObjectIDField(task, "user_id"); legacyState == organisationsBootstrapFieldValue {
				organisationIDs[id] = struct{}{}
			}
		}
		if id, state := organisationsBootstrapObjectID(task, "projectId"); state == organisationsBootstrapFieldValue {
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
			Code: "scope-organisation-not-found", CanonicalOrganisation: scopeID.Hex(),
			ResolvedOrganisations: []string{scopeID.Hex()}, Message: "requested organisation does not exist",
		})
	}
	resourceName := "case media"
	if adapter.Collection == "case_attachments" {
		resourceName = "case attachment"
	} else if adapter.Collection == "case_shares" {
		resourceName = "case share"
	} else if adapter.Collection == "comments" {
		resourceName = "task comment"
	}
	activeTokens := make(map[string]int64)
	for _, document := range documents {
		outcome := resolveOrganisationsBackfillCaseChild(document, resourceName, tasks, organisations, projects)
		if adapter.Collection == "case_shares" {
			outcome = resolveOrganisationsBackfillCaseShareToken(document, outcome)
		}
		if !organisationsBackfillSiteInScope(outcome.organisationsBackfillProjectResourceOutcome, scopeID) {
			continue
		}
		if token, active := organisationsBackfillActiveCaseShareToken(document); active {
			activeTokens[token]++
		}
		observeOrganisationsBackfillDocument(&resolution, document)
		addOrganisationsBackfillSiteOutcome(&resolution, outcome.organisationsBackfillProjectResourceOutcome)
		if outcome.orphanParent {
			resolution.OrphanParents++
		}
		if config.OrganisationID != "" {
			addOrganisationsBackfillSiteScopedInventory(&report, outcome.organisationsBackfillProjectResourceOutcome)
		}
	}
	for _, count := range activeTokens {
		if count > 1 {
			resolution.DuplicateActiveTokens++
			resolution.DuplicateActiveTokenDocuments += count
			resolution.Conflicts += count
			resolution.ConflictEntries = append(resolution.ConflictEntries, organisationsBackfillConflict{
				Code: "duplicate-active-token", Message: fmt.Sprintf("%d active case shares reuse one token", count),
			})
		}
	}
	sort.Slice(resolution.ConflictEntries, func(left, right int) bool {
		if resolution.ConflictEntries[left].DocumentID != resolution.ConflictEntries[right].DocumentID {
			return resolution.ConflictEntries[left].DocumentID < resolution.ConflictEntries[right].DocumentID
		}
		return resolution.ConflictEntries[left].Code < resolution.ConflictEntries[right].Code
	})
	if len(resolution.ConflictEntries) > organisationsBackfillConflictLimit {
		resolution.ConflictEntries = resolution.ConflictEntries[:organisationsBackfillConflictLimit]
	}
	report.Resolution = &resolution
	report.IndexContracts, err = inspectOrganisationsBackfillCaseChildIndexes(ctx, database.Collection(adapter.Collection), adapter.Collection)
	return report, err
}

func resolveOrganisationsBackfillCaseShareToken(document bson.Raw, outcome organisationsBackfillCaseChildOutcome) organisationsBackfillCaseChildOutcome {
	value := document.Lookup("token")
	if value.Type != bsontype.String || value.StringValue() == "" {
		outcome.addConflict("invalid-share-token", "case share token must be a non-empty string")
		outcome.enrichConflicts()
	}
	return outcome
}

func organisationsBackfillActiveCaseShareToken(document bson.Raw) (string, bool) {
	token := document.Lookup("token")
	active := document.Lookup("is_active")
	if token.Type != bsontype.String || token.StringValue() == "" || active.Type != bsontype.Boolean || !active.Boolean() {
		return "", false
	}
	return token.StringValue(), true
}

func resolveOrganisationsBackfillCaseChild(
	document bson.Raw,
	resourceName string,
	tasks map[primitive.ObjectID]bson.Raw,
	organisations map[primitive.ObjectID]bool,
	projects map[primitive.ObjectID]primitive.ObjectID,
) (outcome organisationsBackfillCaseChildOutcome) {
	child := &outcome.organisationsBackfillProjectResourceOutcome
	child.documentID = organisationsBackfillDocumentID(document)
	defer child.enrichConflicts()

	collectionName := ""
	if resourceName == "case share" {
		collectionName = "case_shares"
	} else if resourceName == "task comment" {
		collectionName = "comments"
	}
	taskID, taskState := organisationsBackfillCaseChildTaskID(document, collectionName)
	if taskState != organisationsBootstrapFieldValue {
		child.addConflict("invalid-parent-task", resourceName+" task_id must be a non-zero BSON ObjectID")
		return outcome
	}
	outcome.taskID = taskID
	parent, exists := tasks[taskID]
	if !exists {
		outcome.orphanParent = true
		child.addConflict("orphan-parent-task", resourceName+" task_id does not resolve to a task")
		return outcome
	}

	parentOutcome := resolveOrganisationsBackfillProjectResource(parent, "task", organisations, projects)
	if len(parentOutcome.conflicts) > 0 || !parentOutcome.projectResolved {
		child.addConflict("parent-ownership-conflict", "parent task ownership is unresolved or conflicting")
		return outcome
	}
	expectedOrganisationID := parentOutcome.canonicalID
	if expectedOrganisationID.IsZero() {
		expectedOrganisationID = parentOutcome.resolvedID
	}
	expectedProjectID := parentOutcome.resolvedProjectID
	child.resolvedID = expectedOrganisationID
	child.resolvedProjectID = expectedProjectID

	canonicalField := "organisation_id"
	if resourceName == "task comment" {
		canonicalField = "organisationId"
	}
	canonicalID, canonicalState := organisationsBackfillStringObjectIDField(document, canonicalField)
	switch canonicalState {
	case organisationsBootstrapFieldValue:
		child.canonicalID = canonicalID
		child.canonicalValid = true
		if canonicalID != expectedOrganisationID {
			child.addConflict("parent-organisation-mismatch", resourceName+" "+canonicalField+" differs from its parent task")
		} else {
			child.resolved = true
		}
	case organisationsBootstrapFieldEmpty:
		child.canonicalMissing = true
		child.resolved = true
		child.proposedWrite = true
	default:
		child.canonicalWrong = true
		child.addConflict("invalid-canonical-organisation", canonicalField+" must contain an ObjectID hex string")
	}

	projectID, projectState := organisationsBootstrapObjectID(document, "projectId")
	switch projectState {
	case organisationsBootstrapFieldValue:
		child.projectPresent = true
		if projectID != expectedProjectID {
			child.addConflict("parent-project-mismatch", resourceName+" projectId differs from its parent task")
		} else {
			child.projectResolved = true
		}
	case organisationsBootstrapFieldEmpty:
		child.projectMissing = true
		child.projectResolved = true
		child.proposedProjectWrite = true
	default:
		child.projectWrong = true
		child.addConflict("invalid-project-id", "projectId must be a non-zero BSON ObjectID or null")
	}
	return outcome
}

func organisationsBackfillCaseChildTaskID(document bson.Raw, collectionName string) (primitive.ObjectID, organisationsBootstrapFieldState) {
	if collectionName == "case_shares" {
		return organisationsBackfillStringObjectIDField(document, "task_id")
	}
	if collectionName == "comments" {
		return organisationsBackfillStringObjectIDField(document, "parent_id")
	}
	return organisationsBootstrapObjectID(document, "task_id")
}

func findOrganisationsBackfillCaseParentTasks(
	ctx context.Context,
	collection *mongo.Collection,
	ids map[primitive.ObjectID]struct{},
) (map[primitive.ObjectID]bson.Raw, error) {
	tasks := make(map[primitive.ObjectID]bson.Raw, len(ids))
	if len(ids) == 0 {
		return tasks, nil
	}
	cursor, err := collection.Find(ctx, bson.M{"_id": bson.M{"$in": sortedOrganisationsBackfillObjectIDs(ids)}}, options.Find().SetProjection(bson.M{
		"_id": 1, "organisationId": 1, "projectId": 1, "user_id": 1,
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
		if id, state := organisationsBootstrapObjectID(document, "_id"); state == organisationsBootstrapFieldValue {
			tasks[id] = document
		}
	}
	return tasks, cursor.Err()
}

func inspectOrganisationsBackfillTaskIndexes(ctx context.Context, collection *mongo.Collection) ([]organisationsBackfillIndexContract, error) {
	return inspectOrganisationsBackfillCaseIndexes(ctx, collection, []organisationsBackfillIndexContract{
		organisationsBackfillNewIndexContract("canonical-project-list", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "creation_date", Value: int32(-1)}, {Key: "_id", Value: int32(-1)}}),
		organisationsBackfillNewIndexContract("legacy-project-list", bson.D{{Key: "user_id", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "creation_date", Value: int32(-1)}, {Key: "_id", Value: int32(-1)}}),
	})
}

func inspectOrganisationsBackfillCaseChildIndexes(ctx context.Context, collection *mongo.Collection, collectionName string) ([]organisationsBackfillIndexContract, error) {
	contracts := []organisationsBackfillIndexContract{}
	if collectionName == "case_media" {
		contracts = append(contracts,
			organisationsBackfillNewIndexContract("project-task-list", bson.D{{Key: "organisation_id", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "task_id", Value: int32(1)}, {Key: "role", Value: int32(1)}, {Key: "created_at", Value: int32(1)}}),
			organisationsBackfillNewIndexContract("project-status-recovery", bson.D{{Key: "organisation_id", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "status", Value: int32(1)}}),
			organisationsBackfillNewIndexContract("source-media-lookup", bson.D{{Key: "source_media_id", Value: int32(1)}}),
		)
	} else if collectionName == "case_shares" {
		contracts = caseShareIndexContracts()
	} else if collectionName == "comments" {
		contracts = []organisationsBackfillIndexContract{
			organisationsBackfillNewIndexContract("project-parent-list", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "parent_id", Value: int32(1)}, {Key: "creation_date", Value: int32(-1)}}),
		}
	} else {
		contracts = append(contracts,
			organisationsBackfillNewIndexContract("project-task-list", bson.D{{Key: "organisation_id", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "task_id", Value: int32(1)}, {Key: "created_at", Value: int32(1)}}),
		)
	}
	return inspectOrganisationsBackfillCaseIndexes(ctx, collection, contracts)
}

func caseShareIndexContracts() []organisationsBackfillIndexContract {
	return []organisationsBackfillIndexContract{
		organisationsBackfillNewIndexContract("project-task-list", bson.D{{Key: "organisation_id", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "task_id", Value: int32(1)}, {Key: "created_at", Value: int32(-1)}}),
		organisationsBackfillNewIndexContract("active-token", bson.D{{Key: "token", Value: int32(1)}, {Key: "is_active", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("legacy-management-rollback", bson.D{{Key: "task_id", Value: int32(1)}, {Key: "user_id", Value: int32(1)}, {Key: "created_at", Value: int32(-1)}}),
	}
}

func inspectOrganisationsBackfillCaseIndexes(ctx context.Context, collection *mongo.Collection, contracts []organisationsBackfillIndexContract) ([]organisationsBackfillIndexContract, error) {
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
