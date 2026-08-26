package actions

import (
	"context"
	"sort"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type organisationsBackfillAuditEventResolution struct {
	Scanned                   int64                                `json:"scanned"`
	OrganisationOnly          int64                                `json:"organisationOnly"`
	ProjectContextual         int64                                `json:"projectContextual"`
	ProjectContextResolved    int64                                `json:"projectContextResolved"`
	UnresolvedTargets         int64                                `json:"unresolvedTargets"`
	CanonicalTargetMismatches int64                                `json:"canonicalTargetMismatches"`
	ProposedProjectWrites     int64                                `json:"proposedProjectWrites"`
	Conflicts                 int64                                `json:"conflicts"`
	ObservedFieldTypes        map[string]map[string]int64          `json:"observedFieldTypes"`
	ObservedShapes            map[string]int64                     `json:"observedShapes"`
	ConflictEntries           []organisationsBackfillAuditConflict `json:"conflictEntries,omitempty"`
}

type organisationsBackfillAuditConflict struct {
	Code       string `json:"code"`
	DocumentID string `json:"documentId,omitempty"`
	TargetType string `json:"targetType,omitempty"`
	TargetID   string `json:"targetId,omitempty"`
	Message    string `json:"message"`
}

type organisationsBackfillAuditTarget struct {
	collection string
	idFields   []string
}

var organisationsBackfillAuditTargets = map[string]organisationsBackfillAuditTarget{
	"case":         {collection: "tasks", idFields: []string{"_id"}},
	"device":       {collection: "devices", idFields: []string{"_id", "key"}},
	"media":        {collection: "media", idFields: []string{"key"}},
	"notification": {collection: "notifications", idFields: []string{"_id"}},
	"workflow":     {collection: "workflows", idFields: []string{"_id"}},
}

var organisationsBackfillOrganisationOnlyAuditTargets = map[string]bool{
	"organisation": true,
	"member":       true,
	"membership":   true,
	"role":         true,
	"subscription": true,
	"user":         true,
}

func inspectOrganisationsBackfillAuditEvents(ctx context.Context, database *mongo.Database, adapter organisationsBackfillAdapter, config OrganisationsBackfillConfig, report organisationsBackfillCollection) (organisationsBackfillCollection, error) {
	documents, err := findOrganisationsBackfillAuditDocuments(ctx, database.Collection(adapter.Collection), config)
	if err != nil {
		return report, err
	}
	resolution := organisationsBackfillAuditEventResolution{ObservedFieldTypes: map[string]map[string]int64{}, ObservedShapes: map[string]int64{}}
	for _, document := range documents {
		observeOrganisationsBackfillAuditDocument(&resolution, document)
		resolveOrganisationsBackfillAuditEvent(ctx, database, document, &resolution)
	}
	sort.Slice(resolution.ConflictEntries, func(i, j int) bool {
		if resolution.ConflictEntries[i].DocumentID != resolution.ConflictEntries[j].DocumentID {
			return resolution.ConflictEntries[i].DocumentID < resolution.ConflictEntries[j].DocumentID
		}
		return resolution.ConflictEntries[i].Code < resolution.ConflictEntries[j].Code
	})
	if len(resolution.ConflictEntries) > organisationsBackfillConflictLimit {
		resolution.ConflictEntries = resolution.ConflictEntries[:organisationsBackfillConflictLimit]
	}
	report.AuditEventResolution = &resolution
	report.IndexContracts, err = inspectOrganisationsBackfillAuditEventIndexes(ctx, database.Collection(adapter.Collection))
	return report, err
}

func findOrganisationsBackfillAuditDocuments(ctx context.Context, collection *mongo.Collection, config OrganisationsBackfillConfig) ([]bson.Raw, error) {
	filter := bson.M{}
	if config.DocumentID != "" {
		filter["_id"], _ = primitive.ObjectIDFromHex(config.DocumentID)
	}
	if config.OrganisationID != "" {
		filter["organisationId"], _ = primitive.ObjectIDFromHex(config.OrganisationID)
	}
	cursor, err := collection.Find(ctx, filter, options.Find().SetBatchSize(int32(config.BatchSize)))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var documents []bson.Raw
	for cursor.Next(ctx) {
		documents = append(documents, append(bson.Raw(nil), cursor.Current...))
	}
	return documents, cursor.Err()
}

func resolveOrganisationsBackfillAuditEvent(ctx context.Context, database *mongo.Database, document bson.Raw, resolution *organisationsBackfillAuditEventResolution) {
	resolution.Scanned++
	documentID := organisationsBackfillDocumentID(document)
	targetType := auditString(document, "targetType")
	targetID := auditString(document, "targetId")
	action := auditString(document, "action")
	eventOrganisation, organisationState := organisationsBootstrapObjectID(document, "organisationId")
	if organisationState != organisationsBootstrapFieldValue {
		addOrganisationsBackfillAuditConflict(resolution, "invalid-canonical-organisation", documentID, targetType, targetID, "organisationId must be a non-zero BSON ObjectID; actorId is never ownership authority")
		return
	}
	if organisationsBackfillOrganisationOnlyAuditTargets[targetType] && strings.HasPrefix(action, targetType+".") {
		resolution.OrganisationOnly++
		return
	}
	target, contextual := organisationsBackfillAuditTargets[targetType]
	if !contextual || !strings.HasPrefix(action, targetType+".") {
		addOrganisationsBackfillAuditConflict(resolution, "unclassified-event", documentID, targetType, targetID, "action and targetType are not a known conservative audit classification")
		return
	}
	resolution.ProjectContextual++
	targetDocument, err := findOrganisationsBackfillAuditTarget(ctx, database.Collection(target.collection), target, targetID)
	if err != nil || targetDocument == nil {
		resolution.UnresolvedTargets++
		addOrganisationsBackfillAuditConflict(resolution, "unresolved-target", documentID, targetType, targetID, "authoritative target is missing, deleted, or malformed")
		return
	}
	targetOrganisation, targetProject, ok := organisationsBackfillAuditTargetOwnership(targetDocument)
	if targetType == "notification" {
		sourceOrganisation, sourceProject, sourceOK := resolveOrganisationsBackfillNotificationSource(ctx, database, targetDocument)
		if ok && sourceOK && (targetOrganisation != sourceOrganisation || targetProject != sourceProject) {
			resolution.CanonicalTargetMismatches++
			addOrganisationsBackfillAuditConflict(resolution, "target-source-mismatch", documentID, targetType, targetID, "canonical notification ownership disagrees with its authoritative media source")
			return
		}
		if !ok && sourceOK {
			targetOrganisation, targetProject, ok = sourceOrganisation, sourceProject, true
		}
	}
	if !ok {
		resolution.UnresolvedTargets++
		addOrganisationsBackfillAuditConflict(resolution, "unresolved-target-ownership", documentID, targetType, targetID, "target or source lacks valid canonical organisation and project ownership")
		return
	}
	if eventOrganisation != targetOrganisation {
		resolution.CanonicalTargetMismatches++
		addOrganisationsBackfillAuditConflict(resolution, "canonical-target-mismatch", documentID, targetType, targetID, "event organisationId does not agree with authoritative target ownership")
		return
	}
	metadataProject, metadataState := organisationsBackfillAuditMetadataProject(document)
	if metadataState == organisationsBootstrapFieldWrong || (metadataState == organisationsBootstrapFieldValue && metadataProject != targetProject) {
		addOrganisationsBackfillAuditConflict(resolution, "metadata-project-mismatch", documentID, targetType, targetID, "metadata.projectId is invalid or disagrees with authoritative target ownership")
		return
	}
	eventProject, projectState := organisationsBootstrapObjectID(document, "projectId")
	if projectState == organisationsBootstrapFieldValue && eventProject != targetProject {
		resolution.CanonicalTargetMismatches++
		addOrganisationsBackfillAuditConflict(resolution, "canonical-project-mismatch", documentID, targetType, targetID, "event projectId disagrees with authoritative target ownership")
		return
	}
	if projectState == organisationsBootstrapFieldWrong {
		addOrganisationsBackfillAuditConflict(resolution, "invalid-project-id", documentID, targetType, targetID, "projectId must be a non-zero BSON ObjectID or absent")
		return
	}
	resolution.ProjectContextResolved++
	if projectState == organisationsBootstrapFieldEmpty {
		resolution.ProposedProjectWrites++
	}
}

func auditString(document bson.Raw, field string) string {
	value := document.Lookup(field)
	if value.Type != bson.TypeString {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(value.StringValue()))
}

func findOrganisationsBackfillAuditTarget(ctx context.Context, collection *mongo.Collection, target organisationsBackfillAuditTarget, targetID string) (bson.Raw, error) {
	if targetID == "" {
		return nil, nil
	}
	filters := make(bson.A, 0, len(target.idFields))
	for _, field := range target.idFields {
		if field == "_id" {
			objectID, err := primitive.ObjectIDFromHex(targetID)
			if err == nil {
				filters = append(filters, bson.M{field: objectID})
			}
			continue
		}
		filters = append(filters, bson.M{field: targetID})
	}
	if len(filters) == 0 {
		return nil, nil
	}
	filter := bson.M{"$or": filters}
	if len(filters) == 1 {
		filter = filters[0].(bson.M)
	}
	var document bson.Raw
	err := collection.FindOne(ctx, filter).Decode(&document)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	return document, err
}

func organisationsBackfillAuditTargetOwnership(document bson.Raw) (primitive.ObjectID, primitive.ObjectID, bool) {
	organisationID, organisationState := organisationsBackfillStringObjectIDField(document, "organisationId")
	if organisationState != organisationsBootstrapFieldValue {
		organisationID, organisationState = organisationsBackfillStringObjectIDField(document, "organisation_id")
	}
	projectID, projectState := organisationsBootstrapObjectID(document, "projectId")
	return organisationID, projectID, organisationState == organisationsBootstrapFieldValue && projectState == organisationsBootstrapFieldValue
}

func resolveOrganisationsBackfillNotificationSource(ctx context.Context, database *mongo.Database, notification bson.Raw) (primitive.ObjectID, primitive.ObjectID, bool) {
	mediaKey := rawString(notification, "media_key")
	if mediaKey == "" {
		return primitive.NilObjectID, primitive.NilObjectID, false
	}
	media, err := findOrganisationsBackfillAuditTarget(ctx, database.Collection("media"), organisationsBackfillAuditTargets["media"], mediaKey)
	if err != nil || media == nil {
		return primitive.NilObjectID, primitive.NilObjectID, false
	}
	return organisationsBackfillAuditTargetOwnership(media)
}

func rawString(document bson.Raw, field string) string {
	value := document.Lookup(field)
	if value.Type != bson.TypeString {
		return ""
	}
	return strings.TrimSpace(value.StringValue())
}

func organisationsBackfillAuditMetadataProject(document bson.Raw) (primitive.ObjectID, organisationsBootstrapFieldState) {
	value := document.Lookup("metadata", "projectId")
	if value.Type == 0 || value.Type == bson.TypeNull || (value.Type == bson.TypeString && strings.TrimSpace(value.StringValue()) == "") {
		return primitive.NilObjectID, organisationsBootstrapFieldEmpty
	}
	if value.Type != bson.TypeString {
		return primitive.NilObjectID, organisationsBootstrapFieldWrong
	}
	id, err := primitive.ObjectIDFromHex(value.StringValue())
	if err != nil || id.IsZero() {
		return primitive.NilObjectID, organisationsBootstrapFieldWrong
	}
	return id, organisationsBootstrapFieldValue
}

func addOrganisationsBackfillAuditConflict(resolution *organisationsBackfillAuditEventResolution, code, documentID, targetType, targetID, message string) {
	resolution.Conflicts++
	resolution.ConflictEntries = append(resolution.ConflictEntries, organisationsBackfillAuditConflict{Code: code, DocumentID: documentID, TargetType: targetType, TargetID: targetID, Message: message})
}

func observeOrganisationsBackfillAuditDocument(resolution *organisationsBackfillAuditEventResolution, document bson.Raw) {
	fields := []string{"organisationId", "projectId", "actorId", "action", "targetType", "targetId", "metadata", "metadata.projectId", "timestamp"}
	shape := make([]string, 0, len(fields))
	for _, field := range fields {
		value := document.Lookup(strings.Split(field, ".")...)
		typeName := value.Type.String()
		if value.Type == 0 {
			typeName = "missing"
		} else {
			shape = append(shape, field+":"+typeName)
		}
		if resolution.ObservedFieldTypes[field] == nil {
			resolution.ObservedFieldTypes[field] = map[string]int64{}
		}
		resolution.ObservedFieldTypes[field][typeName]++
	}
	sort.Strings(shape)
	resolution.ObservedShapes[strings.Join(shape, ",")]++
}

func inspectOrganisationsBackfillAuditEventIndexes(ctx context.Context, collection *mongo.Collection) ([]organisationsBackfillIndexContract, error) {
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
		organisationsBackfillNewIndexContract("organisation-timeline", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "timestamp", Value: int32(-1)}}),
		organisationsBackfillNewIndexContract("organisation-actor-timeline", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "actorId", Value: int32(1)}, {Key: "timestamp", Value: int32(-1)}}),
		organisationsBackfillNewIndexContract("organisation-target-timeline", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "targetType", Value: int32(1)}, {Key: "targetId", Value: int32(1)}, {Key: "timestamp", Value: int32(-1)}}),
		organisationsBackfillNewIndexContract("organisation-project-timeline", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "timestamp", Value: int32(-1)}}),
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
