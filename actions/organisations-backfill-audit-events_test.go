package actions

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestAuditEventsAdapterContract(t *testing.T) {
	adapter := organisationsBackfillAdapters()["audit_events"]
	if adapter.TargetField != "organisationId" || adapter.TargetBSONType != "objectId" || adapter.ProjectBSONType != "objectId" {
		t.Fatalf("adapter = %+v", adapter)
	}
	if len(adapter.LegacyCandidates) != 0 || len(adapter.MinimumWriterVersions) != 3 {
		t.Fatalf("adapter ownership/writers = %+v", adapter)
	}
}

func TestAuditEventTargetOwnershipRequiresCanonicalPair(t *testing.T) {
	organisationID := primitive.NewObjectID()
	projectID := primitive.NewObjectID()
	document := organisationsBackfillTestRaw(t, bson.D{
		{Key: "organisationId", Value: organisationID.Hex()},
		{Key: "projectId", Value: projectID},
		{Key: "user_id", Value: primitive.NewObjectID().Hex()},
	})
	resolvedOrganisation, resolvedProject, ok := organisationsBackfillAuditTargetOwnership(document)
	if !ok || resolvedOrganisation != organisationID || resolvedProject != projectID {
		t.Fatalf("ownership = %s/%s, ok=%v", resolvedOrganisation.Hex(), resolvedProject.Hex(), ok)
	}

	withoutProject := organisationsBackfillTestRaw(t, bson.D{{Key: "organisationId", Value: organisationID.Hex()}})
	if _, _, ok := organisationsBackfillAuditTargetOwnership(withoutProject); ok {
		t.Fatal("target without canonical project unexpectedly resolved")
	}
}

func TestAuditEventTargetIDPreservesCase(t *testing.T) {
	document := organisationsBackfillTestRaw(t, bson.D{{Key: "targetId", Value: "Camera/ABC-123"}})
	if got := rawString(document, "targetId"); got != "Camera/ABC-123" {
		t.Fatalf("targetId = %q", got)
	}
}

func TestResolveOrganisationsBackfillAuditEventDoesNotUseActorOwnership(t *testing.T) {
	resolution := organisationsBackfillAuditEventResolution{ObservedFieldTypes: map[string]map[string]int64{}, ObservedShapes: map[string]int64{}}
	resolveOrganisationsBackfillAuditEvent(context.Background(), nil, organisationsBackfillTestRaw(t, bson.D{
		{Key: "actorId", Value: primitive.NewObjectID()},
		{Key: "action", Value: "organisation.updated"},
		{Key: "targetType", Value: "organisation"},
	}), &resolution)
	if resolution.Conflicts != 1 || resolution.ConflictEntries[0].Code != "invalid-canonical-organisation" || resolution.OrganisationOnly != 0 {
		t.Fatalf("resolution = %+v", resolution)
	}
}

func TestInspectOrganisationsBackfillAuditEventsIsReadOnly(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("project contextual case", func(mt *mtest.T) {
		eventID := primitive.NewObjectID()
		caseID := primitive.NewObjectID()
		organisationID := primitive.NewObjectID()
		projectID := primitive.NewObjectID()
		auditNamespace := mt.DB.Name() + ".audit_events"
		tasksNamespace := mt.DB.Name() + ".tasks"
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, auditNamespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: eventID},
				{Key: "organisationId", Value: organisationID},
				{Key: "actorId", Value: primitive.NewObjectID()},
				{Key: "action", Value: "case.updated"},
				{Key: "targetType", Value: "case"},
				{Key: "targetId", Value: caseID.Hex()},
				{Key: "metadata", Value: bson.D{{Key: "projectId", Value: projectID.Hex()}}},
			}),
			mtest.CreateCursorResponse(0, tasksNamespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: caseID},
				{Key: "organisationId", Value: organisationID.Hex()},
				{Key: "projectId", Value: projectID},
			}),
			mtest.CreateCursorResponse(0, auditNamespace, mtest.FirstBatch,
				bson.D{{Key: "name", Value: "audit_event_org_timestamp"}, {Key: "key", Value: bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "timestamp", Value: int32(-1)}}}},
			),
		)

		report, err := inspectOrganisationsBackfillAuditEvents(context.Background(), mt.DB, organisationsBackfillAdapters()["audit_events"], OrganisationsBackfillConfig{BatchSize: 500}, organisationsBackfillCollection{})
		if err != nil {
			mt.Fatalf("inspectOrganisationsBackfillAuditEvents() error = %v", err)
		}
		resolution := report.AuditEventResolution
		if resolution == nil || resolution.ProjectContextResolved != 1 || resolution.ProposedProjectWrites != 1 || resolution.Conflicts != 0 {
			mt.Fatalf("resolution = %+v", resolution)
		}
		if len(report.IndexContracts) != 4 || report.IndexContracts[0].Status != "exact" {
			mt.Fatalf("index contracts = %+v", report.IndexContracts)
		}
		for _, event := range mt.GetAllStartedEvents() {
			if event.CommandName != "find" && event.CommandName != "listIndexes" {
				mt.Fatalf("audit event dry-run issued non-read command %q", event.CommandName)
			}
		}
	})
}

func TestInspectOrganisationsBackfillAuditEventsReportsDeletedTarget(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("deleted workflow", func(mt *mtest.T) {
		auditNamespace := mt.DB.Name() + ".audit_events"
		workflowsNamespace := mt.DB.Name() + ".workflows"
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, auditNamespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: primitive.NewObjectID()},
				{Key: "organisationId", Value: primitive.NewObjectID()},
				{Key: "action", Value: "workflow.deleted"},
				{Key: "targetType", Value: "workflow"},
				{Key: "targetId", Value: primitive.NewObjectID().Hex()},
			}),
			mtest.CreateCursorResponse(0, workflowsNamespace, mtest.FirstBatch),
			mtest.CreateCursorResponse(0, auditNamespace, mtest.FirstBatch),
		)
		report, err := inspectOrganisationsBackfillAuditEvents(context.Background(), mt.DB, organisationsBackfillAdapters()["audit_events"], OrganisationsBackfillConfig{BatchSize: 500}, organisationsBackfillCollection{})
		if err != nil {
			mt.Fatalf("inspectOrganisationsBackfillAuditEvents() error = %v", err)
		}
		resolution := report.AuditEventResolution
		if resolution.UnresolvedTargets != 1 || resolution.Conflicts != 1 || resolution.ConflictEntries[0].Code != "unresolved-target" {
			mt.Fatalf("resolution = %+v", resolution)
		}
	})
}
