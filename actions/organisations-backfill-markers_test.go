package actions

import (
	"reflect"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestResolveOrganisationsBackfillMarkerOwnership(t *testing.T) {
	organisationA := primitive.NewObjectID()
	organisationB := primitive.NewObjectID()
	projectA := primitive.NewObjectID()
	documentID := primitive.NewObjectID()
	parents := organisationsBackfillMarkerParents{
		byKey: map[string][]organisationsBackfillMarkerParent{
			"media-a.mp4": {{organisationID: organisationA, projectID: projectA, valid: true}},
			"media-b.mp4": {{organisationID: organisationB, projectID: organisationB, valid: true}},
		},
	}
	devices := organisationsBackfillMarkerParents{byKey: map[string][]organisationsBackfillMarkerParent{
		"device-a": {{organisationID: organisationA, projectID: projectA, valid: true}},
		"duplicate-device": {
			{organisationID: organisationA, projectID: projectA, valid: true},
			{organisationID: organisationB, projectID: organisationB, valid: true},
		},
	}}
	organisations := map[primitive.ObjectID]bool{organisationA: true, organisationB: true}
	projects := map[primitive.ObjectID]primitive.ObjectID{projectA: organisationA}

	tests := []struct {
		name     string
		document bson.D
		check    func(*testing.T, organisationsBackfillProjectResourceOutcome)
	}{
		{
			name: "canonical wins over conflicting linked parent",
			document: bson.D{
				{Key: "_id", Value: documentID},
				{Key: "organisationId", Value: organisationA.Hex()},
				{Key: "projectId", Value: projectA},
				{Key: "mediaKeys", Value: bson.A{"media-b.mp4"}},
			},
			check: func(t *testing.T, outcome organisationsBackfillProjectResourceOutcome) {
				if !outcome.canonicalValid || outcome.resolved || !outcome.projectResolved || len(outcome.conflicts) != 0 {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name: "mediaKeys resolve ownership before legacy mediaIds",
			document: bson.D{
				{Key: "_id", Value: documentID},
				{Key: "mediaKeys", Value: bson.A{"media-a.mp4"}},
				{Key: "mediaIds", Value: bson.A{"media-b.mp4"}},
			},
			check: func(t *testing.T, outcome organisationsBackfillProjectResourceOutcome) {
				if !outcome.resolved || outcome.resolvedID != organisationA || !outcome.projectResolved || outcome.resolvedProjectID != projectA || !outcome.proposedWrite || !outcome.proposedProjectWrite || len(outcome.conflicts) != 0 {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:     "legacy mediaIds resolve by recording key",
			document: bson.D{{Key: "_id", Value: documentID}, {Key: "mediaIds", Value: bson.A{"media-a.mp4"}}},
			check: func(t *testing.T, outcome organisationsBackfillProjectResourceOutcome) {
				if !outcome.resolved || outcome.resolvedID != organisationA || outcome.resolvedProjectID != projectA || len(outcome.conflicts) != 0 {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:     "empty mediaKeys fall back to legacy mediaIds",
			document: bson.D{{Key: "_id", Value: documentID}, {Key: "mediaKeys", Value: bson.A{}}, {Key: "mediaIds", Value: bson.A{"media-a.mp4"}}},
			check: func(t *testing.T, outcome organisationsBackfillProjectResourceOutcome) {
				if !outcome.resolved || outcome.resolvedID != organisationA || outcome.resolvedProjectID != projectA || len(outcome.conflicts) != 0 {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:     "unlinked marker resolves through one stored device",
			document: bson.D{{Key: "_id", Value: documentID}, {Key: "deviceId", Value: "device-a"}},
			check: func(t *testing.T, outcome organisationsBackfillProjectResourceOutcome) {
				if !outcome.resolved || outcome.resolvedID != organisationA || !outcome.projectResolved || outcome.resolvedProjectID != projectA || len(outcome.conflicts) != 0 {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:     "unlinked marker rejects ambiguous stored devices",
			document: bson.D{{Key: "_id", Value: documentID}, {Key: "deviceId", Value: "duplicate-device"}},
			check: func(t *testing.T, outcome organisationsBackfillProjectResourceOutcome) {
				if !outcome.multipleCandidates || len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "ambiguous-device" {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:     "orphan linked media conflicts",
			document: bson.D{{Key: "_id", Value: documentID}, {Key: "mediaKeys", Value: bson.A{"missing.mp4"}}},
			check: func(t *testing.T, outcome organisationsBackfillProjectResourceOutcome) {
				if len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "orphan-parent" {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:     "different linked owners are ambiguous",
			document: bson.D{{Key: "_id", Value: documentID}, {Key: "mediaKeys", Value: bson.A{"media-a.mp4", "media-b.mp4"}}},
			check: func(t *testing.T, outcome organisationsBackfillProjectResourceOutcome) {
				if !outcome.multipleCandidates || outcome.zeroCandidate || len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "ambiguous-parent" {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:     "non-unique media key is ambiguous even within one project",
			document: bson.D{{Key: "_id", Value: documentID}, {Key: "mediaKeys", Value: bson.A{"duplicate.mp4"}}},
			check: func(t *testing.T, outcome organisationsBackfillProjectResourceOutcome) {
				if !outcome.multipleCandidates || len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "ambiguous-parent" {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
	}
	parents.byKey["duplicate.mp4"] = []organisationsBackfillMarkerParent{
		{organisationID: organisationA, projectID: projectA, valid: true},
		{organisationID: organisationA, projectID: projectA, valid: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outcome := resolveOrganisationsBackfillMarker(organisationsBackfillTestRaw(t, test.document), parents, devices, organisations, projects)
			test.check(t, outcome)
			for _, conflict := range outcome.conflicts {
				if conflict.LegacyUser != "" || len(conflict.ResolvedOrganisations) > 2 {
					t.Fatalf("conflict exposed media links: %+v", conflict)
				}
			}
		})
	}
}

func TestMarkerParentResolutionUsesCanonicalScope(t *testing.T) {
	organisationID := primitive.NewObjectID()
	projectID := primitive.NewObjectID()
	links := organisationsBackfillMarkerLinks{keys: []string{"recording.mp4"}, usesKeys: true}
	parents := organisationsBackfillMarkerParents{byKey: map[string][]organisationsBackfillMarkerParent{
		"recording.mp4": {
			{organisationID: primitive.NewObjectID(), projectID: primitive.NewObjectID(), valid: true},
			{organisationID: organisationID, projectID: projectID, valid: true},
		},
	}}
	parent, code := organisationsBackfillResolveMarkerParent(links, parents, map[primitive.ObjectID]primitive.ObjectID{projectID: organisationID}, organisationID, projectID, true)
	if code != "" || parent.organisationID != organisationID || parent.projectID != projectID {
		t.Fatalf("parent = %+v code=%q", parent, code)
	}
}

func TestMarkerLookupKeysAddsCanonicalPrefix(t *testing.T) {
	organisationID := primitive.NewObjectID()
	got := organisationsBackfillMarkerLookupKeys([]string{"recording.mp4"}, organisationID)
	want := []string{organisationID.Hex() + "/recording.mp4", "recording.mp4"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("lookup keys = %#v, want %#v", got, want)
	}
}

func TestMarkerNumericIdentityNormalizesEquivalentIntegers(t *testing.T) {
	values := []bson.RawValue{
		organisationsBackfillTestRaw(t, bson.D{{Key: "start", Value: int32(42)}}).Lookup("start"),
		organisationsBackfillTestRaw(t, bson.D{{Key: "start", Value: int64(42)}}).Lookup("start"),
		organisationsBackfillTestRaw(t, bson.D{{Key: "start", Value: float64(42)}}).Lookup("start"),
	}
	for _, value := range values {
		if got, ok := organisationsBackfillMarkerNumericIdentity(value); !ok || got != "42" {
			t.Fatalf("numeric identity = %q ok=%v", got, ok)
		}
	}
}

func TestResolveOrganisationsBackfillCanonicalOnly(t *testing.T) {
	organisationID := primitive.NewObjectID()
	projectID := primitive.NewObjectID()
	organisations := map[primitive.ObjectID]bool{organisationID: true}
	projects := map[primitive.ObjectID]primitive.ObjectID{projectID: organisationID}

	missing := resolveOrganisationsBackfillCanonicalOnly(
		organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "value", Value: "secret"}}),
		"marker option",
		organisations,
		projects,
	)
	if !missing.canonicalMissing || !missing.zeroCandidate || missing.proposedWrite || len(missing.conflicts) != 1 || missing.conflicts[0].Code != "missing-canonical-organisation" {
		t.Fatalf("missing outcome = %+v", missing)
	}

	resolved := resolveOrganisationsBackfillCanonicalOnly(
		organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "organisationId", Value: organisationID.Hex()}, {Key: "projectId", Value: projectID}}),
		"marker option",
		organisations,
		projects,
	)
	if !resolved.canonicalValid || !resolved.projectResolved || resolved.resolvedProjectID != projectID || len(resolved.conflicts) != 0 {
		t.Fatalf("resolved outcome = %+v", resolved)
	}
}

func TestMarkerDeviceFallbackRequiresCanonicalOwnership(t *testing.T) {
	document := organisationsBackfillTestRaw(t, bson.D{
		{Key: "key", Value: "device-a"},
		{Key: "user_id", Value: primitive.NewObjectID().Hex()},
	})
	devices := organisationsBackfillMarkerParents{byKey: map[string][]organisationsBackfillMarkerParent{}}
	organisationID, state := organisationsBackfillStringObjectIDField(document, "organisationId")
	if state != organisationsBootstrapFieldEmpty || !organisationID.IsZero() {
		t.Fatalf("canonical organisation = %s state=%v", organisationID.Hex(), state)
	}
	devices.byKey["device-a"] = []organisationsBackfillMarkerParent{{valid: false}}
	marker := organisationsBackfillTestRaw(t, bson.D{{Key: "deviceId", Value: "device-a"}})
	if _, code := organisationsBackfillResolveMarkerDevice(marker, devices, nil); code != "unresolved-device-ownership" {
		t.Fatalf("device fallback code = %q, want unresolved-device-ownership", code)
	}
}

func TestMarkerAdaptersAndIndexContracts(t *testing.T) {
	registry := organisationsBackfillAdapters()
	optionCollections := []string{"marker_options", "marker_tag_options", "marker_event_options", "marker_category_options"}
	for _, collection := range optionCollections {
		adapter := registry[collection]
		if adapter.ResolverKind != organisationsBackfillResolverMarkerCanonical || adapter.OwnershipScope != "project-scoped" || len(adapter.LegacyCandidates) != 0 {
			t.Fatalf("option adapter %q = %+v", collection, adapter)
		}
	}
	rangeCollections := []string{"marker_option_ranges", "marker_tag_option_ranges", "marker_event_option_ranges"}
	for _, collection := range rangeCollections {
		adapter := registry[collection]
		if adapter.ResolverKind != organisationsBackfillResolverMarkerCanonical || adapter.LifecycleStatus != "active" || adapter.OperationalUse != "derived-query-projection" {
			t.Fatalf("range adapter %q = %+v", collection, adapter)
		}
	}

	wantMarker := []organisationsBackfillIndexContract{
		organisationsBackfillNewIndexContract("project-time-list", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "startTimestamp", Value: int32(-1)}, {Key: "_id", Value: int32(-1)}}),
		organisationsBackfillNewIndexContract("project-device-name-time", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "deviceId", Value: int32(1)}, {Key: "name", Value: int32(1)}, {Key: "startTimestamp", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("project-media-keys", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "mediaKeys", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("project-device-time", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "deviceId", Value: int32(1)}, {Key: "startTimestamp", Value: int32(1)}}),
	}
	if got := organisationsBackfillMarkerIndexContracts(); !reflect.DeepEqual(got, wantMarker) {
		t.Fatalf("marker contracts = %#v, want %#v", got, wantMarker)
	}
	optionContracts := organisationsBackfillMarkerCanonicalIndexContracts("marker_options")
	if len(optionContracts) != 2 || optionContracts[0].Name != "project-value-unique" || optionContracts[1].Name != "project-updated-list" {
		t.Fatalf("option contracts = %#v", optionContracts)
	}
	rangeContracts := organisationsBackfillMarkerCanonicalIndexContracts("marker_option_ranges")
	if len(rangeContracts) != 4 || rangeContracts[0].Keys[2].Field != "text" || rangeContracts[1].Name != "project-value-device-start-unique" || !rangeContracts[1].Unique || !reflect.DeepEqual(rangeContracts[1].PartialFilterExpression, markerRangePartialFilter()) || rangeContracts[2].Keys[2].Field != "value" || rangeContracts[2].Keys[3].Field != "deviceKey" {
		t.Fatalf("range contracts = %#v", rangeContracts)
	}
}

func TestMarkerOptionDuplicateReportRedactsValues(t *testing.T) {
	organisationID := primitive.NewObjectID()
	documents := []organisationsBackfillMarkerResolvedDocument{
		{
			document: organisationsBackfillTestRaw(t, bson.D{{Key: "value", Value: "sensitive-value"}}),
			outcome:  organisationsBackfillProjectResourceOutcome{canonicalID: organisationID, canonicalValid: true, resolvedProjectID: organisationID, projectResolved: true},
		},
		{
			document: organisationsBackfillTestRaw(t, bson.D{{Key: "value", Value: "sensitive-value"}}),
			outcome:  organisationsBackfillProjectResourceOutcome{canonicalID: organisationID, canonicalValid: true, resolvedProjectID: organisationID, projectResolved: true},
		},
	}
	report := newOrganisationsBackfillResolution()
	addOrganisationsBackfillMarkerOptionDuplicates(&report, documents)
	if report.Conflicts != 1 || report.MultipleCandidates != 2 || len(report.ConflictEntries) != 1 {
		t.Fatalf("duplicate report = %+v", report)
	}
	conflict := report.ConflictEntries[0]
	if conflict.DocumentID != "" || conflict.CanonicalOrganisation != "" || len(conflict.ResolvedOrganisations) != 0 || strings.Contains(conflict.Message, "sensitive-value") {
		t.Fatalf("duplicate conflict exposed value or identity: %+v", conflict)
	}
}

func TestMarkerRangeDuplicateReportRedactsIdentity(t *testing.T) {
	organisationID := primitive.NewObjectID()
	projectID := primitive.NewObjectID()
	document := bson.D{{Key: "value", Value: "sensitive-value"}, {Key: "deviceId", Value: "sensitive-device"}, {Key: "start", Value: int64(42)}}
	documents := []organisationsBackfillMarkerResolvedDocument{
		{document: organisationsBackfillTestRaw(t, document), outcome: organisationsBackfillProjectResourceOutcome{canonicalID: organisationID, canonicalValid: true, resolvedProjectID: projectID, projectResolved: true}},
		{document: organisationsBackfillTestRaw(t, document), outcome: organisationsBackfillProjectResourceOutcome{canonicalID: organisationID, canonicalValid: true, resolvedProjectID: projectID, projectResolved: true}},
	}
	report := newOrganisationsBackfillResolution()
	addOrganisationsBackfillMarkerRangeDuplicates(&report, documents)
	if report.Conflicts != 1 || report.MultipleCandidates != 2 || len(report.ConflictEntries) != 1 {
		t.Fatalf("duplicate report = %+v", report)
	}
	conflict := report.ConflictEntries[0]
	if strings.Contains(conflict.Message, "sensitive-value") || strings.Contains(conflict.Message, "sensitive-device") {
		t.Fatalf("duplicate conflict exposed range identity: %+v", conflict)
	}
}
