package actions

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestResolveOrganisationsBackfillSubscription(t *testing.T) {
	organisationA := primitive.NewObjectID()
	organisationB := primitive.NewObjectID()
	legacyParent := primitive.NewObjectID()
	userID := primitive.NewObjectID()
	documentID := primitive.NewObjectID()

	tests := []struct {
		name          string
		document      bson.D
		users         map[primitive.ObjectID]bson.Raw
		organisations map[primitive.ObjectID]bool
		check         func(*testing.T, organisationsBackfillSubscriptionOutcome)
	}{
		{
			name: "canonical ownership ignores payer organisation",
			document: bson.D{
				{Key: "_id", Value: documentID},
				{Key: "organisation_id", Value: organisationA},
				{Key: "user_id", Value: userID.Hex()},
			},
			users: map[primitive.ObjectID]bson.Raw{
				userID: organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: userID}, {Key: "organisationId", Value: organisationB}}),
			},
			organisations: map[primitive.ObjectID]bool{organisationA: true, organisationB: true},
			check: func(t *testing.T, outcome organisationsBackfillSubscriptionOutcome) {
				if !outcome.canonicalValid || outcome.resolved || len(outcome.conflicts) != 0 || outcome.proposedWrite {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name: "canonical ownership wins over stale legacy payer",
			document: bson.D{
				{Key: "_id", Value: documentID},
				{Key: "organisation_id", Value: organisationA},
				{Key: "user_id", Value: userID.Hex()},
			},
			users: map[primitive.ObjectID]bson.Raw{
				userID: organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: userID}, {Key: "organisationId", Value: organisationB}}),
			},
			organisations: map[primitive.ObjectID]bool{organisationA: true, organisationB: true},
			check: func(t *testing.T, outcome organisationsBackfillSubscriptionOutcome) {
				if !outcome.canonicalValid || outcome.resolved || len(outcome.conflicts) != 0 {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:     "canonical missing resolves own id despite active selection",
			document: bson.D{{Key: "_id", Value: documentID}, {Key: "user_id", Value: userID.Hex()}},
			users: map[primitive.ObjectID]bson.Raw{
				userID: organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: userID}, {Key: "organisationId", Value: organisationB}}),
			},
			organisations: map[primitive.ObjectID]bool{userID: true, organisationB: true},
			check: func(t *testing.T, outcome organisationsBackfillSubscriptionOutcome) {
				if !outcome.canonicalMissing || outcome.resolvedID != userID || !outcome.resolved || !outcome.proposedWrite {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:     "canonical missing resolves legacy parent",
			document: bson.D{{Key: "_id", Value: documentID}, {Key: "user_id", Value: userID.Hex()}},
			users: map[primitive.ObjectID]bson.Raw{
				userID: organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: userID}, {Key: "user_id", Value: legacyParent.Hex()}}),
			},
			organisations: map[primitive.ObjectID]bool{legacyParent: true},
			check: func(t *testing.T, outcome organisationsBackfillSubscriptionOutcome) {
				if outcome.resolvedID != legacyParent || !outcome.resolved || !outcome.proposedWrite {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:     "canonical missing resolves own id for master",
			document: bson.D{{Key: "_id", Value: documentID}, {Key: "user_id", Value: userID.Hex()}},
			users: map[primitive.ObjectID]bson.Raw{
				userID: organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: userID}}),
			},
			organisations: map[primitive.ObjectID]bool{userID: true},
			check: func(t *testing.T, outcome organisationsBackfillSubscriptionOutcome) {
				if outcome.resolvedID != userID || !outcome.resolved || !outcome.proposedWrite {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:          "zero canonical object id is invalid",
			document:      bson.D{{Key: "_id", Value: documentID}, {Key: "organisation_id", Value: primitive.NilObjectID}},
			organisations: map[primitive.ObjectID]bool{},
			check: func(t *testing.T, outcome organisationsBackfillSubscriptionOutcome) {
				if outcome.canonicalValid || outcome.canonicalMissing || !outcome.canonicalWrong || len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "invalid-canonical-organisation" {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:          "invalid legacy user id",
			document:      bson.D{{Key: "_id", Value: documentID}, {Key: "user_id", Value: "invalid"}},
			organisations: map[primitive.ObjectID]bool{},
			check: func(t *testing.T, outcome organisationsBackfillSubscriptionOutcome) {
				if !outcome.invalidLegacy || len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "invalid-legacy-user-id" {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:          "orphan user",
			document:      bson.D{{Key: "_id", Value: documentID}, {Key: "user_id", Value: userID.Hex()}},
			users:         map[primitive.ObjectID]bson.Raw{},
			organisations: map[primitive.ObjectID]bool{},
			check: func(t *testing.T, outcome organisationsBackfillSubscriptionOutcome) {
				if !outcome.orphanUser || len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "orphan-user" {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:     "orphan organisation",
			document: bson.D{{Key: "_id", Value: documentID}, {Key: "user_id", Value: userID.Hex()}},
			users: map[primitive.ObjectID]bson.Raw{
				userID: organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: userID}, {Key: "organisationId", Value: organisationB}}),
			},
			organisations: map[primitive.ObjectID]bool{},
			check: func(t *testing.T, outcome organisationsBackfillSubscriptionOutcome) {
				if !outcome.orphanOrganisation || len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "orphan-organisation" {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
		{
			name:          "no candidate",
			document:      bson.D{{Key: "_id", Value: documentID}},
			organisations: map[primitive.ObjectID]bool{},
			check: func(t *testing.T, outcome organisationsBackfillSubscriptionOutcome) {
				if !outcome.canonicalMissing || !outcome.zeroCandidate || len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "zero-candidate" {
					t.Fatalf("outcome = %+v", outcome)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outcome := resolveOrganisationsBackfillSubscription(
				organisationsBackfillTestRaw(t, test.document),
				test.users,
				test.organisations,
			)
			test.check(t, outcome)
		})
	}
}

func TestOrganisationsBackfillSubscriptionScopeUsesResolvedUserOrganisation(t *testing.T) {
	requestedOrganisation := primitive.NewObjectID()
	otherOrganisation := primitive.NewObjectID()
	subUserID := primitive.NewObjectID()
	otherUserID := primitive.NewObjectID()
	organisations := map[primitive.ObjectID]bool{requestedOrganisation: true, otherOrganisation: true}
	users := map[primitive.ObjectID]bson.Raw{
		subUserID:   organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: subUserID}, {Key: "user_id", Value: requestedOrganisation.Hex()}}),
		otherUserID: organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: otherUserID}, {Key: "user_id", Value: requestedOrganisation.Hex()}}),
	}
	documents := []bson.Raw{
		organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "user_id", Value: subUserID.Hex()}}),
		organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "organisation_id", Value: requestedOrganisation}}),
		organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "organisation_id", Value: otherOrganisation}, {Key: "user_id", Value: otherUserID.Hex()}}),
		organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "organisation_id", Value: otherOrganisation}}),
	}

	report := organisationsBackfillSubscriptionResolution{}
	for _, document := range documents {
		outcome := resolveOrganisationsBackfillSubscription(document, users, organisations)
		if organisationsBackfillSubscriptionInScope(outcome, requestedOrganisation) {
			addOrganisationsBackfillSubscriptionOutcome(&report, outcome)
		}
	}

	if subUserID == requestedOrganisation {
		t.Fatal("test requires the legacy user and organisation IDs to differ")
	}
	if report.Scanned != 2 || report.CanonicalValid != 1 || report.CanonicalMissing != 1 || report.Resolved != 1 || report.ProposedWrites != 1 || report.Conflicts != 0 {
		t.Fatalf("scoped report = %+v", report)
	}
}

func TestOrganisationsBackfillSubscriptionScopeIncludesMissingRequestedOrganisation(t *testing.T) {
	requestedOrganisation := primitive.NewObjectID()
	userID := primitive.NewObjectID()
	document := organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "user_id", Value: userID.Hex()}})
	users := map[primitive.ObjectID]bson.Raw{
		userID: organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: userID}, {Key: "user_id", Value: requestedOrganisation.Hex()}}),
	}

	outcome := resolveOrganisationsBackfillSubscription(document, users, map[primitive.ObjectID]bool{})
	if !organisationsBackfillSubscriptionInScope(outcome, requestedOrganisation) {
		t.Fatal("scoped selection excluded a relevant missing organisation")
	}
	if !outcome.orphanOrganisation || len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "orphan-organisation" {
		t.Fatalf("outcome = %+v", outcome)
	}
}

func TestOrganisationsBackfillSubscriptionScopeIncludesWrongCanonicalTypeResolvedToScope(t *testing.T) {
	requestedOrganisation := primitive.NewObjectID()
	userID := primitive.NewObjectID()
	document := organisationsBackfillTestRaw(t, bson.D{
		{Key: "_id", Value: primitive.NewObjectID()},
		{Key: "organisation_id", Value: requestedOrganisation.Hex()},
		{Key: "user_id", Value: userID.Hex()},
	})
	users := map[primitive.ObjectID]bson.Raw{
		userID: organisationsBackfillTestRaw(t, bson.D{{Key: "_id", Value: userID}, {Key: "user_id", Value: requestedOrganisation.Hex()}}),
	}
	outcome := resolveOrganisationsBackfillSubscription(document, users, map[primitive.ObjectID]bool{requestedOrganisation: true})
	if !organisationsBackfillSubscriptionInScope(outcome, requestedOrganisation) {
		t.Fatal("scoped selection excluded a wrong-type canonical conflict resolved to the requested organisation")
	}
	if !outcome.canonicalWrong || len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "invalid-canonical-organisation" {
		t.Fatalf("outcome = %+v", outcome)
	}
}

func TestCanonicalSubscriptionReportsMalformedPayerWithoutReinterpretingOwnership(t *testing.T) {
	organisationID := primitive.NewObjectID()
	document := organisationsBackfillTestRaw(t, bson.D{
		{Key: "_id", Value: primitive.NewObjectID()},
		{Key: "organisation_id", Value: organisationID},
		{Key: "user_id", Value: "invalid"},
	})
	outcome := resolveOrganisationsBackfillSubscription(document, nil, map[primitive.ObjectID]bool{organisationID: true})
	if !outcome.canonicalValid || outcome.resolved || !outcome.invalidLegacy {
		t.Fatalf("outcome = %+v", outcome)
	}
	if len(outcome.conflicts) != 1 || outcome.conflicts[0].Code != "invalid-legacy-user-id" {
		t.Fatalf("conflicts = %+v", outcome.conflicts)
	}
}

func TestOrganisationsBackfillOrderedIndexCoverage(t *testing.T) {
	required := []organisationsBackfillIndexKey{
		{Field: "organisation_id", Direction: 1},
		{Field: "ends_at", Direction: 1},
	}
	tests := []struct {
		name string
		key  bson.D
		want string
	}{
		{name: "exact", key: bson.D{{Key: "organisation_id", Value: int32(1)}, {Key: "ends_at", Value: int64(1)}}, want: "exact"},
		{name: "covered prefix", key: bson.D{{Key: "organisation_id", Value: 1}, {Key: "ends_at", Value: 1}, {Key: "_id", Value: -1}}, want: "prefix"},
		{name: "shorter index", key: bson.D{{Key: "organisation_id", Value: 1}}, want: "missing"},
		{name: "wrong order", key: bson.D{{Key: "ends_at", Value: 1}, {Key: "organisation_id", Value: 1}}, want: "missing"},
		{name: "wrong direction", key: bson.D{{Key: "organisation_id", Value: 1}, {Key: "ends_at", Value: -1}}, want: "missing"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := organisationsBackfillOrderedIndexCoverage(test.key, required); got != test.want {
				t.Fatalf("organisationsBackfillOrderedIndexCoverage() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestObserveOrganisationsBackfillSubscriptionDocumentRecordsTypesAndShape(t *testing.T) {
	report := organisationsBackfillSubscriptionResolution{
		ObservedFieldTypes: make(map[string]map[string]int64),
		ObservedShapes:     make(map[string]int64),
	}
	document := organisationsBackfillTestRaw(t, bson.D{
		{Key: "_id", Value: primitive.NewObjectID()},
		{Key: "organisation_id", Value: primitive.NewObjectID()},
		{Key: "user_id", Value: primitive.NewObjectID().Hex()},
	})
	observeOrganisationsBackfillSubscriptionDocument(&report, document)
	if report.ObservedFieldTypes["organisation_id"]["objectID"] != 1 || report.ObservedFieldTypes["user_id"]["string"] != 1 {
		t.Fatalf("observed field types = %#v", report.ObservedFieldTypes)
	}
	if len(report.ObservedShapes) != 1 {
		t.Fatalf("observed shapes = %#v", report.ObservedShapes)
	}
}

func organisationsBackfillTestRaw(t *testing.T, document bson.D) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(document)
	if err != nil {
		t.Fatalf("bson.Marshal() error = %v", err)
	}
	return raw
}
