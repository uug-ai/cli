package actions

import (
	"slices"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestPlanUserAccessReferencesRemovesOnlyMissingIds(t *testing.T) {
	organisation, kept, deleted := primitive.NewObjectID(), primitive.NewObjectID(), primitive.NewObjectID()
	keptGroup, deletedGroup := primitive.NewObjectID(), primitive.NewObjectID()
	sites := map[string]bool{kept.Hex(): true}
	groups := map[string]bool{keptGroup.Hex(): true}
	embedded := rawValue(t, bson.M{"name": "Old site", "devices": bson.A{"cam"}})
	subAccount := accessReferencesUser{Id: primitive.NewObjectID(), Master: organisation.Hex(),
		Sites: rawStrings(t, kept.Hex(), deleted.Hex(), "not-an-id"), Groups: rawStrings(t, keptGroup.Hex(), deletedGroup.Hex())}
	clean := accessReferencesUser{Id: primitive.NewObjectID(), Master: organisation.Hex(), Sites: rawStrings(t, kept.Hex())}
	owner := accessReferencesUser{Id: organisation, Sites: []bson.RawValue{embedded}, Groups: rawStrings(t, deletedGroup.Hex())}

	removals, skipped := planUserAccessReferences([]accessReferencesUser{subAccount, clean, owner}, sites, groups)

	if skipped != 1 {
		t.Errorf("skipped %d, want the embedded site document", skipped)
	}

	if len(removals) != 2 {
		t.Fatalf("removals %+v, want the sub-account and the owner", removals)
	}
	byUser := map[string]accessReferencesUserRemoval{}
	for _, removal := range removals {
		byUser[removal.UserId] = removal
	}
	got := byUser[subAccount.Id.Hex()]
	if !slices.Equal(got.Sites, []string{deleted.Hex(), "not-an-id"}) || !slices.Equal(got.Groups, []string{deletedGroup.Hex()}) || got.OrganisationId != organisation.Hex() {
		t.Errorf("sub-account removal %+v", got)
	}
	if got := byUser[organisation.Hex()]; got.OrganisationId != organisation.Hex() || len(got.Sites) != 0 || !slices.Equal(got.Groups, []string{deletedGroup.Hex()}) {
		t.Errorf("owner removal %+v: the organisation's own account belongs to itself", got)
	}
}

func rawValue(t *testing.T, value any) bson.RawValue {
	t.Helper()
	kind, data, err := bson.MarshalValue(value)
	if err != nil {
		t.Fatal(err)
	}
	return bson.RawValue{Type: kind, Value: data}
}

func rawStrings(t *testing.T, values ...string) []bson.RawValue {
	raw := make([]bson.RawValue, 0, len(values))
	for _, value := range values {
		raw = append(raw, rawValue(t, value))
	}
	return raw
}

func TestPlanAssignmentAccessReferencesRemovesOnlyMissingIds(t *testing.T) {
	kept, deleted, deletedGroup := primitive.NewObjectID(), primitive.NewObjectID(), primitive.NewObjectID()
	assignment := accessReferencesAssignment{Id: primitive.NewObjectID(), OrganisationId: primitive.NewObjectID()}
	assignment.Scope.SiteIds = []primitive.ObjectID{kept, deleted}
	assignment.Scope.GroupIds = []primitive.ObjectID{deletedGroup}
	untouched := accessReferencesAssignment{Id: primitive.NewObjectID()}
	untouched.Scope.SiteIds = []primitive.ObjectID{kept}

	removals := planAssignmentAccessReferences([]accessReferencesAssignment{assignment, untouched}, map[string]bool{kept.Hex(): true}, map[string]bool{})

	if len(removals) != 1 || removals[0].AssignmentId != assignment.Id.Hex() ||
		!slices.Equal(removals[0].Sites, []primitive.ObjectID{deleted}) || !slices.Equal(removals[0].Groups, []primitive.ObjectID{deletedGroup}) {
		t.Fatalf("removals %+v", removals)
	}
}

func TestAccessReferencesPullOnlyNamesPlannedIds(t *testing.T) {
	pull := accessReferencesPull("sites", []string{"a"}, "groups", []string(nil))
	want := bson.M{"$pull": bson.M{"sites": bson.M{"$in": []string{"a"}}}}
	if len(pull["$pull"].(bson.M)) != 1 || pull["$pull"].(bson.M)["sites"].(bson.M)["$in"].([]string)[0] != "a" {
		t.Errorf("pull %v, want %v", pull, want)
	}
}

func TestOrganisationsCleanupAccessReferencesValidatesFlags(t *testing.T) {
	valid := OrganisationsCleanupAccessReferencesConfig{MongoDBURI: "mongodb://127.0.0.1:1", Database: "hub"}
	for name, mutate := range map[string]func(*OrganisationsCleanupAccessReferencesConfig){
		"unknown mode":         func(c *OrganisationsCleanupAccessReferencesConfig) { c.Mode = "apply" },
		"no uri":               func(c *OrganisationsCleanupAccessReferencesConfig) { c.MongoDBURI = "" },
		"no database":          func(c *OrganisationsCleanupAccessReferencesConfig) { c.Database = "" },
		"invalid organisation": func(c *OrganisationsCleanupAccessReferencesConfig) { c.OrganisationID = "nope" },
		"zero organisation id": func(c *OrganisationsCleanupAccessReferencesConfig) { c.OrganisationID = primitive.NilObjectID.Hex() },
	} {
		t.Run(name, func(t *testing.T) {
			config := valid
			mutate(&config)
			if err := OrganisationsCleanupAccessReferences(config); err == nil {
				t.Error("accepted invalid flags")
			}
		})
	}
}
