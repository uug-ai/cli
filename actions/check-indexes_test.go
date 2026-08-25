package actions

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

// The parser had no tests before partialFilterExpression support was added. The
// first group pins the behaviour that existed then, so the shared
// splitTopLevelFields extraction cannot change how ordinary index files parse.

func TestParseKeyFieldsSingleAndCompound(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		want bson.D
	}{
		{
			name: "single ascending",
			doc:  "{ _id: 1 }",
			want: bson.D{{Key: "_id", Value: int32(1)}},
		},
		{
			name: "compound mixed direction",
			doc:  "{ organisationId: 1, createdAt: -1 }",
			want: bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "createdAt", Value: int32(-1)}},
		},
		{
			name: "quoted keys",
			doc:  "{ 'alert_master_user': 1, \"timestamp\": -1 }",
			want: bson.D{{Key: "alert_master_user", Value: int32(1)}, {Key: "timestamp", Value: int32(-1)}},
		},
		{
			name: "text plugin value is kept as a string",
			doc:  "{ description: 'text' }",
			want: bson.D{{Key: "description", Value: "text"}},
		},
		{
			name: "numeric constructor is unwrapped",
			doc:  "{ createdAt: NumberLong('-1') }",
			want: bson.D{{Key: "createdAt", Value: int32(-1)}},
		},
		{
			name: "empty document",
			doc:  "{}",
			want: bson.D{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseKeyFields(tc.doc)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("parseKeyFields(%q) = %#v, want %#v", tc.doc, got, tc.want)
			}
		})
	}
}

func TestNormalizeKeyPreservesCompoundOrder(t *testing.T) {
	forward := bson.D{{Key: "organisation_id", Value: int32(1)}, {Key: "ends_at", Value: int32(1)}}
	reverse := bson.D{{Key: "ends_at", Value: int32(1)}, {Key: "organisation_id", Value: int32(1)}}
	if got, want := normalizeKey(forward), "organisation_id:1.ends_at:1"; got != want {
		t.Fatalf("normalizeKey(forward) = %q, want %q", got, want)
	}
	if normalizeKey(forward) == normalizeKey(reverse) {
		t.Fatal("reverse-order compound index normalized as the required index")
	}
}

func TestExtractUnique(t *testing.T) {
	if extractUnique("{ v: 2, key: { slug: 1 }, name: 'slug_1', unique: true }") != true {
		t.Fatal("unique: true not detected")
	}
	if extractUnique("{ v: 2, key: { slug: 1 }, name: 'slug_1' }") != false {
		t.Fatal("absent unique reported as true")
	}
}

func TestRedactMongoURI(t *testing.T) {
	tests := map[string]string{
		"mongodb+srv://user:secret@example.mongodb.net/?retryWrites=true": "mongodb+srv://<redacted>",
		"mongodb://user:secret@localhost:27017/database":                  "mongodb://<redacted>",
		"not-a-mongodb-uri": "<redacted>",
	}
	for input, want := range tests {
		if got := redactMongoURI(input); got != want {
			t.Errorf("redactMongoURI(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestExtractKeyDocStopsAtBalancedBrace(t *testing.T) {
	obj := "{ v: 2, key: { organisationId: 1, slug: 1 }, name: 'organisationId_1_slug_1' }"
	if got, want := extractKeyDoc(obj), "{ organisationId: 1, slug: 1 }"; got != want {
		t.Fatalf("extractKeyDoc = %q, want %q", got, want)
	}
}

// Partial filter expressions.

func TestExtractPartialFilterExpression(t *testing.T) {
	obj := "{ v: 2, key: { slug: 1 }, name: 'slug_1', unique: true, " +
		"partialFilterExpression: { slug: { $exists: true, $type: 'string' } } }"

	got := extractPartialFilterExpression(obj)
	want := "{ slug: { $exists: true, $type: 'string' } }"
	if got != want {
		t.Fatalf("extractPartialFilterExpression = %q, want %q", got, want)
	}

	if extractPartialFilterExpression("{ v: 2, key: { slug: 1 }, name: 'slug_1' }") != "" {
		t.Fatal("absent partialFilterExpression reported as present")
	}
}

func TestParseFilterDocPreservesTypes(t *testing.T) {
	// The organisation slug filter is the reason this parser exists:
	// parseKeyFields would flatten the nested operator document to slug:1.
	got := parseFilterDoc("{ slug: { $exists: true, $type: 'string' } }")
	want := bson.M{"slug": bson.M{"$exists": true, "$type": "string"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseFilterDoc = %#v, want %#v", got, want)
	}
}

func TestParseFilterDocValueTypes(t *testing.T) {
	got := parseFilterDoc("{ enabled: false, retries: 3, state: 'active', nested: { $gt: 0 } }")
	want := bson.M{
		"enabled": false,
		"retries": int32(3),
		"state":   "active",
		"nested":  bson.M{"$gt": int32(0)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseFilterDoc = %#v, want %#v", got, want)
	}
}

func TestParseFilterDocRejectsNonDocument(t *testing.T) {
	if got := parseFilterDoc(""); got != nil {
		t.Fatalf("parseFilterDoc(\"\") = %#v, want nil", got)
	}
	if got := parseFilterDoc("{ slug: 1"); got != nil {
		t.Fatalf("unbalanced document parsed as %#v, want nil", got)
	}
}

func TestParseIndexArrayBlockCarriesPartialFilter(t *testing.T) {
	block := []string{
		"[",
		"{ v: 2, key: { _id: 1 }, name: '_id_' },",
		"{ v: 2, key: { slug: 1 }, name: 'slug_1', unique: true, partialFilterExpression: { slug: { $exists: true, $type: 'string' } } }",
		"]",
	}

	specs := parseIndexArrayBlock(block)
	if len(specs) != 2 {
		t.Fatalf("parsed %d specs, want 2", len(specs))
	}

	if specs[0].PartialFilterExpression != nil {
		t.Fatalf("_id_ carries a partial filter: %#v", specs[0].PartialFilterExpression)
	}

	slugSpec := specs[1]
	if slugSpec.Name != "slug_1" || !slugSpec.Unique {
		t.Fatalf("slug spec = %+v, want name slug_1 and unique", slugSpec)
	}
	want := bson.M{"slug": bson.M{"$exists": true, "$type": "string"}}
	if !reflect.DeepEqual(slugSpec.PartialFilterExpression, want) {
		t.Fatalf("slug partial filter = %#v, want %#v", slugSpec.PartialFilterExpression, want)
	}
}

// The shipped indexes file is the contract the checker actually applies, so it
// is asserted directly rather than through a fixture.

func TestShippedIndexFileDeclaresSlugIndexes(t *testing.T) {
	path := filepath.Join("..", "indexes", "hub-20-08-2026.txt")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("indexes file missing: %v", err)
	}

	canonical, err := loadCanonicalIndexSpecsFromFile(path)
	if err != nil {
		t.Fatalf("loadCanonicalIndexSpecsFromFile: %v", err)
	}

	slugFilter := bson.M{"slug": bson.M{"$exists": true, "$type": "string"}}

	// organisation {slug: 1}: the partial filter is mandatory here. Organisation
	// slugs are omitempty, so a plain unique index fails to build the moment two
	// organisations have no slug key.
	orgSpec := findSpecByKey(t, canonical["organisation"], "slug:1")
	if !orgSpec.Unique {
		t.Fatal("organisation slug index is not unique")
	}
	if !reflect.DeepEqual(orgSpec.PartialFilterExpression, slugFilter) {
		t.Fatalf("organisation slug partial filter = %#v, want %#v", orgSpec.PartialFilterExpression, slugFilter)
	}

	// project {organisationId: 1, slug: 1}: matches ensureProjectIndexes exactly.
	// A plain unique index on the same keys is a different index, and Mongo
	// rejects the second definition with IndexOptionsConflict.
	projSpec := findSpecByKey(t, canonical["project"], "organisationId:1.slug:1")
	if !projSpec.Unique {
		t.Fatal("project slug index is not unique")
	}
	if !reflect.DeepEqual(projSpec.PartialFilterExpression, slugFilter) {
		t.Fatalf("project slug partial filter = %#v, want %#v", projSpec.PartialFilterExpression, slugFilter)
	}

	// The listing index the bootstrap preflight also requires.
	findSpecByKey(t, canonical["project"], "organisationId:1")
}

func TestSubscriptionOwnershipIndexFileDeclaresOrderedContracts(t *testing.T) {
	path := filepath.Join("..", "indexes", "migration-hub-subscription-ownership-21-08-2026.txt")
	canonical, err := loadCanonicalIndexSpecsFromFile(path)
	if err != nil {
		t.Fatalf("loadCanonicalIndexSpecsFromFile: %v", err)
	}

	want := []IndexSpec{
		{
			Name: "ends_at_1",
			Key:  bson.D{{Key: "ends_at", Value: int32(1)}},
		},
		{
			Name: "organisation_id_1_ends_at_1",
			Key:  bson.D{{Key: "organisation_id", Value: int32(1)}, {Key: "ends_at", Value: int32(1)}},
		},
		{
			Name: "user_id_1_ends_at_1",
			Key:  bson.D{{Key: "user_id", Value: int32(1)}, {Key: "ends_at", Value: int32(1)}},
		},
		{
			Name: "organisation_id_1_updated_at_-1_created_at_-1__id_-1",
			Key: bson.D{
				{Key: "organisation_id", Value: int32(1)},
				{Key: "updated_at", Value: int32(-1)},
				{Key: "created_at", Value: int32(-1)},
				{Key: "_id", Value: int32(-1)},
			},
		},
	}
	if got := canonical["subscriptions"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("subscription ownership indexes = %#v, want %#v", got, want)
	}
}

func TestAlertOwnershipIndexFileDeclaresOrderedContracts(t *testing.T) {
	path := filepath.Join("..", "indexes", "migration-hub-alert-ownership-21-08-2026.txt")
	canonical, err := loadCanonicalIndexSpecsFromFile(path)
	if err != nil {
		t.Fatalf("loadCanonicalIndexSpecsFromFile: %v", err)
	}

	want := []IndexSpec{
		{Name: "organisationId_1_projectId_1_enabled_1", Key: bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "enabled", Value: int32(1)}}},
		{Name: "master_user_id_1_projectId_1_enabled_1", Key: bson.D{{Key: "master_user_id", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "enabled", Value: int32(1)}}},
		{Name: "user_id_1_projectId_1_enabled_1", Key: bson.D{{Key: "user_id", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "enabled", Value: int32(1)}}},
		{Name: "master_user_id_1_enabled_1", Key: bson.D{{Key: "master_user_id", Value: int32(1)}, {Key: "enabled", Value: int32(1)}}},
	}
	if got := canonical["alerts"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("alert ownership indexes = %#v, want %#v", got, want)
	}
}

func TestDeviceScopeIndexFileDeclaresCanonicalAndLegacyContracts(t *testing.T) {
	path := filepath.Join("..", "indexes", "migration-hub-device-scope-25-08-2026.txt")
	canonical, err := loadCanonicalIndexSpecsFromFile(path)
	if err != nil {
		t.Fatalf("loadCanonicalIndexSpecsFromFile: %v", err)
	}

	want := []IndexSpec{
		{
			Name: "organisationId_1_projectId_1_key_1",
			Key: bson.D{
				{Key: "organisationId", Value: int32(1)},
				{Key: "projectId", Value: int32(1)},
				{Key: "key", Value: int32(1)},
			},
		},
		{
			Name: "organisationId_1_projectId_1_key_1_analytics.cloudpublickey_1",
			Key: bson.D{
				{Key: "organisationId", Value: int32(1)},
				{Key: "projectId", Value: int32(1)},
				{Key: "key", Value: int32(1)},
				{Key: "analytics.cloudpublickey", Value: int32(1)},
			},
		},
		{
			Name: "key_1_user_id_1",
			Key: bson.D{
				{Key: "key", Value: int32(1)},
				{Key: "user_id", Value: int32(1)},
			},
		},
		{
			Name: "key_1_user_id_1_analytics.cloudpublickey_1",
			Key: bson.D{
				{Key: "key", Value: int32(1)},
				{Key: "user_id", Value: int32(1)},
				{Key: "analytics.cloudpublickey", Value: int32(1)},
			},
		},
	}
	if got := canonical["devices"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("device scope indexes = %#v, want %#v", got, want)
	}
	for _, spec := range canonical["devices"] {
		if spec.Unique {
			t.Fatalf("device scope index %q must remain non-unique before reconciliation", spec.Name)
		}
	}
	wantUsers := []IndexSpec{{
		Name: "amazon_access_key_id_1",
		Key:  bson.D{{Key: "amazon_access_key_id", Value: int32(1)}},
	}}
	if got := canonical["users"]; !reflect.DeepEqual(got, wantUsers) {
		t.Fatalf("cloud-key user indexes = %#v, want %#v", got, wantUsers)
	}
}

func findSpecByKey(t *testing.T, specs []IndexSpec, normalized string) IndexSpec {
	t.Helper()
	for _, s := range specs {
		if normalizeKey(s.Key) == normalized {
			return s
		}
	}
	t.Fatalf("no index with key %q among %d specs", normalized, len(specs))
	return IndexSpec{}
}

func TestDescribePartialFilterIsStable(t *testing.T) {
	filter := bson.M{"slug": bson.M{"$type": "string", "$exists": true}}
	want := "{slug: {$exists: true, $type: string}}"
	for i := 0; i < 8; i++ {
		if got := describePartialFilter(filter); got != want {
			t.Fatalf("describePartialFilter = %q, want %q", got, want)
		}
	}
	if got := describePartialFilter(nil); got != "-" {
		t.Fatalf("describePartialFilter(nil) = %q, want %q", got, "-")
	}
}
