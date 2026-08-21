package actions

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type IndexSpec struct {
	Name   string
	Key    bson.D
	Unique bool
	// PartialFilterExpression restricts which documents the index covers. It is
	// nil for the ordinary full index that every other entry in the indexes
	// files describes.
	//
	// A unique index needs this whenever the indexed field is optional. Mongo
	// treats an absent field as null for a plain unique index, so the first
	// document without the field indexes as null and the second collides — the
	// build fails outright on any collection that already holds two of them.
	// The organisation slug is exactly that case: the field is `omitempty`, so
	// most organisations have no slug key at all, and the uniqueness has to
	// apply only to the ones that do.
	PartialFilterExpression bson.M
}

const (
	DefaultmongodbDestinationDatabase = "Kerberos"
	DefaultMongoURI                   = "mongodb://localhost:27017"
	DefaultIndexVersion               = "hub-08-12-2025"
	ServerSelectionTimeoutCheck       = 10 // seconds
)

func CheckIndexes(
	mongoURI string,
	mongodbDestinationDatabase string,
	collectionsCSV string,
	mode string,
	indexVersion string,
) {
	HandleSignals()
	flag.Parse()

	// --- mongodb-uri ---
	if WasFlagPassed("mongodb-uri") {
		if mongoURI == "" {
			mongoURI = DefaultMongoURI
		}
		fmt.Printf("[info] using flag -mongodb-uri=%s\n", redactMongoURI(mongoURI))
	} else {
		in := PromptString(fmt.Sprintf("MongoDB URI (-mongodb-uri, default %s): ", DefaultMongoURI))
		if strings.TrimSpace(in) == "" {
			mongoURI = DefaultMongoURI
			fmt.Printf("[info] using default -mongodb-uri=%s\n", redactMongoURI(mongoURI))
		} else {
			mongoURI = in
			fmt.Printf("[info] using input -mongodb-uri=%s\n", redactMongoURI(mongoURI))
		}
	}

	// --- destination database ---
	if WasFlagPassed("mongodb-destination-database") {
		if mongodbDestinationDatabase == "" {
			mongodbDestinationDatabase = DefaultmongodbDestinationDatabase
		}
		fmt.Printf("[info] using flag -mongodb-destination-database=%s\n", mongodbDestinationDatabase)
	} else {
		in := PromptString(fmt.Sprintf("Database (-mongodb-destination-database, default %s): ", DefaultmongodbDestinationDatabase))
		if strings.TrimSpace(in) == "" {
			mongodbDestinationDatabase = DefaultmongodbDestinationDatabase
			fmt.Printf("[info] using default -mongodb-destination-database=%s\n", mongodbDestinationDatabase)
		} else {
			mongodbDestinationDatabase = in
			fmt.Printf("[info] using input -mongodb-destination-database=%s\n", mongodbDestinationDatabase)
		}
	}

	// --- collections (optional) ---
	var collections []string
	if WasFlagPassed("collections") {
		collections = parseCSV(collectionsCSV) // may be empty => all
		if len(collections) == 0 {
			fmt.Println("[info] -collections passed empty: checking all collections from indexes file")
		} else {
			fmt.Printf("[info] using flag -collections=%v\n", collections)
		}
	} else {
		in := PromptString("Collections to check (-collections, comma-separated, empty for all): ")
		collections = parseCSV(in)
		if len(collections) == 0 {
			fmt.Println("[info] using default: all collections present in the indexes file")
		} else {
			fmt.Printf("[info] using input -collections=%v\n", collections)
		}
	}

	// --- mode ---
	if WasFlagPassed("mode") {
		fmt.Printf("[info] using flag -mode=%s\n", mode)
	} else {
		in := PromptString("Mode (live/dry-run, default dry-run): ")
		mode = strings.TrimSpace(in)
		if mode == "" {
			mode = "dry-run"
		}
		fmt.Printf("[info] using mode=%s\n", mode)
	}

	// --- index version ---
	var indexesFile string
	var version string = strings.TrimSpace(indexVersion)

	if WasFlagPassed("index-version") {
		if version == "" {
			// Flag was passed but empty; fall back to default
			indexesFile = fmt.Sprintf("indexes/%s.txt", DefaultIndexVersion)
			fmt.Printf("[warn] -index-version was passed but empty; using default version %q\n", DefaultIndexVersion)
		} else {
			// Flag passed and non-empty; use it
			indexesFile = fmt.Sprintf("indexes/%s.txt", version)
			fmt.Printf("[info] using -index-version=%q -> file %s\n", version, indexesFile)
		}
	} else {
		// Flag not passed; use default
		indexesFile = fmt.Sprintf("indexes/%s.txt", DefaultIndexVersion)
		fmt.Printf("[info] -index-version not set; using default version %q -> file %s\n", DefaultIndexVersion, indexesFile)
	}

	indexesFile = filepath.Clean(indexesFile)
	fmt.Printf("[info] using indexes file: %s\n", indexesFile)

	// Connect
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().
		ApplyURI(mongoURI).
		SetServerSelectionTimeout(ServerSelectionTimeoutCheck*time.Second))
	if err != nil {
		fmt.Printf("[error] connect mongo: %v\n", err)
		os.Exit(1)
	}
	defer client.Disconnect(ctx)
	db := client.Database(mongodbDestinationDatabase)

	// Load canonical index specs from file
	canonical, err := loadCanonicalIndexSpecsFromFile(indexesFile)
	if err != nil {
		fmt.Printf("[error] parsing indexes file %s: %v\n", indexesFile, err)
		os.Exit(1)
	}

	// Determine target collections
	targetCollections := collections
	if len(targetCollections) == 0 {
		for coll := range canonical {
			targetCollections = append(targetCollections, coll)
		}
	}

	// Accumulate missing specs for a single creation pass later
	type missEntry struct {
		coll string
		spec IndexSpec
	}
	var allMissing []missEntry
	missingTotal := 0
	missingByCollection := make(map[string][]IndexSpec)

	for _, collName := range targetCollections {
		specs, ok := canonical[collName]
		if !ok {
			fmt.Printf("[warn] collection %q not present in indexes file; skipping\n", collName)
			continue
		}

		existing, err := listIndexKeys(ctx, db.Collection(collName))
		if err != nil {
			fmt.Printf("[error] list indexes for %s: %v\n", collName, err)
			continue
		}

		// Compare
		var missing []IndexSpec
		for _, s := range specs {
			normalized := normalizeKey(s.Key)
			if _, found := existing[normalized]; !found {
				missing = append(missing, s)
				allMissing = append(allMissing, missEntry{coll: collName, spec: s})
			}
		}

		if len(missing) == 0 {
			continue
		}

		missingTotal += len(missing)
		missingByCollection[collName] = missing
	}

	if missingTotal == 0 {
		fmt.Println("")
		fmt.Println("[ok] all canonical indexes present across checked collections.")
		fmt.Println("")
		return
	} else {
		fmt.Println("")
		fmt.Println("Missing indexes:")
		fmt.Println("")
	}

	// Render tables per collection for missing indexes only
	for collName, misses := range missingByCollection {
		fmt.Println("")
		fmt.Printf(">> Collection: %s\n", collName)
		fmt.Println("")
		border := "  +------------------------------+----------------------------------------------------+---------+------------------------------------------+"
		fmt.Println(border)
		fmt.Printf("  | %-28s | %-50s | %-7s | %-40s |\n", "Name", "Key", "Unique", "Partial")
		fmt.Println(border)
		for _, m := range misses {
			name := m.Name
			key := normalizeKey(m.Key)
			unique := "false"
			if m.Unique {
				unique = "true"
			}
			fmt.Printf("  | %-28s | %-50s | %-7s | %-40s |\n", name, key, unique, describePartialFilter(m.PartialFilterExpression))
		}
		fmt.Println(border)
	}

	fmt.Printf("\n[summary] missing_total=%d mode=%s\n", missingTotal, mode)

	// Mode gate: dry-run skips creation and any prompts
	if strings.EqualFold(mode, "dry-run") {
		fmt.Println("[info] dry-run mode: skipping index creation and prompts.")
		return
	}

	if strings.EqualFold(mode, "live") {
		// Create all missing indexes in one pass
		fmt.Printf("[action] creating %d missing index(es) across %d collection(s)...\n", missingTotal, len(targetCollections))
		for _, m := range allMissing {
			opts := options.Index().SetName(m.spec.Name)
			if m.spec.Unique {
				opts.SetUnique(true)
			}
			if len(m.spec.PartialFilterExpression) > 0 {
				opts.SetPartialFilterExpression(m.spec.PartialFilterExpression)
			}
			_, err := db.Collection(m.coll).Indexes().CreateOne(ctx, mongo.IndexModel{
				Keys:    m.spec.Key,
				Options: opts,
			})
			if err != nil {
				fmt.Printf("  [error] create %s/%s: %v\n", m.coll, m.spec.Name, err)
			} else {
				fmt.Printf("  [ok] created %s/%s\n", m.coll, m.spec.Name)
			}
		}
		fmt.Println("")
		fmt.Println("[done] index creation pass complete.")
		fmt.Println("")
		return
	}

	fmt.Printf("[info] unrecognized mode %q; skipping index creation.\n", mode)
}

// Helpers

func redactMongoURI(uri string) string {
	uri = strings.TrimSpace(uri)
	for _, scheme := range []string{"mongodb+srv://", "mongodb://"} {
		if strings.HasPrefix(uri, scheme) {
			return scheme + "<redacted>"
		}
	}
	return "<redacted>"
}

func parseCSV(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// listIndexKeys returns a set keyed by ordered key spec like "name:1.user_id:1".
func listIndexKeys(ctx context.Context, coll *mongo.Collection) (map[string]struct{}, error) {
	cur, err := coll.Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	out := make(map[string]struct{})
	for cur.Next(ctx) {
		var doc struct {
			Key bson.D `bson:"key"`
		}
		if err := cur.Decode(&doc); err != nil || len(doc.Key) == 0 {
			continue
		}
		out[normalizeKey(doc.Key)] = struct{}{}
	}
	return out, cur.Err()
}

// normalizeKey builds an order-preserving string like "field1:1.field2:-1".
// Compound index order changes which query prefixes the index can support.
func normalizeKey(d bson.D) string {
	if len(d) == 0 {
		return ""
	}
	var sb strings.Builder
	for i, e := range d {
		if i > 0 {
			sb.WriteString(".")
		}
		sb.WriteString(e.Key)
		sb.WriteString(":")
		switch v := e.Value.(type) {
		case int32:
			sb.WriteString(fmt.Sprintf("%d", v))
		case int64:
			sb.WriteString(fmt.Sprintf("%d", v))
		case int:
			sb.WriteString(fmt.Sprintf("%d", v))
		default:
			sb.WriteString(fmt.Sprintf("%v", v))
		}
	}
	return sb.String()
}

// describePartialFilter renders a partial filter expression for the missing-index
// table. Keys are sorted so the same filter always prints the same way.
func describePartialFilter(m bson.M) string {
	if len(m) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var sb strings.Builder
	sb.WriteString("{")
	for i, key := range keys {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(key)
		sb.WriteString(": ")
		if nested, ok := m[key].(bson.M); ok {
			sb.WriteString(describePartialFilter(nested))
			continue
		}
		sb.WriteString(fmt.Sprintf("%v", m[key]))
	}
	sb.WriteString("}")
	return sb.String()
}

func loadCanonicalIndexSpecsFromFile(path string) (map[string][]IndexSpec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(data), "\n")

	result := make(map[string][]IndexSpec)
	var currentColl string
	var buf []string
	inArray := false

	flushArray := func() {
		if currentColl == "" || len(buf) == 0 {
			buf = nil
			inArray = false
			return
		}
		specs := parseIndexArrayBlock(buf)
		if len(specs) > 0 {
			result[currentColl] = specs
		}
		buf = nil
		inArray = false
	}

	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}

		// Begin array (may start and end on same line)
		if strings.HasPrefix(line, "[") {
			inArray = true
		}
		if inArray {
			buf = append(buf, line)
			if strings.HasSuffix(line, "]") {
				flushArray()
			}
			continue
		}

		// New collection header (plain text line)
		if !strings.HasPrefix(line, "[") && !strings.HasPrefix(line, "{") {
			if currentColl != "" && len(buf) > 0 {
				flushArray()
			}
			currentColl = line
			continue
		}
	}
	if currentColl != "" && len(buf) > 0 {
		flushArray()
	}
	return result, nil
}

func parseIndexArrayBlock(lines []string) []IndexSpec {
	block := strings.Join(lines, "\n")
	parts := splitObjects(block)
	specs := make([]IndexSpec, 0, len(parts))
	for _, p := range parts {
		name := extractName(p)
		keyDoc := extractKeyDoc(p)
		if keyDoc == "" {
			continue
		}
		key := parseKeyFields(keyDoc)
		if len(key) == 0 {
			continue
		}
		specs = append(specs, IndexSpec{
			Name:                    name,
			Key:                     key,
			Unique:                  extractUnique(p),
			PartialFilterExpression: parseFilterDoc(extractPartialFilterExpression(p)),
		})
	}
	return specs
}

func splitObjects(block string) []string {
	var parts []string
	var cur strings.Builder
	depth := 0
	for _, r := range block {
		cur.WriteRune(r)
		switch r {
		case '{':
			depth++
		case '}':
			if depth > 0 {
				depth--
			}
		}
		if depth == 0 && strings.HasSuffix(cur.String(), "},") {
			parts = append(parts, strings.TrimSpace(cur.String()))
			cur.Reset()
		}
	}
	rest := strings.TrimSpace(cur.String())
	if rest != "" {
		rest = strings.TrimPrefix(rest, "[")
		rest = strings.TrimSuffix(rest, "]")
		if trimmed := strings.TrimSpace(rest); trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return parts
}

func extractName(obj string) string {
	start := strings.Index(obj, "name:")
	if start == -1 {
		return ""
	}
	s := strings.TrimSpace(obj[start+5:])
	q := strings.IndexAny(s, "'\"")
	if q == -1 {
		return ""
	}
	s = s[q+1:]
	end := strings.IndexAny(s, "'\"")
	if end == -1 {
		return ""
	}
	return s[:end]
}

func extractKeyDoc(obj string) string {
	return extractBraceDoc(obj, "key:")
}

// extractPartialFilterExpression returns the raw `partialFilterExpression: {...}`
// document from an index object, or "" when the index has none. Both the
// mongosh dump spelling and the shorthand used when hand-writing an indexes
// file are accepted.
func extractPartialFilterExpression(obj string) string {
	for _, label := range []string{"partialFilterExpression:", "partialFilter:"} {
		if doc := extractBraceDoc(obj, label); doc != "" {
			return doc
		}
	}
	return ""
}

// extractBraceDoc returns the balanced `{...}` document that follows label in
// obj, or "" when label is absent or is not followed by one. Quoted braces are
// not counted, so a filter such as { name: '{literal}' } stays balanced.
func extractBraceDoc(obj string, label string) string {
	start := strings.Index(obj, label)
	if start == -1 {
		return ""
	}
	s := strings.TrimSpace(obj[start+len(label):])
	if !strings.HasPrefix(s, "{") {
		return ""
	}
	var (
		b       strings.Builder
		inQuote rune
		depth   int
	)
	for _, r := range s {
		b.WriteRune(r)
		switch r {
		case '\'', '"':
			if inQuote == 0 {
				inQuote = r
			} else if inQuote == r {
				inQuote = 0
			}
		case '{':
			if inQuote == 0 {
				depth++
			}
		case '}':
			if inQuote == 0 {
				depth--
				if depth == 0 {
					return b.String()
				}
			}
		}
	}
	// Unbalanced: the object was truncated. Treat it as absent rather than
	// creating an index with a half-read filter.
	return ""
}

func extractUnique(obj string) bool {
	// Detect "unique: true" in object (optional; not present in most dumps)
	return strings.Contains(obj, "unique: true")
}

// parseFilterDoc parses a partialFilterExpression document into bson.M.
//
// This is deliberately not parseKeyFields: a key document maps every value to a
// sort direction, coercing anything unrecognized toward int32(1), whereas a
// filter carries booleans, strings and nested operator documents that have to
// survive intact. `{ slug: { $exists: true, $type: 'string' } }` would come out
// of parseKeyFields as slug:1.
func parseFilterDoc(doc string) bson.M {
	doc = strings.TrimSpace(doc)
	if !strings.HasPrefix(doc, "{") || !strings.HasSuffix(doc, "}") {
		return nil
	}
	inner := strings.TrimSpace(doc[1 : len(doc)-1])
	if inner == "" {
		return bson.M{}
	}

	out := bson.M{}
	for _, field := range splitTopLevelFields(inner) {
		kv := strings.SplitN(field, ":", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(strings.Trim(strings.TrimSpace(kv[0]), "'\""))
		if key == "" {
			continue
		}
		out[key] = parseFilterValue(strings.TrimSpace(kv[1]))
	}
	return out
}

// parseFilterValue converts a single filter value literal into the Go type the
// driver should send: a nested document, a bool, an int32, or a string.
func parseFilterValue(val string) interface{} {
	val = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(val), ","))
	switch {
	case val == "":
		return ""
	case strings.HasPrefix(val, "{"):
		if nested := parseFilterDoc(val); nested != nil {
			return nested
		}
		return val
	case val == "true":
		return true
	case val == "false":
		return false
	case strings.HasPrefix(val, "'"), strings.HasPrefix(val, "\""):
		return strings.Trim(val, "'\"")
	}
	if i, err := parseInt(unwrapNumericConstructor(val)); err == nil {
		return int32(i)
	}
	return strings.Trim(val, "'\"")
}

// splitTopLevelFields splits a comma-separated document body on the commas that
// sit outside quotes and outside any nested {} or []. Both parseKeyFields and
// parseFilterDoc need this, and they must agree on it.
func splitTopLevelFields(body string) []string {
	var (
		parts   []string
		cur     strings.Builder
		inQuote rune
		depth   int
	)

	for _, r := range body {
		switch r {
		case '\'', '"':
			if inQuote == 0 {
				inQuote = r
			} else if inQuote == r {
				inQuote = 0
			}
			cur.WriteRune(r)
		case '{', '[':
			if inQuote == 0 {
				depth++
			}
			cur.WriteRune(r)
		case '}', ']':
			if inQuote == 0 && depth > 0 {
				depth--
			}
			cur.WriteRune(r)
		case ',':
			if inQuote == 0 && depth == 0 {
				if segment := strings.TrimSpace(cur.String()); segment != "" {
					parts = append(parts, segment)
				}
				cur.Reset()
			} else {
				cur.WriteRune(r)
			}
		default:
			cur.WriteRune(r)
		}
	}
	if tail := strings.TrimSpace(cur.String()); tail != "" {
		parts = append(parts, tail)
	}
	return parts
}

func parseKeyFields(doc string) bson.D {
	doc = strings.TrimSpace(doc)
	doc = strings.TrimPrefix(doc, "{")
	doc = strings.TrimSuffix(doc, "}")
	doc = strings.TrimSpace(doc)
	if doc == "" {
		return bson.D{}
	}

	parts := splitTopLevelFields(doc)

	out := make(bson.D, 0, len(parts))
	for _, p := range parts {
		kv := strings.SplitN(p, ":", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(strings.Trim(kv[0], "'\""))
		val := strings.TrimSpace(strings.Trim(kv[1], ","))
		if key == "" {
			continue
		}

		var parsed interface{} = int32(1)
		if val != "" {
			val = unwrapNumericConstructor(val)
			switch val {
			case "1", "+1":
				parsed = int32(1)
			case "-1":
				parsed = int32(-1)
			default:
				if i, err := parseInt(val); err == nil {
					parsed = int32(i)
				} else {
					parsed = strings.Trim(val, "'\"")
				}
			}
		}
		out = append(out, bson.E{Key: key, Value: parsed})
	}
	return out
}

func parseInt(s string) (int, error) {
	s = strings.TrimSpace(s)
	sign := 1
	if after, ok := strings.CutPrefix(s, "+"); ok {
		s = after
	}
	if strings.HasPrefix(s, "-") {
		sign = -1
		s = strings.TrimPrefix(s, "-")
	}
	if s == "" {
		return 0, fmt.Errorf("non-numeric")
	}
	var n int
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("non-numeric")
		}
		n = n*10 + int(r-'0')
	}
	return n * sign, nil
}

// unwrapNumericConstructor strips mongosh/EJSON numeric wrappers such as
// Long('1'), NumberLong("1"), NumberInt(1), NumberDecimal('1'), Double(1),
// and the EJSON form { "$numberLong": "1" } from a key value, returning
// the inner literal. Non-numeric wrappers (e.g. "text", "2dsphere") are
// returned unchanged so they can still be used as index plugin names.
func unwrapNumericConstructor(val string) string {
	v := strings.TrimSpace(val)
	// Serialized BSON Long object: {"high":0,"low":1,"unsigned":false}
	// (produced when a Long(1) direction is JSON.stringify'd). Use the low word
	// as the effective direction value.
	if strings.HasPrefix(v, "{") && strings.Contains(v, "\"low\"") {
		if i := strings.Index(v, "\"low\""); i != -1 {
			rest := v[i+len("\"low\""):]
			if c := strings.Index(rest, ":"); c != -1 {
				rest = rest[c+1:]
				// read until the next comma or closing brace
				end := strings.IndexAny(rest, ",}")
				if end != -1 {
					return strings.Trim(strings.TrimSpace(rest[:end]), "'\"")
				}
			}
		}
	}
	// EJSON: { "$numberLong": "1" } / { "$numberInt": "1" } / { "$numberDouble": "1" }
	if strings.HasPrefix(v, "{") && strings.Contains(v, "$number") {
		if i := strings.Index(v, ":"); i != -1 {
			inner := strings.TrimSpace(v[i+1:])
			inner = strings.TrimSuffix(strings.TrimSpace(strings.TrimSuffix(inner, "}")), ",")
			return strings.Trim(strings.TrimSpace(inner), "'\"")
		}
	}
	// Constructor form: Name(<inner>)
	open := strings.Index(v, "(")
	if open <= 0 || !strings.HasSuffix(v, ")") {
		return v
	}
	name := strings.ToLower(strings.TrimSpace(v[:open]))
	switch name {
	case "long", "numberlong", "numberint", "int", "numberdecimal", "decimal", "double", "numberdouble":
		inner := strings.TrimSpace(v[open+1 : len(v)-1])
		return strings.Trim(inner, "'\"")
	}
	return v
}

// CLI entry wrapper to match SeedMedia signature, if you prefer calling like others.
func RunCheckIndexesCLI(
	mongodbURI string,
	mongodbDestinationDatabase string,
	collections string,
	mode string,
	indexVersion string,
) {
	if err := runCheckIndexesInternal(mongodbURI, mongodbDestinationDatabase, collections, mode, indexVersion); err != nil {
		fmt.Printf("[error] check-indexes: %v\n", err)
		os.Exit(1)
	}
}

// If you want a single-return API:
func runCheckIndexesInternal(
	mongoURI string,
	mongodbDestinationDatabase string,
	collectionsCSV string,
	mode string,
	indexVersion string,
) error {
	// Delegate to CheckIndexes which handles prompts and prints. Keeping a simple API surface.
	CheckIndexes(mongoURI, mongodbDestinationDatabase, collectionsCSV, mode, indexVersion)
	return nil
}
