package actions

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
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
}

const (
	DefaultDBName               = "Kerberos"
	DefaultMongoURI             = "mongodb://localhost:27017"
	DefaultIndexesFilePath      = "indexes/08-12-2025.txt"
	ServerSelectionTimeoutCheck = 10 // seconds
)

func CheckIndexes(
	mongoURI string,
	dbName string,
	collectionsCSV string,
	autoFix bool,
	mode string,
	indexesFilePath string,
) {
	HandleSignals()
	flag.Parse()

	// --- mongodb-uri ---
	if WasFlagPassed("mongodb-uri") {
		if mongoURI == "" {
			mongoURI = DefaultMongoURI
		}
		fmt.Printf("[info] using flag -mongodb-uri=%s\n", mongoURI)
	} else {
		in := PromptString(fmt.Sprintf("MongoDB URI (-mongodb-uri, default %s): ", DefaultMongoURI))
		if strings.TrimSpace(in) == "" {
			mongoURI = DefaultMongoURI
			fmt.Printf("[info] using default -mongodb-uri=%s\n", mongoURI)
		} else {
			mongoURI = in
			fmt.Printf("[info] using input -mongodb-uri=%s\n", mongoURI)
		}
	}

	// --- db ---
	if WasFlagPassed("db") {
		if dbName == "" {
			dbName = DefaultDBName
		}
		fmt.Printf("[info] using flag -db=%s\n", dbName)
	} else {
		in := PromptString(fmt.Sprintf("Database (-db, default %s): ", DefaultDBName))
		if strings.TrimSpace(in) == "" {
			dbName = DefaultDBName
			fmt.Printf("[info] using default -db=%s\n", dbName)
		} else {
			dbName = in
			fmt.Printf("[info] using input -db=%s\n", dbName)
		}
	}

	// --- collections (optional) ---
	var collections []string
	if WasFlagPassed("collections") {
		collections = parseCSV(collectionsCSV)
		fmt.Printf("[info] using flag -collections=%v\n", collections)
	} else {
		in := PromptString("Collections to check (-collections, comma-separated, empty for all): ")
		collections = parseCSV(in)
		if len(collections) == 0 {
			fmt.Println("[info] using default: all collections present in the indexes file")
		} else {
			fmt.Printf("[info] using input -collections=%v\n", collections)
		}
	}

	// --- auto-fix ---
	// if WasFlagPassed("auto-fix") {
	// 	fmt.Printf("[info] using flag -auto-fix=%v\n", autoFix)
	// } else {
	// 	fmt.Printf("[info] -auto-fix not set; will ask after reporting if missing indexes exist (unless dry-run).\n")
	// }

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

	// --- indexes file ---
	indexesFile := strings.TrimSpace(indexesFilePath)
	if WasFlagPassed("indexes-file") {
		if indexesFile == "" {
			indexesFile = DefaultIndexesFilePath
		}
		fmt.Printf("[info] using flag -indexes-file=%s\n", indexesFile)
	} else if indexesFile != "" {
		fmt.Printf("[info] using provided indexes file=%s\n", indexesFile)
	}
	if indexesFile == "" {
		indexesFile = DefaultIndexesFilePath
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
	db := client.Database(dbName)

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
		fmt.Println("  +------------------------------+-----------------------------------------------+---------+")
		fmt.Println("  | Name                         | Key                                           | Unique  |")
		fmt.Println("  +------------------------------+-----------------------------------------------+---------+")
		for _, m := range misses {
			name := m.Name
			key := normalizeKey(m.Key)
			unique := "false"
			if m.Unique {
				unique = "true"
			}
			fmt.Printf("  | %-28s | %-45s | %-7s |\n", name, key, unique)
		}
		fmt.Println("  +------------------------------+-----------------------------------------------+---------+")
	}

	fmt.Printf("\n[summary] missing_total=%d auto_fix=%v mode=%s\n", missingTotal, autoFix, mode)

	// Mode gate: dry-run skips creation and any prompts
	if strings.EqualFold(mode, "dry-run") {
		fmt.Println("[info] dry-run mode: skipping index creation and prompts.")
		return
	}

	// Decide creation behavior (live mode)
	proceed := autoFix
	if !autoFix {
		proceed = PromptBool("Create ALL missing indexes across listed collections now? (y/N): ", false)
	}
	if !proceed {
		fmt.Println("[info] skipping index creation. Re-run with -auto-fix to create without prompt.")
		return
	}

	// Create all missing indexes in one pass
	fmt.Printf("[action] creating %d missing index(es) across %d collection(s)...\n", missingTotal, len(targetCollections))
	for _, m := range allMissing {
		opts := options.Index().SetName(m.spec.Name)
		if m.spec.Unique {
			opts.SetUnique(true)
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
}

// Helpers

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

// listIndexKeys returns a set keyed by normalized key spec like "name:1.user_id:1"
func listIndexKeys(ctx context.Context, coll *mongo.Collection) (map[string]struct{}, error) {
	cur, err := coll.Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	out := make(map[string]struct{})
	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			continue
		}
		// doc["key"] is a document of fields
		keyDoc, _ := doc["key"].(bson.M)
		if keyDoc == nil {
			// try bson.D
			if d, ok := doc["key"].(bson.D); ok {
				out[normalizeKey(d)] = struct{}{}
				continue
			}
			continue
		}
		// Convert bson.M to bson.D (stable order)
		var d bson.D
		for k, v := range keyDoc {
			// try to preserve natural ordering isn't possible with map; normalize handles reordering
			d = append(d, bson.E{Key: k, Value: v})
		}
		out[normalizeKey(d)] = struct{}{}
	}
	return out, nil
}

// normalizeKey builds a stable string like "field1:1.field2:-1"
func normalizeKey(d bson.D) string {
	if len(d) == 0 {
		return ""
	}
	// Sort by field name for stable comparison
	type kv struct {
		k string
		v interface{}
	}
	arr := make([]kv, 0, len(d))
	for _, e := range d {
		arr = append(arr, kv{k: e.Key, v: e.Value})
	}
	// simple bubble (len small) or use map then sort
	for i := 0; i < len(arr); i++ {
		for j := i + 1; j < len(arr); j++ {
			if arr[j].k < arr[i].k {
				arr[i], arr[j] = arr[j], arr[i]
			}
		}
	}
	var sb strings.Builder
	for i, e := range arr {
		if i > 0 {
			sb.WriteString(".")
		}
		sb.WriteString(e.k)
		sb.WriteString(":")
		switch v := e.v.(type) {
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

// keyAsJS returns a JS object literal for createIndex keys, e.g. { user_id: 1, timestamp: 1 }
func keyAsJS(d bson.D) string {
	if len(d) == 0 {
		return "{}"
	}
	// Preserve original order provided in spec
	var parts []string
	for _, e := range d {
		parts = append(parts, fmt.Sprintf("%s: %v", e.Key, e.Value))
	}
	return "{ " + strings.Join(parts, ", ") + " }"
}

func canonicalIndexSpecs() map[string][]IndexSpec {
	return map[string][]IndexSpec{
		"media": {
			{Name: "_id_", Key: bson.D{{Key: "_id", Value: 1}}},
			{Name: "startTimestamp_1", Key: bson.D{{Key: "startTimestamp", Value: 1}}},
			{Name: "deviceId_1", Key: bson.D{{Key: "deviceId", Value: 1}}},
			{Name: "organisationId_1", Key: bson.D{{Key: "organisationId", Value: 1}}},
			{Name: "tags_1", Key: bson.D{{Key: "tags", Value: 1}}},
			{Name: "detections_1", Key: bson.D{{Key: "detections", Value: 1}}},
			{Name: "duration_1", Key: bson.D{{Key: "duration", Value: 1}}},
			{Name: "deviceId_1_startTimestamp_1", Key: bson.D{{Key: "deviceId", Value: 1}, {Key: "startTimestamp", Value: 1}}},
			{Name: "organisationId_1_startTimestamp_1", Key: bson.D{{Key: "organisationId", Value: 1}, {Key: "startTimestamp", Value: 1}}},
			{Name: "organisationId_1_deviceId_1_startTimestamp_1_endTimestamp_1", Key: bson.D{
				{Key: "organisationId", Value: 1},
				{Key: "deviceId", Value: 1},
				{Key: "startTimestamp", Value: 1},
				{Key: "endTimestamp", Value: 1},
			}},
			{Name: "startTimestamp_1__id_1", Key: bson.D{{Key: "startTimestamp", Value: 1}, {Key: "_id", Value: 1}}},
		},
		"settings": {
			{Name: "_id_", Key: bson.D{{Key: "_id", Value: 1}}},
		},
		"subscriptions": {
			{Name: "_id_", Key: bson.D{{Key: "_id", Value: 1}}},
		},
		"groups": {
			{Name: "_id_", Key: bson.D{{Key: "_id", Value: 1}}},
		},
		"marker_options": {
			{Name: "_id_", Key: bson.D{{Key: "_id", Value: 1}}},
			{Name: "value_1", Key: bson.D{{Key: "value", Value: 1}}},
			{Name: "text_1", Key: bson.D{{Key: "text", Value: 1}}},
		},
		"devices": {
			{Name: "_id_", Key: bson.D{{Key: "_id", Value: 1}}},
		},
		"event_options": {
			{Name: "_id_", Key: bson.D{{Key: "_id", Value: 1}}},
			{Name: "value_1", Key: bson.D{{Key: "value", Value: 1}}},
			{Name: "text_1", Key: bson.D{{Key: "text", Value: 1}}},
		},
		"users": {
			{Name: "_id_", Key: bson.D{{Key: "_id", Value: 1}}},
			{Name: "username_1", Key: bson.D{{Key: "username", Value: 1}}, Unique: true},
			{Name: "email_1", Key: bson.D{{Key: "email", Value: 1}}, Unique: true},
			{Name: "amazon_access_key_id_1", Key: bson.D{{Key: "amazon_access_key_id", Value: 1}}, Unique: true},
			{Name: "amazon_secret_access_key_1", Key: bson.D{{Key: "amazon_secret_access_key", Value: 1}}, Unique: true},
		},
		"marker_option_ranges": {
			{Name: "_id_", Key: bson.D{{Key: "_id", Value: 1}}},
			{Name: "value_1", Key: bson.D{{Key: "value", Value: 1}}},
			{Name: "start_1", Key: bson.D{{Key: "start", Value: 1}}},
			{Name: "end_1", Key: bson.D{{Key: "end", Value: 1}}},
			{Name: "deviceId_1", Key: bson.D{{Key: "deviceId", Value: 1}}},
			{Name: "organisationId_1_deviceId_1_end_1_groupId_1_start_1_text_1", Key: bson.D{
				{Key: "organisationId", Value: 1},
				{Key: "deviceId", Value: 1},
				{Key: "end", Value: 1},
				{Key: "groupId", Value: 1},
				{Key: "start", Value: 1},
				{Key: "text", Value: 1},
			}},
			{Name: "organisationId_1_text_1_start_1_end_1_deviceId_1", Key: bson.D{
				{Key: "organisationId", Value: 1},
				{Key: "text", Value: 1},
				{Key: "start", Value: 1},
				{Key: "end", Value: 1},
				{Key: "deviceId", Value: 1},
			}},
		},
		"tag_options": {
			{Name: "_id_", Key: bson.D{{Key: "_id", Value: 1}}},
			{Name: "value_1", Key: bson.D{{Key: "value", Value: 1}}},
			{Name: "text_1", Key: bson.D{{Key: "text", Value: 1}}},
		},
		"sites": {
			{Name: "_id_", Key: bson.D{{Key: "_id", Value: 1}}},
		},
		"event_option_ranges": {
			{Name: "_id_", Key: bson.D{{Key: "_id", Value: 1}}},
			{Name: "value_1", Key: bson.D{{Key: "value", Value: 1}}},
			{Name: "start_1", Key: bson.D{{Key: "start", Value: 1}}},
			{Name: "end_1", Key: bson.D{{Key: "end", Value: 1}}},
			{Name: "deviceId_1", Key: bson.D{{Key: "deviceId", Value: 1}}},
			{Name: "organisationId_1_text_1", Key: bson.D{{Key: "organisationId", Value: 1}, {Key: "text", Value: 1}}},
			{Name: "organisationId_1_text_1_start_1_end_1_deviceId_1", Key: bson.D{
				{Key: "organisationId", Value: 1},
				{Key: "text", Value: 1},
				{Key: "start", Value: 1},
				{Key: "end", Value: 1},
				{Key: "deviceId", Value: 1},
			}},
			{Name: "organisationId_1_deviceId_1_groupId_1_start_1_end_1_text_1", Key: bson.D{
				{Key: "organisationId", Value: 1},
				{Key: "deviceId", Value: 1},
				{Key: "groupId", Value: 1},
				{Key: "start", Value: 1},
				{Key: "end", Value: 1},
				{Key: "text", Value: 1},
			}},
		},
		"tag_option_ranges": {
			{Name: "_id_", Key: bson.D{{Key: "_id", Value: 1}}},
			{Name: "value_1", Key: bson.D{{Key: "value", Value: 1}}},
			{Name: "start_1", Key: bson.D{{Key: "start", Value: 1}}},
			{Name: "end_1", Key: bson.D{{Key: "end", Value: 1}}},
			{Name: "deviceId_1", Key: bson.D{{Key: "deviceId", Value: 1}}},
			{Name: "organisationId_1_text_1_start_1_end_1_deviceId_1", Key: bson.D{
				{Key: "organisationId", Value: 1},
				{Key: "text", Value: 1},
				{Key: "start", Value: 1},
				{Key: "end", Value: 1},
				{Key: "deviceId", Value: 1},
			}},
		},
		"markers": {
			{Name: "_id_", Key: bson.D{{Key: "_id", Value: 1}}},
			{Name: "startTimestamp_1", Key: bson.D{{Key: "startTimestamp", Value: 1}}},
			{Name: "deviceId_1", Key: bson.D{{Key: "deviceId", Value: 1}}},
			{Name: "organisationId_1", Key: bson.D{{Key: "organisationId", Value: 1}}},
			{Name: "tags.name_1", Key: bson.D{{Key: "tags.name", Value: 1}}},
			{Name: "duration_1", Key: bson.D{{Key: "duration", Value: 1}}},
			{Name: "name_1", Key: bson.D{{Key: "name", Value: 1}}},
		},
	}
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
			Name:   name,
			Key:    key,
			Unique: extractUnique(p),
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
	start := strings.Index(obj, "key:")
	if start == -1 {
		return ""
	}
	s := strings.TrimSpace(obj[start+4:])
	if !strings.HasPrefix(s, "{") {
		return ""
	}
	depth := 0
	var b strings.Builder
	for _, r := range s {
		if r == '{' {
			depth++
		}
		b.WriteRune(r)
		if r == '}' {
			depth--
			if depth == 0 {
				break
			}
		}
	}
	return b.String()
}

func extractUnique(obj string) bool {
	// Detect "unique: true" in object (optional; not present in most dumps)
	return strings.Contains(obj, "unique: true")
}

func parseKeyFields(doc string) bson.D {
	doc = strings.TrimSpace(doc)
	doc = strings.TrimPrefix(doc, "{")
	doc = strings.TrimSuffix(doc, "}")
	doc = strings.TrimSpace(doc)
	if doc == "" {
		return bson.D{}
	}

	var (
		parts   []string
		cur     strings.Builder
		inQuote rune
	)

	for _, r := range doc {
		switch r {
		case '\'', '"':
			if inQuote == 0 {
				inQuote = r
			} else if inQuote == r {
				inQuote = 0
			}
			cur.WriteRune(r)
		case ',':
			if inQuote == 0 {
				segment := strings.TrimSpace(cur.String())
				if segment != "" {
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
	if strings.HasPrefix(s, "+") {
		s = strings.TrimPrefix(s, "+")
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

// CLI entry wrapper to match SeedMedia signature, if you prefer calling like others.
func RunCheckIndexesCLI(
	mongodbURI string,
	dbName string,
	collections string,
	autoFix bool,
	mode string,
	indexesFilePath string,
) {
	if err := runCheckIndexesInternal(mongodbURI, dbName, collections, autoFix, mode, indexesFilePath); err != nil {
		fmt.Printf("[error] check-indexes: %v\n", err)
		os.Exit(1)
	}
}

// If you want a single-return API:
func runCheckIndexesInternal(
	mongoURI string,
	dbName string,
	collectionsCSV string,
	autoFix bool,
	mode string,
	indexesFilePath string,
) error {
	// Delegate to CheckIndexes which handles prompts and prints. Keeping a simple API surface.
	CheckIndexes(mongoURI, dbName, collectionsCSV, autoFix, mode, indexesFilePath)
	return nil
}
