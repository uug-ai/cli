package actions

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const hubIndexSnapshot = "hub-19-10-2026"

// supersededContractIndexes lists migration contract indexes the Hub snapshot
// intentionally replaces. Keys are "<file>/<collection>/<index name>".
var supersededContractIndexes = map[string]string{
	// hub-api creates detections_key_runid_unique on the same keys; MongoDB
	// cannot hold a unique and a non-unique index on one key pattern.
	"migration-hub-analysis-detection-ownership-27-08-2026/detections/key_1_source.runId_1": "unique in hub-api",
	// hub-cleanup replaces the legacy run retention indexes with partial
	// {userid: {$exists: true}} variants and drops the full ones.
	"migration-hub-workflow-ownership-26-08-2026/workflow_runs/userid_1_recordingtimestamp_1__id_1": "partial _legacy in hub-cleanup",
	"migration-hub-workflow-ownership-26-08-2026/workflow_runs/userid_1_start_1__id_1":              "partial _legacy in hub-cleanup",
}

// The snapshot is the full Hub index set, so every hub migration contract must
// be present in it. migration-hub-12-01-2026 is the one-off sequences/media
// migration whose indexes were pruned before the 16-09-2026 snapshot.
func TestHubIndexSnapshotCoversMigrationContracts(t *testing.T) {
	snapshot, err := loadCanonicalIndexSpecsFromFile(filepath.Join("..", "indexes", hubIndexSnapshot+".txt"))
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}

	contracts, err := filepath.Glob(filepath.Join("..", "indexes", "migration-hub-*.txt"))
	if err != nil || len(contracts) == 0 {
		t.Fatalf("glob contracts: %v (%d files)", err, len(contracts))
	}

	for _, path := range contracts {
		version := strings.TrimSuffix(filepath.Base(path), ".txt")
		if version == "migration-hub-12-01-2026" {
			continue
		}
		contract, err := loadCanonicalIndexSpecsFromFile(path)
		if err != nil {
			t.Fatalf("load %s: %v", version, err)
		}
		for coll, specs := range contract {
			for _, spec := range specs {
				if _, ok := supersededContractIndexes[version+"/"+coll+"/"+spec.Name]; ok {
					continue
				}
				found := false
				for _, candidate := range snapshot[coll] {
					if indexSpecMatches(spec, candidate) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("%s: %s.%s (%s) missing from %s", version, coll, spec.Name, normalizeKey(spec.Key), hubIndexSnapshot)
				}
			}
		}
	}
}

// The parser keeps only the last block for a repeated collection header, so a
// duplicated header would silently drop indexes.
func TestHubIndexSnapshotHasOneBlockPerCollection(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "indexes", hubIndexSnapshot+".txt"))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "[") || strings.HasPrefix(line, "]") ||
			strings.HasPrefix(line, "{") || strings.HasPrefix(line, "}") || strings.Contains(line, ":") {
			continue
		}
		if seen[line] {
			t.Errorf("collection %q declared twice", line)
		}
		seen[line] = true
	}
}

func TestStorageIndexSnapshotCoversPreviousFiles(t *testing.T) {
	snapshot, err := loadCanonicalIndexSpecsFromFile(filepath.Join("..", "indexes", "storage-19-10-2026.txt"))
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	for _, version := range []string{"storage-09-12-2025", "migration-storage-12-01-2026"} {
		previous, err := loadCanonicalIndexSpecsFromFile(filepath.Join("..", "indexes", version+".txt"))
		if err != nil {
			t.Fatalf("load %s: %v", version, err)
		}
		for coll, specs := range previous {
			for _, spec := range specs {
				found := false
				for _, candidate := range snapshot[coll] {
					if indexSpecMatches(spec, candidate) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("%s: %s.%s missing from storage snapshot", version, coll, spec.Name)
				}
			}
		}
	}
	if got := len(snapshot["media"]); got != 14 {
		t.Errorf("storage media indexes = %d, want 14", got)
	}
}
