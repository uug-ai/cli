package actions

import (
	"path/filepath"
	"testing"
)

// productionReleases pins the Hub index set of each production release. The
// file holds the indexes the release's service
// versions need, nothing that ships later, so a check against it stays valid
// while hub-19-10-2026 grows.
var productionReleases = []struct {
	version string
	// contracts are the migration-hub contracts the release must contain.
	contracts []string
	// absent are collections whose indexes ship after the release.
	absent []string
}{
	{
		version: "production-2026-11-18",
		contracts: []string{
			"migration-hub-channels-20-10-2026",
			"migration-hub-media-forwardable-26-10-2026",
			"migration-hub-audit-export-cursor-02-11-2026",
		},
		absent: []string{"teams", "team_members", "role_assignments"},
	},
}

func TestProductionIndexFiles(t *testing.T) {
	for _, release := range productionReleases {
		t.Run(release.version, func(t *testing.T) {
			specs, err := loadCanonicalIndexSpecsFromFile(filepath.Join("..", "indexes", release.version+".txt"))
			if err != nil {
				t.Fatalf("load %s: %v", release.version, err)
			}
			for _, contractVersion := range release.contracts {
				contract, err := loadCanonicalIndexSpecsFromFile(filepath.Join("..", "indexes", contractVersion+".txt"))
				if err != nil {
					t.Fatalf("load %s: %v", contractVersion, err)
				}
				for coll, required := range contract {
					for _, spec := range required {
						found := false
						for _, candidate := range specs[coll] {
							if indexSpecMatches(spec, candidate) {
								found = true
								break
							}
						}
						if !found {
							t.Errorf("%s: %s.%s missing", contractVersion, coll, spec.Name)
						}
					}
				}
			}
			for _, coll := range release.absent {
				if len(specs[coll]) > 0 {
					t.Errorf("%s declares %s indexes, which ship after this release", release.version, coll)
				}
			}
		})
	}
}
