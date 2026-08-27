package actions

import (
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestValidateOrganisationsBackfillConfig(t *testing.T) {
	valid := normalizeOrganisationsBackfillConfig(OrganisationsBackfillConfig{
		Mode:                       "dry-run",
		MongoDBURI:                 "mongodb://localhost:27017",
		MongoDBDestinationDatabase: "hub",
		Collection:                 "sites",
	})

	tests := []struct {
		name      string
		mutate    func(OrganisationsBackfillConfig) OrganisationsBackfillConfig
		wantError string
	}{
		{
			name: "valid collection",
			mutate: func(config OrganisationsBackfillConfig) OrganisationsBackfillConfig {
				return config
			},
		},
		{
			name: "requires connection settings",
			mutate: func(config OrganisationsBackfillConfig) OrganisationsBackfillConfig {
				config.MongoDBURI = ""
				return config
			},
			wantError: "mongodb-uri",
		},
		{
			name: "requires selection",
			mutate: func(config OrganisationsBackfillConfig) OrganisationsBackfillConfig {
				config.Collection = ""
				return config
			},
			wantError: "exactly one",
		},
		{
			name: "rejects collection and all",
			mutate: func(config OrganisationsBackfillConfig) OrganisationsBackfillConfig {
				config.AllCollections = true
				return config
			},
			wantError: "mutually exclusive",
		},
		{
			name: "rejects live mode",
			mutate: func(config OrganisationsBackfillConfig) OrganisationsBackfillConfig {
				config.Mode = "live"
				return config
			},
			wantError: "live mode is disabled",
		},
		{
			name: "rejects index writes",
			mutate: func(config OrganisationsBackfillConfig) OrganisationsBackfillConfig {
				config.ApplyMigrationIndexes = true
				return config
			},
			wantError: "index creation is disabled",
		},
		{
			name: "rejects invalid document id",
			mutate: func(config OrganisationsBackfillConfig) OrganisationsBackfillConfig {
				config.DocumentID = "invalid"
				return config
			},
			wantError: "invalid document-id",
		},
		{
			name: "accepts scoped subscriptions",
			mutate: func(config OrganisationsBackfillConfig) OrganisationsBackfillConfig {
				config.Collection = "subscriptions"
				config.OrganisationID = "507f1f77bcf86cd799439011"
				return config
			},
		},
		{
			name: "accepts scoped sites",
			mutate: func(config OrganisationsBackfillConfig) OrganisationsBackfillConfig {
				config.OrganisationID = "507f1f77bcf86cd799439011"
				return config
			},
		},
		{
			name: "rejects invalid scope",
			mutate: func(config OrganisationsBackfillConfig) OrganisationsBackfillConfig {
				config.Collection = "subscriptions"
				config.OrganisationID = "invalid"
				return config
			},
			wantError: "invalid organisation-id",
		},
		{
			name: "reports blocked adapter",
			mutate: func(config OrganisationsBackfillConfig) OrganisationsBackfillConfig {
				config.Collection = "channels"
				return config
			},
			wantError: "blocked",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := validateOrganisationsBackfillConfig(test.mutate(valid))
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("validateOrganisationsBackfillConfig() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("validateOrganisationsBackfillConfig() error = %v, want containing %q", err, test.wantError)
			}
		})
	}
}

func TestSelectOrganisationsBackfillAdaptersAllIsDeterministic(t *testing.T) {
	adapters, err := selectOrganisationsBackfillAdapters("", true)
	if err != nil {
		t.Fatalf("selectOrganisationsBackfillAdapters() error = %v", err)
	}
	want := []string{"alerts", "counting", "devices", "groups", "heatmap", "io", "labels", "sites", "subscriptions", "videowalls"}
	if len(adapters) != len(want) {
		t.Fatalf("len(adapters) = %d, want %d", len(adapters), len(want))
	}
	for index := range want {
		if adapters[index].Collection != want[index] {
			t.Errorf("adapters[%d].Collection = %q, want %q", index, adapters[index].Collection, want[index])
		}
	}
}

func TestSubscriptionsAdapterDeclaresDeploymentPrerequisites(t *testing.T) {
	adapter := organisationsBackfillAdapters()["subscriptions"]
	if adapter.OwnershipScope != "organisation-only" || adapter.ProjectField != "" {
		t.Fatalf("subscription ownership scope = %q project field = %q", adapter.OwnershipScope, adapter.ProjectField)
	}
	if len(adapter.MinimumWriterVersions) == 0 || len(adapter.MinimumReaderVersions) != 5 {
		t.Fatalf("subscription deployment prerequisites = writers %#v readers %#v", adapter.MinimumWriterVersions, adapter.MinimumReaderVersions)
	}
}

func TestAlertsAdapterDeclaresProjectScope(t *testing.T) {
	adapter := organisationsBackfillAdapters()["alerts"]
	if adapter.OwnershipScope != "project-scoped" || adapter.ProjectField != "projectId" || adapter.ProjectBSONType != "objectId" {
		t.Fatalf("alert ownership scope = %+v", adapter)
	}
}

func TestSitesAdapterDeclaresProjectScope(t *testing.T) {
	adapter := organisationsBackfillAdapters()["sites"]
	if adapter.OwnershipScope != "project-scoped" || adapter.ProjectField != "projectId" || adapter.ProjectBSONType != "objectId" || adapter.ResolverKind != organisationsBackfillResolverSite {
		t.Fatalf("site ownership scope = %+v", adapter)
	}
}

func TestGroupsAdapterDeclaresProjectScope(t *testing.T) {
	adapter := organisationsBackfillAdapters()["groups"]
	if adapter.OwnershipScope != "project-scoped" || adapter.ProjectField != "projectId" || adapter.ProjectBSONType != "objectId" || adapter.ResolverKind != organisationsBackfillResolverGroup {
		t.Fatalf("group ownership scope = %+v", adapter)
	}
}

func TestIOAdapterDeclaresProjectScopeAndActorFallback(t *testing.T) {
	adapter := organisationsBackfillAdapters()["io"]
	if adapter.OwnershipScope != "project-scoped" || adapter.ProjectField != "projectId" || adapter.ProjectBSONType != "objectId" || adapter.ResolverKind != organisationsBackfillResolverIO {
		t.Fatalf("IO ownership scope = %+v", adapter)
	}
	if len(adapter.LegacyCandidates) != 1 || adapter.LegacyCandidates[0] != "user_id" || len(adapter.PreservedFields) != 1 || adapter.PreservedFields[0] != "user_id" {
		t.Fatalf("IO actor contract = %+v", adapter)
	}
}

func TestLabelsAdapterDeclaresStableOwnerScope(t *testing.T) {
	adapter := organisationsBackfillAdapters()["labels"]
	if adapter.OwnershipScope != "project-scoped" || adapter.ProjectField != "projectId" || adapter.ResolverKind != organisationsBackfillResolverLabel {
		t.Fatalf("label ownership scope = %+v", adapter)
	}
	if len(adapter.LegacyCandidates) != 1 || adapter.LegacyCandidates[0] != "owner_id" || len(adapter.PreservedFields) != 1 || adapter.PreservedFields[0] != "user_id" {
		t.Fatalf("label stable owner/provenance contract = %+v", adapter)
	}
}

func TestCountingAdapterDeclaresSourceOwnership(t *testing.T) {
	adapter := organisationsBackfillAdapters()["counting"]
	if adapter.ResolverKind != organisationsBackfillResolverCounting || len(adapter.LegacyCandidates) != 1 || adapter.LegacyCandidates[0] != "user_id" {
		t.Fatalf("counting ownership contract = %+v", adapter)
	}
}

func TestVideowallAdapterDeclaresTenantOwnership(t *testing.T) {
	adapter := organisationsBackfillAdapters()["videowalls"]
	if adapter.ResolverKind != organisationsBackfillResolverVideowall || len(adapter.LegacyCandidates) != 1 || adapter.LegacyCandidates[0] != "master_user_id" {
		t.Fatalf("videowall ownership contract = %+v", adapter)
	}
}

func TestOrganisationsBackfillIndexCoversField(t *testing.T) {
	key := bson.D{{Key: "organisationId", Value: 1}, {Key: "timestamp", Value: -1}}
	if !organisationsBackfillIndexCoversField(key, "organisationId") {
		t.Fatal("organisationsBackfillIndexCoversField() did not find leading field")
	}
	if organisationsBackfillIndexCoversField(key, "user_id") {
		t.Fatal("organisationsBackfillIndexCoversField() found absent field")
	}
}

func TestOrganisationsBackfillExitCode(t *testing.T) {
	err := &organisationsBackfillError{code: organisationsBackfillExitData, err: errTestDataConflict}
	if got := OrganisationsBackfillExitCode(err); got != organisationsBackfillExitData {
		t.Fatalf("OrganisationsBackfillExitCode() = %d, want %d", got, organisationsBackfillExitData)
	}
}

var errTestDataConflict = &testBackfillError{"data conflict"}

type testBackfillError struct {
	message string
}

func (e *testBackfillError) Error() string {
	return e.message
}
