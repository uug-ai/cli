package loadtest

import (
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/uug-ai/models/pkg/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func fixtureManifest() Manifest {
	return Manifest{
		Schema: Schema, RunID: "fixture-test", CreatedAt: time.Unix(1789718400, 0).UTC(), Timestamp: 1789718400,
		Organisations: 2, ProjectsPerOrganisation: 2, DevicesPerProject: 2,
		HistoryPerDevice: 2, Rate: 16, Duration: time.Second, Concurrency: 1, Total: 16, State: "prepared",
	}
}

func TestBuildEventSchemaAndLegacyFilename(t *testing.T) {
	m := fixtureManifest()
	keys := map[string]bool{}
	traces := map[string]bool{}
	for index := 0; index < m.Total; index++ {
		event, err := BuildEvent(m, index)
		if err != nil {
			t.Fatal(err)
		}
		body, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		var decoded models.PipelineEvent
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.MonitorStage != nil || decoded.EventStage != nil || decoded.Payload.SignedURL != "" ||
			decoded.Operation != "event" || decoded.Request != "persist" || decoded.Storage != "kstorage" || decoded.Provider != "loadtest" {
			t.Fatalf("event must exercise real monitor checks without signed URLs: %s", body)
		}
		if !reflect.DeepEqual(decoded.Stages, []string{"monitor", "sequence", "loadtest-fixture-test"}) ||
			CompletionQueue(m) != "kcloud-loadtest-fixture-test-queue" {
			t.Fatalf("wrong completion chain: %v", decoded.Stages)
		}
		if !strings.Contains(string(body), `"productid":`) {
			t.Fatalf("missing structured productid metadata: %s", body)
		}
		trace, err := hex.DecodeString(decoded.TraceId)
		if err != nil || len(trace) != 16 || traces[decoded.TraceId] {
			t.Fatalf("invalid/duplicate trace: %q", decoded.TraceId)
		}
		traces[decoded.TraceId] = true
		key := RecordingKey(m, index)
		if key != decoded.Payload.FileName || keys[key] {
			t.Fatalf("invalid/duplicate key: %s", key)
		}
		keys[key] = true
		identity, err := ExpectedIdentity(m, index)
		if err != nil {
			t.Fatal(err)
		}
		media, err := decoded.GetMedia()
		if err != nil || media.Duration != 30000 || media.StartTimestamp != m.Timestamp ||
			media.DeviceKey != identity.DeviceKey || media.Metadata.FPS != 25 || media.Metadata.FileSize != 1024*1024 {
			t.Fatalf("structured event schema cannot be read by models: %+v / %v", media, err)
		}
		decoded.Payload.Metadata.DeviceId = ""
		legacyMedia, err := decoded.GetMedia()
		if err != nil || legacyMedia.DeviceKey != identity.DeviceKey || legacyMedia.Duration != 30000 ||
			legacyMedia.StartTimestamp != m.Timestamp {
			t.Fatalf("legacy six-part filename invalid: %+v / %v", legacyMedia, err)
		}
	}
}

func TestFixtureIdentityStableAndScoped(t *testing.T) {
	m := fixtureManifest()
	other := m
	other.RunID = "another-run"
	seen := map[primitive.ObjectID]bool{}
	for device := 0; device < fixtureDeviceCount(m); device++ {
		identity := identityForDevice(m, device)
		again, err := ExpectedIdentity(m, device+fixtureDeviceCount(m))
		if err != nil || identity != again {
			t.Fatalf("identity not stable across rounds: %v / %v", again, err)
		}
		if identity == identityForDevice(other, device) || identity.DeviceID.IsZero() ||
			identity.ProjectID.IsZero() || identity.OrganisationID.IsZero() || seen[identity.DeviceID] {
			t.Fatalf("zero or overlapping identity: %+v", identity)
		}
		seen[identity.DeviceID] = true
		projectNumber := device / m.DevicesPerProject % m.ProjectsPerOrganisation
		if (identity.ProjectID == identity.OrganisationID) != (projectNumber == 0) {
			t.Fatalf("default project identity violated: %+v", identity)
		}
	}
}

func decodeFixture(t *testing.T, doc bson.M, into any) {
	t.Helper()
	data, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := bson.Unmarshal(data, into); err != nil {
		t.Fatal(err)
	}
}

func TestFixturesUseRealModelBSONTypes(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "canonical", true: "legacy"}[legacy], func(t *testing.T) {
			m := fixtureManifest()
			m.Legacy = legacy
			if legacy {
				m.ProjectsPerOrganisation = 1
			}
			counts := map[string]int{}
			liveKeys := map[string]bool{}
			for index := 0; index < m.Total; index++ {
				liveKeys[RecordingKey(m, index)] = true
			}
			historyKeys := map[string]bool{}
			err := walkFixtures(m, func(collection string, doc bson.M) error {
				counts[collection]++
				if doc["loadtestRunID"] != m.RunID || doc["loadtestSchema"] != Schema {
					t.Fatalf("fixture without ownership tag: %v", doc)
				}
				switch collection {
				case "users":
					var user models.User
					decodeFixture(t, doc, &user)
					if user.Id.IsZero() || user.Email == "" || user.Username == "" || user.Timezone != "UTC" ||
						len(user.Activity) != 1 || user.Activity[0].Day != "18-09-2026" || user.Activity[0].Devices == nil {
						t.Fatalf("monitor user fixture invalid: %+v", user)
					}
					if len(user.Days) != 0 {
						t.Fatal("expected dates must not be preseeded")
					}
				case "devices":
					var device models.Device
					decodeFixture(t, doc, &device)
					if device.Id.IsZero() || device.Key == "" || device.UserId == "" {
						t.Fatalf("invalid stable device: %+v", device)
					}
					_, hasOrg := doc["organisationId"]
					_, hasProject := doc["projectId"]
					if legacy == hasOrg || legacy == hasProject {
						t.Fatalf("incorrect profile ownership fields: %v", doc)
					}
					if !legacy && (device.ProjectId == nil || device.ProjectId.IsZero() || device.OrganisationId != device.UserId) {
						t.Fatalf("wrong canonical BSON types: %+v", device)
					}
				case "subscriptions":
					var subscription models.Subscription
					decodeFixture(t, doc, &subscription)
					if subscription.UserId == "" || subscription.StripePlan != "loadtest" ||
						subscription.Status != "active" || !subscription.EndsAt.IsZero() {
						t.Fatalf("subscription not active: %+v", subscription)
					}
					_, canonical := doc["organisation_id"]
					if canonical == legacy {
						t.Fatalf("wrong subscription profile: %v", doc)
					}
				case "media":
					var media models.Media
					decodeFixture(t, doc, &media)
					if liveKeys[media.VideoFile] || historyKeys[media.VideoFile] || media.ProjectId == nil ||
						media.StartTimestamp != m.Timestamp-86400 || media.Duration != 30000 || media.Metadata.FPS != 25 {
						t.Fatalf("invalid or overlapping history fixture: %+v", media)
					}
					historyKeys[media.VideoFile] = true
				case "project":
					if _, ok := doc["organisationId"].(primitive.ObjectID); !ok {
						t.Fatalf("project organisationId must be BSON ObjectID: %v", doc)
					}
				case "organisation":
					if doc["ownerId"] != doc["_id"] {
						t.Fatalf("organisation must use stable owner: %v", doc)
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if counts["users"] != m.Organisations || counts["subscriptions"] != m.Organisations ||
				counts["devices"] != fixtureDeviceCount(m) || counts["media"] != fixtureDeviceCount(m)*m.HistoryPerDevice {
				t.Fatalf("wrong fixture cardinalities: %v", counts)
			}
			if legacy && (counts["organisation"] != 0 || counts["project"] != 0) {
				t.Fatalf("legacy fixtures must exercise owner calendar fallback: %v", counts)
			}
		})
	}
}

func TestSharedPlanFixtureDecodesAsRealSettings(t *testing.T) {
	var settings models.Settings
	decodeFixture(t, planFixture(), &settings)
	if settings.Id.IsZero() || settings.Key != "plan" || len(settings.Map) != 1 {
		t.Fatalf("invalid monitor settings fixture: %+v", settings)
	}
	var repeated models.Settings
	decodeFixture(t, planFixture(), &repeated)
	if settings.Id != repeated.Id {
		t.Fatal("shared plan identity must remain stable across preparations")
	}
	encoded, err := bson.Marshal(settings.Map["loadtest"])
	if err != nil {
		t.Fatal(err)
	}
	var plan models.Plan
	if err := bson.Unmarshal(encoded, &plan); err != nil {
		t.Fatal(err)
	}
	if plan.DayLimit != 30 || plan.Level != 5 || plan.UploadLimit != 99999999 ||
		plan.VideoLimit != 99999999 || plan.Usage != 99999999 {
		t.Fatalf("monitor plan fixture cannot be decoded: %+v", plan)
	}
}

func TestFixtureValidationWithoutCLI(t *testing.T) {
	cases := []func(*Manifest){
		func(m *Manifest) { m.RunID = "unsafe.run" },
		func(m *Manifest) { m.Schema = "other" },
		func(m *Manifest) { m.Total = 0 },
		func(m *Manifest) { m.Organisations = 0 },
		func(m *Manifest) { m.ProjectsPerOrganisation = -1 },
		func(m *Manifest) { m.DevicesPerProject = 0 },
		func(m *Manifest) { m.HistoryPerDevice = -1 },
		func(m *Manifest) { m.Timestamp = 0 },
		func(m *Manifest) { m.Legacy = true },
		func(m *Manifest) { m.Organisations = int(^uint(0) >> 1) },
		func(m *Manifest) { m.HistoryPerDevice = 1000001 },
	}
	for _, change := range cases {
		m := fixtureManifest()
		change(&m)
		if _, err := BuildEvent(m, 0); err == nil {
			t.Fatalf("invalid fixture accepted: %+v", m)
		}
	}
	for _, index := range []int{-1, fixtureManifest().Total} {
		if _, err := BuildEvent(fixtureManifest(), index); err == nil || RecordingKey(fixtureManifest(), index) != "" {
			t.Fatalf("invalid index %d accepted", index)
		}
	}
}

func TestCalendarTargetsOnlyTouchedScopes(t *testing.T) {
	m := fixtureManifest()
	m.Total = 3
	canonical := calendarTargets(m)
	if len(canonical["project"]) != 2 || len(canonical["project_device_days"]) != 3 || len(canonical["users"]) != 0 {
		t.Fatalf("wrong canonical targets: %v", canonical)
	}
	m.Legacy, m.ProjectsPerOrganisation = true, 1
	legacy := calendarTargets(m)
	if len(legacy["users"]) != 2 || len(legacy["project"]) != 0 || len(legacy["project_device_days"]) != 0 {
		t.Fatalf("wrong legacy targets: %v", legacy)
	}
}
