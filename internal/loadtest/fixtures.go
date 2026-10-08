package loadtest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/uug-ai/models/pkg/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const fixtureBatchSize = 256

var fixtureRunID = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,47}$`)

// FixtureIdentity uses the same owner for the organisation and its default project.
type FixtureIdentity struct {
	OrganisationID primitive.ObjectID
	ProjectID      primitive.ObjectID
	DeviceID       primitive.ObjectID
	DeviceKey      string
	Username       string
}

func fixtureID(m Manifest, kind string, index int) primitive.ObjectID {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%s\x00%d", Schema, m.RunID, kind, index)))
	var id primitive.ObjectID
	copy(id[:], sum[:12])
	return id
}

func validateFixtureManifest(m Manifest) error {
	if m.Schema != Schema || !fixtureRunID.MatchString(m.RunID) {
		return fmt.Errorf("invalid load-test manifest schema or run ID")
	}
	if m.Timestamp <= 0 || m.Organisations <= 0 || m.ProjectsPerOrganisation <= 0 ||
		m.DevicesPerProject <= 0 || m.HistoryPerDevice < 0 || m.Total <= 0 {
		return fmt.Errorf("invalid load-test fixture dimensions, total or timestamp")
	}
	maxInt := int(^uint(0) >> 1)
	if m.Organisations > maxInt/m.ProjectsPerOrganisation ||
		m.Organisations*m.ProjectsPerOrganisation > maxInt/m.DevicesPerProject {
		return fmt.Errorf("fixture dimensions overflow")
	}
	devices := fixtureDeviceCount(m)
	if devices > 100000 || m.HistoryPerDevice > 100000 ||
		(m.HistoryPerDevice > 0 && devices > 1000000/m.HistoryPerDevice) || m.Total > 1000000 {
		return fmt.Errorf("fixture dimensions exceed harness safety limits")
	}
	if m.Legacy && m.ProjectsPerOrganisation != 1 {
		return fmt.Errorf("legacy fixtures require exactly one default project per organisation")
	}
	return nil
}

func identityForDevice(m Manifest, device int) FixtureIdentity {
	project := device / m.DevicesPerProject
	organisation := project / m.ProjectsPerOrganisation
	orgID := fixtureID(m, "owner", organisation)
	projectID := orgID
	if project%m.ProjectsPerOrganisation != 0 {
		projectID = fixtureID(m, "project", project)
	}
	deviceID := fixtureID(m, "device", device)
	return FixtureIdentity{
		OrganisationID: orgID,
		ProjectID:      projectID,
		DeviceID:       deviceID,
		DeviceKey:      "lt" + deviceID.Hex(),
		Username:       fmt.Sprintf("loadtest-%s-o%d", m.RunID, organisation),
	}
}

func fixtureDeviceCount(m Manifest) int {
	return m.Organisations * m.ProjectsPerOrganisation * m.DevicesPerProject
}

// ExpectedIdentity maps events round-robin onto the frozen fixture devices.
func ExpectedIdentity(m Manifest, index int) (FixtureIdentity, error) {
	if err := validateFixtureManifest(m); err != nil {
		return FixtureIdentity{}, err
	}
	if index < 0 || index >= m.Total {
		return FixtureIdentity{}, fmt.Errorf("event index %d outside manifest total", index)
	}
	return identityForDevice(m, index%fixtureDeviceCount(m)), nil
}

func recordingKey(identity FixtureIdentity, timestamp int64, suffix string) string {
	// Keep all six legacy filename attributes, including the duration, parseable.
	return fmt.Sprintf("%s/%d_%s_%s_0_0_30000.mp4", identity.Username, timestamp, suffix, identity.DeviceKey)
}

func RecordingKey(m Manifest, index int) string {
	identity, err := ExpectedIdentity(m, index)
	if err != nil {
		return ""
	}
	return recordingKey(identity, m.Timestamp, strconv.Itoa(index))
}

func CompletionStage(m Manifest) string { return "loadtest-" + m.RunID }
func CompletionQueue(m Manifest) string { return "kcloud-" + CompletionStage(m) + "-queue" }

// BuildEvent describes synthetic metadata only; it neither uploads nor signs media.
func BuildEvent(m Manifest, index int) (models.PipelineEvent, error) {
	identity, err := ExpectedIdentity(m, index)
	if err != nil {
		return models.PipelineEvent{}, err
	}
	trace := sha256.Sum256([]byte(fmt.Sprintf("%s\x00trace\x00%d", m.RunID, index)))
	return models.PipelineEvent{
		Request:   "persist",
		Operation: "event",
		Stages:    []string{"monitor", "sequence", CompletionStage(m)},
		Storage:   "kstorage",
		Provider:  "loadtest",
		TraceId:   hex.EncodeToString(trace[:16]),
		Timestamp: m.Timestamp,
		FileName:  RecordingKey(m, index),
		Payload: models.PipelinePayload{
			Timestamp: m.Timestamp,
			FileName:  RecordingKey(m, index),
			FileSize:  1024 * 1024,
			Duration:  "30000",
			Metadata: models.PipelineMetadata{
				DeviceId:   identity.DeviceKey,
				DeviceName: identity.DeviceKey,
				Timestamp:  strconv.FormatInt(m.Timestamp, 10),
				Duration:   "30000",
				FPS:        "25",
			},
		},
	}, nil
}

func taggedFixture(m Manifest, id any) bson.M {
	return bson.M{"_id": id, "loadtestRunID": m.RunID, "loadtestSchema": Schema}
}

func planFixture() bson.M {
	return bson.M{
		"_id": fixtureID(Manifest{RunID: "shared-harness"}, "plan", 0), "key": "plan", "loadtestSchema": Schema,
		"map": bson.M{"loadtest": bson.M{
			"level": 5, "uploadLimit": 99999999, "videoLimit": 99999999,
			"usage": 99999999, "analysisLimit": 0, "dayLimit": 30,
		}},
	}
}

// walkFixtures keeps history generation bounded instead of materializing the workload.
func walkFixtures(m Manifest, emit func(string, bson.M) error) error {
	day := time.Unix(m.Timestamp, 0).UTC().Format("02-01-2006")
	for org := 0; org < m.Organisations; org++ {
		identity := identityForDevice(m, org*m.ProjectsPerOrganisation*m.DevicesPerProject)
		user := taggedFixture(m, identity.OrganisationID)
		user["username"], user["email"], user["timezone"] = identity.Username, identity.Username+"@example.invalid", "UTC"
		user["activity"] = bson.A{bson.M{
			"day": day, "timestamp": m.Timestamp, "requests": int64(0),
			"videos": int64(0), "usage": int64(0), "devices": bson.M{},
		}}
		user["dates"] = bson.A{}
		if !m.Legacy {
			user["organisationId"] = identity.OrganisationID
		}
		if err := emit("users", user); err != nil {
			return err
		}
		sub := taggedFixture(m, fixtureID(m, "subscription", org))
		sub["user_id"], sub["stripe_plan"], sub["status"] = identity.OrganisationID.Hex(), "loadtest", "active"
		sub["created_at"], sub["updated_at"] = m.CreatedAt, m.CreatedAt
		if !m.Legacy {
			sub["organisation_id"] = identity.OrganisationID
		}
		if err := emit("subscriptions", sub); err != nil {
			return err
		}
		if !m.Legacy {
			organisation := taggedFixture(m, identity.OrganisationID)
			organisation["ownerId"], organisation["name"] = identity.OrganisationID, identity.Username
			organisation["settings"] = bson.M{"timezone": "UTC"}
			if err := emit("organisation", organisation); err != nil {
				return err
			}
		}
	}
	for device := 0; device < fixtureDeviceCount(m); device++ {
		identity := identityForDevice(m, device)
		if !m.Legacy && device%m.DevicesPerProject == 0 {
			project := taggedFixture(m, identity.ProjectID)
			project["organisationId"], project["name"], project["dates"] = identity.OrganisationID, "loadtest", bson.A{}
			if err := emit("project", project); err != nil {
				return err
			}
		}
		doc := taggedFixture(m, identity.DeviceID)
		doc["key"], doc["name"], doc["type"], doc["user_id"] = identity.DeviceKey, identity.DeviceKey, "camera", identity.OrganisationID.Hex()
		if !m.Legacy {
			doc["organisationId"], doc["projectId"] = identity.OrganisationID.Hex(), identity.ProjectID
		}
		if err := emit("devices", doc); err != nil {
			return err
		}
		for h := 0; h < m.HistoryPerDevice; h++ {
			key := recordingKey(identity, m.Timestamp-86400, fmt.Sprintf("history%d", h))
			history := taggedFixture(m, fixtureID(m, fmt.Sprintf("history-%d", device), h))
			history["videoFile"], history["deviceId"], history["deviceKey"] = key, identity.DeviceKey, identity.DeviceKey
			history["organisationId"], history["projectId"] = identity.OrganisationID.Hex(), identity.ProjectID
			history["startTimestamp"], history["endTimestamp"], history["duration"] = m.Timestamp-86400, m.Timestamp-86370, 30000
			history["storageSolution"], history["videoProvider"] = "kstorage", "loadtest"
			history["metadata"] = bson.M{"fileSize": int64(1024 * 1024), "fps": 25.0}
			if err := emit("media", history); err != nil {
				return err
			}
		}
	}
	return nil
}

func deviceDaysID(identity FixtureIdentity) string {
	sum := sha256.Sum256([]byte(identity.OrganisationID.Hex() + "\x00" + identity.ProjectID.Hex() + "\x00" + identity.DeviceKey))
	return hex.EncodeToString(sum[:])
}
