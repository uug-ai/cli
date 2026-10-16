package actions

import (
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestValidateDeviceNamesBackfillConfig(t *testing.T) {
	valid := normalizeDeviceNamesBackfillConfig(DeviceNamesBackfillConfig{
		MongoDBURI:                 "mongodb://localhost:27017",
		MongoDBDestinationDatabase: "hub",
	})
	if valid.Mode != "dry-run" {
		t.Fatalf("default mode = %q, want dry-run", valid.Mode)
	}
	if valid.BatchSize != deviceNamesBackfillDefaultBatchSize || valid.DeviceCollection != "devices" {
		t.Fatalf("defaults = %+v", valid)
	}

	tests := []struct {
		name      string
		mutate    func(DeviceNamesBackfillConfig) DeviceNamesBackfillConfig
		wantError string
	}{
		{name: "valid dry-run", mutate: func(c DeviceNamesBackfillConfig) DeviceNamesBackfillConfig { return c }},
		{name: "valid live", mutate: func(c DeviceNamesBackfillConfig) DeviceNamesBackfillConfig { c.Mode = "live"; return c }},
		{name: "rejects unknown mode", mutate: func(c DeviceNamesBackfillConfig) DeviceNamesBackfillConfig { c.Mode = "apply"; return c }, wantError: "mode must be"},
		{name: "requires connection", mutate: func(c DeviceNamesBackfillConfig) DeviceNamesBackfillConfig { c.MongoDBURI = ""; return c }, wantError: "mongodb-uri"},
		{name: "requires database", mutate: func(c DeviceNamesBackfillConfig) DeviceNamesBackfillConfig {
			c.MongoDBDestinationDatabase = ""
			return c
		}, wantError: "database is required"},
		{name: "rejects flag as database", mutate: func(c DeviceNamesBackfillConfig) DeviceNamesBackfillConfig {
			c.MongoDBDestinationDatabase = "-mode"
			return c
		}, wantError: "database is required"},
		{name: "rejects negative batch", mutate: func(c DeviceNamesBackfillConfig) DeviceNamesBackfillConfig { c.BatchSize = -1; return c }, wantError: "batch-size"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateDeviceNamesBackfillConfig(test.mutate(valid))
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want containing %q", err, test.wantError)
			}
		})
	}
}

func TestPlanDeviceNamesBackfill(t *testing.T) {
	tests := []struct {
		name string
		doc  bson.M
		want deviceNamesBackfillPlan
	}{
		{
			name: "missing name gets agent.name",
			doc:  bson.M{"agent": bson.M{"name": "Front door"}},
			want: deviceNamesBackfillPlan{SetName: "Front door"},
		},
		{
			name: "null name gets agent.name",
			doc:  bson.M{"name": nil, "agent": bson.M{"name": "Front door"}},
			want: deviceNamesBackfillPlan{SetName: "Front door"},
		},
		{
			name: "empty name gets agent.name",
			doc:  bson.M{"name": "", "agent": bson.M{"name": "Front door"}},
			want: deviceNamesBackfillPlan{SetName: "Front door"},
		},
		{
			name: "whitespace name gets trimmed agent.name",
			doc:  bson.M{"name": " \t ", "agent": bson.M{"name": "  Front door "}},
			want: deviceNamesBackfillPlan{SetName: "Front door"},
		},
		{
			name: "agent.name preferred over analytics",
			doc: bson.M{
				"agent":     bson.M{"name": "Front door"},
				"analytics": bson.A{bson.M{"cameraname": "Legacy"}},
			},
			want: deviceNamesBackfillPlan{SetName: "Front door"},
		},
		{
			name: "falls back to last analytics cameraname",
			doc: bson.M{
				"analytics": bson.A{bson.M{"cameraname": "Older"}, bson.M{"cameraname": "Backyard"}},
			},
			want: deviceNamesBackfillPlan{SetName: "Backyard"},
		},
		{
			name: "blank agent.name falls back to analytics",
			doc: bson.M{
				"name":      "",
				"agent":     bson.M{"name": "  "},
				"analytics": bson.A{bson.M{"cameraname": "Backyard"}},
			},
			want: deviceNamesBackfillPlan{SetName: "Backyard"},
		},
		{
			name: "non-empty name untouched",
			doc: bson.M{
				"name":      "Renamed by user",
				"agent":     bson.M{"name": "Front door"},
				"analytics": bson.A{bson.M{"cameraname": "Legacy"}},
			},
			want: deviceNamesBackfillPlan{},
		},
		{
			name: "non-string name untouched",
			doc:  bson.M{"name": int32(5), "agent": bson.M{"name": "Front door"}},
			want: deviceNamesBackfillPlan{},
		},
		{
			name: "no agent name anywhere is skipped",
			doc:  bson.M{"name": "", "agent": bson.M{}, "analytics": bson.A{bson.M{"cameraname": " "}}},
			want: deviceNamesBackfillPlan{SkippedNoAgentName: true},
		},
		{
			name: "no agent and no analytics is skipped",
			doc:  bson.M{},
			want: deviceNamesBackfillPlan{SkippedNoAgentName: true},
		},
		{
			name: "cameraName copied to missing agent.name and unset",
			doc:  bson.M{"name": "Renamed", "agent": bson.M{"cameraName": "Dev build"}},
			want: deviceNamesBackfillPlan{UnsetCameraName: true, CopyAgentName: "Dev build"},
		},
		{
			name: "cameraName copied to blank agent.name",
			doc:  bson.M{"name": "Renamed", "agent": bson.M{"name": "", "cameraName": "Dev build"}},
			want: deviceNamesBackfillPlan{UnsetCameraName: true, CopyAgentName: "Dev build"},
		},
		{
			name: "cameraName not copied over existing agent.name",
			doc:  bson.M{"name": "Renamed", "agent": bson.M{"name": "Front door", "cameraName": "Dev build"}},
			want: deviceNamesBackfillPlan{UnsetCameraName: true},
		},
		{
			name: "empty cameraName only unset",
			doc:  bson.M{"name": "Renamed", "agent": bson.M{"cameraName": ""}},
			want: deviceNamesBackfillPlan{UnsetCameraName: true},
		},
		{
			name: "missing name uses cameraName copied into agent.name",
			doc: bson.M{
				"agent":     bson.M{"cameraName": "Dev build"},
				"analytics": bson.A{bson.M{"cameraname": "Legacy"}},
			},
			want: deviceNamesBackfillPlan{SetName: "Dev build", UnsetCameraName: true, CopyAgentName: "Dev build"},
		},
		{
			name: "missing name uses agent.name and cameraName is only unset",
			doc:  bson.M{"agent": bson.M{"name": "Front door", "cameraName": "Dev build"}},
			want: deviceNamesBackfillPlan{SetName: "Front door", UnsetCameraName: true},
		},
		{
			name: "already backfilled document is a no-op",
			doc: bson.M{
				"name":      "Front door",
				"agent":     bson.M{"name": "Front door"},
				"analytics": bson.A{bson.M{"cameraname": "Front door"}},
			},
			want: deviceNamesBackfillPlan{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := planDeviceNamesBackfill(test.doc)
			if got != test.want {
				t.Fatalf("plan = %+v, want %+v", got, test.want)
			}
		})
	}
}

func TestTruncateDeviceKey(t *testing.T) {
	if got := truncateDeviceKey("abcdefghij"); got != "abcdef" {
		t.Fatalf("truncateDeviceKey = %q", got)
	}
	if got := truncateDeviceKey("abc"); got != "abc" {
		t.Fatalf("truncateDeviceKey = %q", got)
	}
}
