package actions

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

type deviceNamesBackfillUpdate struct {
	Q bson.M `bson:"q"`
	U bson.M `bson:"u"`
}

func deviceNamesBackfillUpdates(mt *mtest.T) [][]deviceNamesBackfillUpdate {
	var commands [][]deviceNamesBackfillUpdate
	for _, event := range mt.GetAllStartedEvents() {
		switch event.CommandName {
		case "find":
		case "update":
			var command struct {
				Updates []deviceNamesBackfillUpdate `bson:"updates"`
			}
			if err := bson.Unmarshal(event.Command, &command); err != nil {
				mt.Fatalf("decode update command: %v", err)
			}
			commands = append(commands, command.Updates)
		default:
			mt.Fatalf("unexpected command %q", event.CommandName)
		}
	}
	return commands
}

func deviceNamesBackfillSuccess(n int32) bson.D {
	return mtest.CreateSuccessResponse(bson.E{Key: "n", Value: n}, bson.E{Key: "nModified", Value: n})
}

// deviceNamesBackfillFixture covers every outcome: name from agent.name, name
// from analytics, cameraName copied + unset, cameraName unset only, and skipped.
func deviceNamesBackfillFixture() (ids []primitive.ObjectID, docs []bson.D) {
	for i := 0; i < 5; i++ {
		ids = append(ids, primitive.NewObjectID())
	}
	docs = []bson.D{
		{{Key: "_id", Value: ids[0]}, {Key: "key", Value: "device-key-0"}, {Key: "agent", Value: bson.D{{Key: "name", Value: "Front door"}}}},
		{{Key: "_id", Value: ids[1]}, {Key: "key", Value: "device-key-1"}, {Key: "name", Value: " "}, {Key: "analytics", Value: bson.A{bson.D{{Key: "cameraname", Value: "Backyard"}}}}},
		{{Key: "_id", Value: ids[2]}, {Key: "key", Value: "device-key-2"}, {Key: "name", Value: "Renamed"}, {Key: "agent", Value: bson.D{{Key: "cameraName", Value: "Dev build"}}}},
		{{Key: "_id", Value: ids[3]}, {Key: "key", Value: "device-key-3"}, {Key: "name", Value: "Renamed"}, {Key: "agent", Value: bson.D{{Key: "name", Value: "Garage"}, {Key: "cameraName", Value: "Dev build"}}}},
		{{Key: "_id", Value: ids[4]}, {Key: "key", Value: "device-key-4"}, {Key: "name", Value: nil}},
	}
	return ids, docs
}

func TestRunDeviceNamesBackfillDryRunIssuesNoWrites(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("dry run", func(mt *mtest.T) {
		_, docs := deviceNamesBackfillFixture()
		mt.AddMockResponses(mtest.CreateCursorResponse(0, mt.DB.Name()+"."+mt.Coll.Name(), mtest.FirstBatch, docs...))

		report, err := runDeviceNamesBackfill(context.Background(), mt.Coll, DeviceNamesBackfillConfig{Mode: "dry-run", BatchSize: 2})
		if err != nil {
			mt.Fatalf("runDeviceNamesBackfill() error = %v", err)
		}
		if report.Scanned != 5 || report.NamesToSet != 2 || report.CameraNameToRemove != 2 || report.AgentNameToCopy != 1 || report.SkippedNoAgentName != 1 {
			mt.Fatalf("report = %+v", report)
		}
		if report.Written != nil {
			mt.Fatalf("dry run reported writes: %+v", report.Written)
		}
		if updates := deviceNamesBackfillUpdates(mt); len(updates) != 0 {
			mt.Fatalf("dry run issued %d update commands", len(updates))
		}
	})
}

func TestRunDeviceNamesBackfillLiveWritesConditionalUpdates(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("live", func(mt *mtest.T) {
		ids, docs := deviceNamesBackfillFixture()
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, mt.DB.Name()+"."+mt.Coll.Name(), mtest.FirstBatch, docs...),
			deviceNamesBackfillSuccess(2), // names
			deviceNamesBackfillSuccess(1), // agent.name copy + unset
			deviceNamesBackfillSuccess(1), // unset only
		)

		report, err := runDeviceNamesBackfill(context.Background(), mt.Coll, DeviceNamesBackfillConfig{Mode: "live", BatchSize: 500})
		if err != nil {
			mt.Fatalf("runDeviceNamesBackfill() error = %v", err)
		}
		if report.Scanned != 5 || report.NamesToSet != 2 || report.CameraNameToRemove != 2 || report.AgentNameToCopy != 1 || report.SkippedNoAgentName != 1 {
			mt.Fatalf("report = %+v", report)
		}
		want := deviceNamesBackfillWrites{NamesSet: 2, CameraNameRemoved: 2, AgentNameCopied: 1}
		if report.Written == nil || *report.Written != want {
			mt.Fatalf("written = %+v, want %+v", report.Written, want)
		}

		commands := deviceNamesBackfillUpdates(mt)
		if len(commands) != 3 || len(commands[0]) != 2 || len(commands[1]) != 1 || len(commands[2]) != 1 {
			mt.Fatalf("update commands = %+v", commands)
		}

		names := commands[0]
		for i, wantName := range []string{"Front door", "Backyard"} {
			update := names[i]
			if update.Q["_id"] != ids[i] {
				mt.Fatalf("name update %d targets %v", i, update.Q["_id"])
			}
			assertDeviceNamesBlankGuard(mt, update.Q, "name")
			set, _ := update.U["$set"].(bson.M)
			if len(update.U) != 1 || len(set) != 1 || set["name"] != wantName {
				mt.Fatalf("name update %d = %+v", i, update.U)
			}
		}

		copyUpdate := commands[1][0]
		if copyUpdate.Q["_id"] != ids[2] || copyUpdate.Q["agent.cameraName"] != "Dev build" {
			mt.Fatalf("copy filter = %+v", copyUpdate.Q)
		}
		assertDeviceNamesBlankGuard(mt, copyUpdate.Q, "agent.name")
		copySet, _ := copyUpdate.U["$set"].(bson.M)
		copyUnset, _ := copyUpdate.U["$unset"].(bson.M)
		if len(copySet) != 1 || copySet["agent.name"] != "Dev build" || len(copyUnset) != 1 || copyUnset["agent.cameraName"] == nil {
			mt.Fatalf("copy update = %+v", copyUpdate.U)
		}

		unsetUpdate := commands[2][0]
		exists, _ := unsetUpdate.Q["agent.cameraName"].(bson.M)
		if unsetUpdate.Q["_id"] != ids[3] || exists["$exists"] != true {
			mt.Fatalf("unset filter = %+v", unsetUpdate.Q)
		}
		unset, _ := unsetUpdate.U["$unset"].(bson.M)
		if len(unsetUpdate.U) != 1 || len(unset) != 1 || unset["agent.cameraName"] == nil {
			mt.Fatalf("unset update = %+v", unsetUpdate.U)
		}
	})
}

func assertDeviceNamesBlankGuard(mt *mtest.T, filter bson.M, field string) {
	mt.Helper()
	or, ok := filter["$or"].(bson.A)
	if !ok || len(or) != 2 {
		mt.Fatalf("filter %+v has no blank guard for %s", filter, field)
	}
	isNull, _ := or[0].(bson.M)
	if value, present := isNull[field]; !present || value != nil {
		mt.Fatalf("filter %+v does not match missing/null %s", filter, field)
	}
	blank, _ := or[1].(bson.M)
	regex, _ := blank[field].(bson.M)
	if regex["$regex"] != deviceNamesBackfillBlankPattern {
		mt.Fatalf("filter %+v does not match blank %s", filter, field)
	}
}

func TestRunDeviceNamesBackfillWritesInBatches(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("batches", func(mt *mtest.T) {
		ids, docs := deviceNamesBackfillFixture()
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, mt.DB.Name()+"."+mt.Coll.Name(), mtest.FirstBatch, docs[0], docs[1], docs[4]),
			deviceNamesBackfillSuccess(1),
			deviceNamesBackfillSuccess(1),
		)

		report, err := runDeviceNamesBackfill(context.Background(), mt.Coll, DeviceNamesBackfillConfig{Mode: "live", BatchSize: 1})
		if err != nil {
			mt.Fatalf("runDeviceNamesBackfill() error = %v", err)
		}
		if report.Written == nil || report.Written.NamesSet != 2 || report.SkippedNoAgentName != 1 {
			mt.Fatalf("report = %+v written = %+v", report, report.Written)
		}
		commands := deviceNamesBackfillUpdates(mt)
		if len(commands) != 2 || len(commands[0]) != 1 || len(commands[1]) != 1 ||
			commands[0][0].Q["_id"] != ids[0] || commands[1][0].Q["_id"] != ids[1] {
			mt.Fatalf("update commands = %+v", commands)
		}
	})
}

func TestRunDeviceNamesBackfillRerunIsNoop(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("re-run", func(mt *mtest.T) {
		// After a live run only devices without any agent name still match the
		// selector; backfilled devices no longer do. Even if a backfilled
		// document is returned, it plans no write.
		_, docs := deviceNamesBackfillFixture()
		backfilled := bson.D{
			{Key: "_id", Value: primitive.NewObjectID()},
			{Key: "name", Value: "Front door"},
			{Key: "agent", Value: bson.D{{Key: "name", Value: "Front door"}}},
		}
		mt.AddMockResponses(mtest.CreateCursorResponse(0, mt.DB.Name()+"."+mt.Coll.Name(), mtest.FirstBatch, docs[4], backfilled))

		report, err := runDeviceNamesBackfill(context.Background(), mt.Coll, DeviceNamesBackfillConfig{Mode: "live", BatchSize: 500})
		if err != nil {
			mt.Fatalf("runDeviceNamesBackfill() error = %v", err)
		}
		if report.Scanned != 2 || report.NamesToSet != 0 || report.CameraNameToRemove != 0 || report.SkippedNoAgentName != 1 {
			mt.Fatalf("report = %+v", report)
		}
		if report.Written == nil || *report.Written != (deviceNamesBackfillWrites{}) {
			mt.Fatalf("written = %+v", report.Written)
		}
		if updates := deviceNamesBackfillUpdates(mt); len(updates) != 0 {
			mt.Fatalf("re-run issued %d update commands", len(updates))
		}
	})
}

func TestRunDeviceNamesBackfillConcurrentRenameIsNotOverwritten(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("concurrent rename", func(mt *mtest.T) {
		// The device is renamed between scan and write: the guarded filter no
		// longer matches, so the server reports n=0 and nothing is modified.
		_, docs := deviceNamesBackfillFixture()
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, mt.DB.Name()+"."+mt.Coll.Name(), mtest.FirstBatch, docs[0]),
			deviceNamesBackfillSuccess(0),
		)

		report, err := runDeviceNamesBackfill(context.Background(), mt.Coll, DeviceNamesBackfillConfig{Mode: "live", BatchSize: 500})
		if err != nil {
			mt.Fatalf("runDeviceNamesBackfill() error = %v", err)
		}
		if report.NamesToSet != 1 || report.Written == nil || report.Written.NamesSet != 0 || report.Written.SkippedConcurrentChange != 1 {
			mt.Fatalf("report = %+v written = %+v", report, report.Written)
		}
	})
}
