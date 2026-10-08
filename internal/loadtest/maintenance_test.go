package loadtest

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestMongoLockDurabilityPreservesExplicitJournalAndWriteTimeout(t *testing.T) {
	options := loadTestMongoOptions("mongodb://localhost/loadtest_local?w=1&journal=true&wtimeoutMS=1234")
	if options.WriteConcern.W != "majority" || options.WriteConcern.Journal == nil || !*options.WriteConcern.Journal ||
		options.WriteConcern.WTimeout != 1234*time.Millisecond {
		t.Fatalf("lost explicit durability settings: %#v", options.WriteConcern)
	}
}

func TestMongoMaintenanceCannotContaminateAnActiveRun(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("prepare stops before any fixture writes", func(mt *mtest.T) {
		s := mockStore(mt)
		mt.AddMockResponses(mockGuard(),
			mtest.CreateWriteErrorsResponse(mtest.WriteError{Index: 0, Code: 11000, Message: "another run owns the lock"}))
		if err := s.Prepare(context.Background(), fixtureManifest()); err == nil || !strings.Contains(err.Error(), "active-run lock") {
			mt.Fatalf("prepare did not respect the active-run lock: %v", err)
		}
		if events := mt.GetAllStartedEvents(); len(events) != 2 || rawString(events[1].Command, "insert") != sentinelCollection {
			mt.Fatalf("prepare modified data despite contention: %#v", events)
		}
	})
	mt.Run("cleanup stops before deleting data", func(mt *mtest.T) {
		s := mockStore(mt)
		m := fixtureManifest()
		m.State, m.Report = "finished", &Report{Passed: true}
		mt.AddMockResponses(mockGuard(), mockCursor(manifestCollection, mockDocument(mt, m)),
			mtest.CreateWriteErrorsResponse(mtest.WriteError{Index: 0, Code: 11000, Message: "another run owns the lock"}))
		if err := s.Cleanup(context.Background(), m); err == nil || !strings.Contains(err.Error(), "active-run lock") {
			mt.Fatalf("cleanup did not respect the active-run lock: %v", err)
		}
		if events := mt.GetAllStartedEvents(); len(events) != 3 || rawString(events[2].Command, "insert") != sentinelCollection {
			mt.Fatalf("cleanup modified data despite contention: %#v", events)
		}
	})
}
