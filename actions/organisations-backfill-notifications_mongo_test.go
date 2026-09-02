package actions

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestInspectOrganisationsBackfillNotificationsIsReadOnly(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("mailbox and legacy flat event", func(mt *mtest.T) {
		organisationID := primitive.NewObjectID()
		notificationsNamespace := mt.DB.Name() + ".notifications"
		organisationsNamespace := mt.DB.Name() + ".organisation"
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, notificationsNamespace, mtest.FirstBatch,
				bson.D{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "user_id", Value: primitive.NewObjectID().Hex()}, {Key: "data", Value: bson.A{}}},
				bson.D{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "alert_master_user", Value: organisationID.Hex()}, {Key: "userid", Value: primitive.NewObjectID().Hex()}, {Key: "timestamp", Value: int64(1)}},
			),
			mtest.CreateCursorResponse(0, organisationsNamespace, mtest.FirstBatch, bson.D{{Key: "_id", Value: organisationID}}),
			mtest.CreateCursorResponse(0, notificationsNamespace, mtest.FirstBatch,
				bson.D{{Key: "name", Value: "timestamp_1"}, {Key: "key", Value: bson.D{{Key: "timestamp", Value: int32(1)}}}},
			),
		)

		report, err := inspectOrganisationsBackfillNotifications(
			context.Background(),
			mt.DB,
			organisationsBackfillAdapters()["notifications"],
			OrganisationsBackfillConfig{BatchSize: 500},
			organisationsBackfillCollection{LegacyCandidateCount: map[string]int64{"alert_master_user": 1, "userid": 1}},
		)
		if err != nil {
			mt.Fatalf("inspectOrganisationsBackfillNotifications() error = %v", err)
		}
		if report.NotificationShapes == nil || report.NotificationShapes.Mailbox != 1 || report.NotificationShapes.FlatEvents != 1 || !report.NotificationShapes.MailboxExcluded {
			mt.Fatalf("notification shapes = %+v", report.NotificationShapes)
		}
		if report.Total != 1 || report.Resolution == nil || report.Resolution.Resolved != 1 || report.Resolution.ProjectResolved != 1 || report.Resolution.Conflicts != 0 {
			mt.Fatalf("report = %+v", report)
		}
		if len(report.IndexContracts) != 7 || report.IndexContracts[6].Status != "exact" {
			mt.Fatalf("index contracts = %+v", report.IndexContracts)
		}
		for _, event := range mt.GetAllStartedEvents() {
			if event.CommandName != "find" && event.CommandName != "listIndexes" {
				mt.Fatalf("notification dry-run issued non-read command %q", event.CommandName)
			}
		}
	})
}
