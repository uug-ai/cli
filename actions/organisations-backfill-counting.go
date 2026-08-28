package actions

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func inspectOrganisationsBackfillCountingIndexes(ctx context.Context, collection *mongo.Collection) ([]organisationsBackfillIndexContract, error) {
	return inspectOrganisationsBackfillOrderedIndexes(ctx, collection, []organisationsBackfillIndexContract{
		organisationsBackfillNewIndexContract("canonical-project-time", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "timestamp", Value: int32(-1)}}),
		organisationsBackfillNewIndexContract("canonical-project-device-time", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "device_id", Value: int32(1)}, {Key: "timestamp", Value: int32(-1)}}),
		organisationsBackfillNewIndexContract("legacy-project-time", bson.D{{Key: "user_id", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "timestamp", Value: int32(-1)}}),
		organisationsBackfillNewIndexContract("legacy-project-device-time", bson.D{{Key: "user_id", Value: int32(1)}, {Key: "projectId", Value: int32(1)}, {Key: "device_id", Value: int32(1)}, {Key: "timestamp", Value: int32(-1)}}),
		organisationsBackfillNewIndexContract("global-retention", bson.D{{Key: "timestamp", Value: int32(1)}}),
	})
}
