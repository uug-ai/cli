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

func inspectOrganisationsBackfillOrderedIndexes(ctx context.Context, collection *mongo.Collection, contracts []organisationsBackfillIndexContract) ([]organisationsBackfillIndexContract, error) {
	cursor, err := collection.Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var indexes []struct {
		Name string `bson:"name"`
		Key  bson.D `bson:"key"`
	}
	if err := cursor.All(ctx, &indexes); err != nil {
		return nil, err
	}
	for index := range contracts {
		contracts[index].Status = "missing"
		for _, candidate := range indexes {
			status := organisationsBackfillOrderedIndexCoverage(candidate.Key, contracts[index].Keys)
			if status == "exact" || (status == "prefix" && contracts[index].Status == "missing") {
				contracts[index].Status = status
				contracts[index].IndexName = candidate.Name
			}
			if status == "exact" {
				break
			}
		}
	}
	return contracts, nil
}
