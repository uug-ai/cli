package actions

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

func inspectOrganisationsBackfillGroups(
	ctx context.Context,
	database *mongo.Database,
	adapter organisationsBackfillAdapter,
	config OrganisationsBackfillConfig,
	report organisationsBackfillCollection,
) (organisationsBackfillCollection, error) {
	return inspectOrganisationsBackfillProjectResource(
		ctx,
		database,
		adapter,
		config,
		report,
		"group",
		inspectOrganisationsBackfillGroupIndexes,
	)
}

func resolveOrganisationsBackfillGroup(
	document bson.Raw,
	organisations map[primitive.ObjectID]bool,
	projects map[primitive.ObjectID]primitive.ObjectID,
) organisationsBackfillProjectResourceOutcome {
	return resolveOrganisationsBackfillProjectResource(document, "group", organisations, projects)
}

func inspectOrganisationsBackfillGroupIndexes(ctx context.Context, collection *mongo.Collection) ([]organisationsBackfillIndexContract, error) {
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
	contracts := []organisationsBackfillIndexContract{
		organisationsBackfillNewIndexContract("canonical-project-list", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("legacy-project-list", bson.D{{Key: "user_id", Value: int32(1)}, {Key: "projectId", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("device-membership-lookup", bson.D{{Key: "devices", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("site-membership-lookup", bson.D{{Key: "sites", Value: int32(1)}}),
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
