package actions

import (
	"context"
	"sort"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func inspectOrganisationsBackfillVideowalls(
	ctx context.Context,
	database *mongo.Database,
	adapter organisationsBackfillAdapter,
	config OrganisationsBackfillConfig,
	report organisationsBackfillCollection,
) (organisationsBackfillCollection, error) {
	report, err := inspectOrganisationsBackfillProjectResource(
		ctx,
		database,
		adapter,
		config,
		report,
		"videowall",
		inspectOrganisationsBackfillVideowallIndexes,
	)
	if err != nil || report.Resolution == nil {
		return report, err
	}

	if config.OrganisationID != "" || config.DocumentID != "" {
		return report, nil
	}
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"short_link": bson.M{"$type": "string", "$ne": ""}}}},
		{{Key: "$group", Value: bson.M{"_id": "$short_link", "count": bson.M{"$sum": 1}}}},
		{{Key: "$match", Value: bson.M{"count": bson.M{"$gt": 1}}}},
		{{Key: "$project", Value: bson.M{"_id": 0, "count": 1}}},
		{{Key: "$limit", Value: organisationsBackfillConflictLimit}},
	}
	cursor, err := database.Collection(adapter.Collection).Aggregate(ctx, pipeline)
	if err != nil {
		return report, err
	}
	defer cursor.Close(ctx)
	var duplicates []struct {
		Count int64 `bson:"count"`
	}
	if err := cursor.All(ctx, &duplicates); err != nil {
		return report, err
	}
	for _, duplicate := range duplicates {
		report.Resolution.MultipleCandidates += duplicate.Count
		report.Resolution.Conflicts++
		report.Resolution.ConflictEntries = append(report.Resolution.ConflictEntries, organisationsBackfillConflict{
			Code:    "duplicate-short-link",
			Message: "a redacted short-link capability resolves to multiple videowalls",
		})
	}
	sort.Slice(report.Resolution.ConflictEntries, func(left, right int) bool {
		return report.Resolution.ConflictEntries[left].Code < report.Resolution.ConflictEntries[right].Code
	})
	if len(report.Resolution.ConflictEntries) > organisationsBackfillConflictLimit {
		report.Resolution.ConflictEntries = report.Resolution.ConflictEntries[:organisationsBackfillConflictLimit]
	}
	return report, nil
}

func inspectOrganisationsBackfillVideowallIndexes(ctx context.Context, collection *mongo.Collection) ([]organisationsBackfillIndexContract, error) {
	return inspectOrganisationsBackfillOrderedIndexes(ctx, collection, []organisationsBackfillIndexContract{
		organisationsBackfillNewIndexContract("canonical-project-list", bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "projectId", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("legacy-project-list", bson.D{{Key: "master_user_id", Value: int32(1)}, {Key: "projectId", Value: int32(1)}}),
		organisationsBackfillNewIndexContract("short-link-capability", bson.D{{Key: "short_link", Value: int32(1)}}),
	})
}
