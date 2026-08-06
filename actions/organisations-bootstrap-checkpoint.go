package actions

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const organisationsBootstrapLeaseDuration = 5 * time.Minute

type organisationsBootstrapCheckpointDocument struct {
	ID                   string             `bson:"_id"`
	MigrationVersion     int                `bson:"migrationVersion"`
	Database             string             `bson:"database"`
	Stage                string             `bson:"stage"`
	Scope                string             `bson:"scope"`
	Mode                 string             `bson:"mode"`
	Status               string             `bson:"status"`
	LeaseOwner           string             `bson:"leaseOwner"`
	LeaseExpiresAt       time.Time          `bson:"leaseExpiresAt"`
	LastVerifiedMasterID primitive.ObjectID `bson:"lastVerifiedMasterId,omitempty"`
	StartedAt            time.Time          `bson:"startedAt"`
	UpdatedAt            time.Time          `bson:"updatedAt"`
	CompletedAt          time.Time          `bson:"completedAt,omitempty"`
}

func (r *organisationsBootstrapRunner) acquireCheckpoint(ctx context.Context) error {
	if r.config.Mode != "live" {
		return nil
	}
	collection := r.database.Collection("migration_checkpoints")
	now := time.Now().UTC()
	checkpointID := r.report.Checkpoint.ID
	leaseOwner := primitive.NewObjectID().Hex()

	if r.config.Restart {
		result, err := collection.DeleteOne(ctx, bson.M{
			"_id": checkpointID,
			"$or": bson.A{
				bson.M{"status": bson.M{"$ne": "running"}},
				bson.M{"leaseExpiresAt": bson.M{"$lte": now}},
			},
		})
		if err != nil {
			return fmt.Errorf("restart bootstrap checkpoint: %w", err)
		}
		if result.DeletedCount == 0 {
			count, countErr := collection.CountDocuments(ctx, bson.M{"_id": checkpointID}, options.Count().SetLimit(1))
			if countErr != nil {
				return fmt.Errorf("inspect bootstrap checkpoint: %w", countErr)
			}
			if count == 1 {
				return &organisationsBootstrapError{code: organisationsBootstrapExitOperational, err: errors.New("bootstrap checkpoint has an active lease")}
			}
		}
	}

	if r.config.Resume {
		var checkpoint organisationsBootstrapCheckpointDocument
		err := collection.FindOneAndUpdate(ctx, bson.M{
			"_id":    checkpointID,
			"status": bson.M{"$in": bson.A{"running", "failed", "blocked"}},
			"$or": bson.A{
				bson.M{"status": bson.M{"$ne": "running"}},
				bson.M{"leaseExpiresAt": bson.M{"$lte": now}},
			},
		}, bson.M{
			"$set": bson.M{
				"status":         "running",
				"leaseOwner":     leaseOwner,
				"leaseExpiresAt": now.Add(organisationsBootstrapLeaseDuration),
				"updatedAt":      now,
			},
			"$unset": bson.M{"completedAt": ""},
		}, options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&checkpoint)
		if errors.Is(err, mongo.ErrNoDocuments) {
			return &organisationsBootstrapError{code: organisationsBootstrapExitUsage, err: errors.New("no resumable bootstrap checkpoint exists; use a fresh run or -restart")}
		}
		if err != nil {
			return fmt.Errorf("resume bootstrap checkpoint: %w", err)
		}
		r.checkpointLastMaster = checkpoint.LastVerifiedMasterID
	} else {
		document := organisationsBootstrapCheckpointDocument{
			ID:               checkpointID,
			MigrationVersion: r.config.MigrationVersion,
			Database:         r.config.MongoDBDestinationDatabase,
			Stage:            r.config.Stage,
			Scope:            organisationsBootstrapScope(r.config),
			Mode:             r.config.Mode,
			Status:           "running",
			LeaseOwner:       leaseOwner,
			LeaseExpiresAt:   now.Add(organisationsBootstrapLeaseDuration),
			StartedAt:        now,
			UpdatedAt:        now,
		}
		if _, err := collection.InsertOne(ctx, document); err != nil {
			if mongo.IsDuplicateKeyError(err) {
				return &organisationsBootstrapError{code: organisationsBootstrapExitUsage, err: errors.New("bootstrap checkpoint already exists; use -resume or -restart")}
			}
			return fmt.Errorf("create bootstrap checkpoint: %w", err)
		}
	}

	r.checkpointAcquired = true
	r.checkpointLeaseOwner = leaseOwner
	r.checkpointLeaseExpiry = now.Add(organisationsBootstrapLeaseDuration)
	r.report.Checkpoint.Status = "running"
	if !r.checkpointLastMaster.IsZero() {
		r.report.Checkpoint.LastVerifiedMasterID = r.checkpointLastMaster.Hex()
	}
	return nil
}

func (r *organisationsBootstrapRunner) advanceCheckpoint(ctx context.Context, masterID primitive.ObjectID) error {
	if !r.checkpointAcquired {
		return nil
	}
	now := time.Now().UTC()
	result, err := r.database.Collection("migration_checkpoints").UpdateOne(ctx, bson.M{
		"_id":        r.report.Checkpoint.ID,
		"status":     "running",
		"leaseOwner": r.checkpointLeaseOwner,
	}, bson.M{"$set": bson.M{
		"lastVerifiedMasterId": masterID,
		"leaseExpiresAt":       now.Add(organisationsBootstrapLeaseDuration),
		"updatedAt":            now,
		"counters": bson.M{
			"masters":      r.report.Masters,
			"subUsers":     r.report.SubUsers,
			"memberships":  r.report.Memberships,
			"writes":       r.report.Writes,
			"verification": r.report.Verification,
		},
		"conflicts": r.report.Conflicts,
	}})
	if err != nil {
		return fmt.Errorf("advance bootstrap checkpoint: %w", err)
	}
	if result.MatchedCount != 1 {
		return errors.New("bootstrap checkpoint lease was lost")
	}
	r.checkpointLastMaster = masterID
	r.checkpointLeaseExpiry = now.Add(organisationsBootstrapLeaseDuration)
	r.report.Checkpoint.LastVerifiedMasterID = masterID.Hex()
	return nil
}

func (r *organisationsBootstrapRunner) renewCheckpoint(ctx context.Context) error {
	if !r.checkpointAcquired || time.Now().UTC().Before(r.checkpointLeaseExpiry.Add(-organisationsBootstrapLeaseDuration/2)) {
		return nil
	}
	now := time.Now().UTC()
	leaseExpiresAt := now.Add(organisationsBootstrapLeaseDuration)
	result, err := r.database.Collection("migration_checkpoints").UpdateOne(ctx, bson.M{
		"_id":        r.report.Checkpoint.ID,
		"status":     "running",
		"leaseOwner": r.checkpointLeaseOwner,
	}, bson.M{"$set": bson.M{
		"leaseExpiresAt": leaseExpiresAt,
		"updatedAt":      now,
	}})
	if err != nil {
		return fmt.Errorf("renew bootstrap checkpoint: %w", err)
	}
	if result.MatchedCount != 1 {
		return errors.New("bootstrap checkpoint lease was lost")
	}
	r.checkpointLeaseExpiry = leaseExpiresAt
	return nil
}

func (r *organisationsBootstrapRunner) finishCheckpoint(status string) error {
	if !r.checkpointAcquired {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	now := time.Now().UTC()
	result, err := r.database.Collection("migration_checkpoints").UpdateOne(ctx, bson.M{
		"_id":        r.report.Checkpoint.ID,
		"status":     "running",
		"leaseOwner": r.checkpointLeaseOwner,
	}, bson.M{
		"$set": bson.M{
			"status":      status,
			"updatedAt":   now,
			"completedAt": now,
			"counters": bson.M{
				"masters":      r.report.Masters,
				"subUsers":     r.report.SubUsers,
				"memberships":  r.report.Memberships,
				"writes":       r.report.Writes,
				"verification": r.report.Verification,
			},
			"conflicts": r.report.Conflicts,
		},
		"$unset": bson.M{
			"leaseOwner":     "",
			"leaseExpiresAt": "",
		},
	})
	if err != nil {
		return fmt.Errorf("finalize bootstrap checkpoint: %w", err)
	}
	if result.MatchedCount != 1 {
		return errors.New("bootstrap checkpoint lease was lost before finalization")
	}
	r.report.Checkpoint.Status = status
	r.checkpointAcquired = false
	return nil
}

func organisationsBootstrapResumeFilter(filter bson.M, lastVerifiedMasterID primitive.ObjectID) bson.M {
	if lastVerifiedMasterID.IsZero() {
		return filter
	}
	if exactID, exists := filter["_id"]; exists {
		scopedID, ok := exactID.(primitive.ObjectID)
		if ok && bytes.Compare(scopedID[:], lastVerifiedMasterID[:]) > 0 {
			return filter
		}
		filter["_id"] = bson.M{"$exists": false}
		return filter
	}
	filter["_id"] = bson.M{"$gt": lastVerifiedMasterID}
	return filter
}
