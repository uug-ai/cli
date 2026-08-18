package actions

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const projectsBootstrapLeaseDuration = 5 * time.Minute

type projectsBootstrapCheckpointDocument struct {
	ID                   string                              `bson:"_id"`
	MigrationVersion     int                                 `bson:"migrationVersion"`
	Database             string                              `bson:"database"`
	Stage                string                              `bson:"stage"`
	Scope                string                              `bson:"scope"`
	Mode                 string                              `bson:"mode"`
	Status               string                              `bson:"status"`
	LeaseOwner           string                              `bson:"leaseOwner"`
	LeaseExpiresAt       time.Time                           `bson:"leaseExpiresAt"`
	LastVerifiedMasterID primitive.ObjectID                  `bson:"lastVerifiedMasterId,omitempty"`
	StartedAt            time.Time                           `bson:"startedAt"`
	UpdatedAt            time.Time                           `bson:"updatedAt"`
	CompletedAt          time.Time                           `bson:"completedAt,omitempty"`
	Counters             projectsBootstrapCheckpointCounters `bson:"counters,omitempty"`
	Conflicts            []projectsBootstrapConflict         `bson:"conflicts,omitempty"`
}

type projectsBootstrapCheckpointCounters struct {
	Masters       projectsBootstrapMasterCounts       `bson:"masters"`
	SubUsers      projectsBootstrapSubUserCounts      `bson:"subUsers"`
	Users         projectsBootstrapUserCounts         `bson:"users"`
	Organisations projectsBootstrapOrganisationCounts `bson:"organisations"`
	Projects      projectsBootstrapProjectCounts      `bson:"projects"`
	Writes        projectsBootstrapWriteCounts        `bson:"writes"`
	Verification  projectsBootstrapVerificationCounts `bson:"verification"`
}

func (r *projectsBootstrapRunner) acquireCheckpoint(ctx context.Context) error {
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
			return fmt.Errorf("restart projects bootstrap checkpoint: %w", err)
		}
		if result.DeletedCount == 0 {
			count, countErr := collection.CountDocuments(ctx, bson.M{"_id": checkpointID}, options.Count().SetLimit(1))
			if countErr != nil {
				return fmt.Errorf("inspect projects bootstrap checkpoint: %w", countErr)
			}
			if count == 1 {
				return &projectsBootstrapError{code: projectsBootstrapExitOperational, err: errors.New("projects bootstrap checkpoint has an active lease")}
			}
		}
	}

	if r.config.Resume {
		var checkpoint projectsBootstrapCheckpointDocument
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
				"leaseExpiresAt": now.Add(projectsBootstrapLeaseDuration),
				"updatedAt":      now,
			},
			"$unset": bson.M{"completedAt": ""},
		}, options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&checkpoint)
		if errors.Is(err, mongo.ErrNoDocuments) {
			return &projectsBootstrapError{code: projectsBootstrapExitUsage, err: errors.New("no resumable projects bootstrap checkpoint exists; use a fresh run or -restart")}
		}
		if err != nil {
			return fmt.Errorf("resume projects bootstrap checkpoint: %w", err)
		}
		r.checkpointLastMaster = checkpoint.LastVerifiedMasterID
		r.restoreCheckpoint(checkpoint)
	} else {
		document := projectsBootstrapCheckpointDocument{
			ID:               checkpointID,
			MigrationVersion: r.config.MigrationVersion,
			Database:         r.config.MongoDBDestinationDatabase,
			Stage:            r.config.Stage,
			Scope:            projectsBootstrapScope(r.config),
			Mode:             r.config.Mode,
			Status:           "running",
			LeaseOwner:       leaseOwner,
			LeaseExpiresAt:   now.Add(projectsBootstrapLeaseDuration),
			StartedAt:        now,
			UpdatedAt:        now,
		}
		if _, err := collection.InsertOne(ctx, document); err != nil {
			if mongo.IsDuplicateKeyError(err) {
				return &projectsBootstrapError{code: projectsBootstrapExitUsage, err: errors.New("projects bootstrap checkpoint already exists; use -resume or -restart")}
			}
			return fmt.Errorf("create projects bootstrap checkpoint: %w", err)
		}
	}

	r.checkpointAcquired = true
	r.checkpointLeaseOwner = leaseOwner
	r.checkpointLeaseExpiry = now.Add(projectsBootstrapLeaseDuration)
	r.report.Checkpoint.Status = "running"
	if !r.checkpointLastMaster.IsZero() {
		r.report.Checkpoint.LastVerifiedMasterID = r.checkpointLastMaster.Hex()
	}
	return nil
}

// restoreCheckpoint replays counters but deliberately drops historical
// conflicts: a resumed run must be judged on what it observes now, so an
// operator fix is not permanently blocked by a stale finding.
func (r *projectsBootstrapRunner) restoreCheckpoint(checkpoint projectsBootstrapCheckpointDocument) {
	r.report.Masters = checkpoint.Counters.Masters
	r.report.SubUsers = checkpoint.Counters.SubUsers
	r.report.Users = checkpoint.Counters.Users
	r.report.Organisations = checkpoint.Counters.Organisations
	r.report.Projects = checkpoint.Counters.Projects
	r.report.Writes = checkpoint.Counters.Writes
	r.report.Verification = checkpoint.Counters.Verification
}

func (r *projectsBootstrapRunner) checkpointCounters() projectsBootstrapCheckpointCounters {
	return projectsBootstrapCheckpointCounters{
		Masters:       r.report.Masters,
		SubUsers:      r.report.SubUsers,
		Users:         r.report.Users,
		Organisations: r.report.Organisations,
		Projects:      r.report.Projects,
		Writes:        r.report.Writes,
		Verification:  r.report.Verification,
	}
}

func (r *projectsBootstrapRunner) advanceCheckpoint(ctx context.Context, masterID primitive.ObjectID) error {
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
		"leaseExpiresAt":       now.Add(projectsBootstrapLeaseDuration),
		"updatedAt":            now,
		"counters":             r.checkpointCounters(),
		"conflicts":            r.report.Conflicts,
	}})
	if err != nil {
		return fmt.Errorf("advance projects bootstrap checkpoint: %w", err)
	}
	if result.MatchedCount != 1 {
		return errors.New("projects bootstrap checkpoint lease was lost")
	}
	r.checkpointLastMaster = masterID
	r.checkpointLeaseExpiry = now.Add(projectsBootstrapLeaseDuration)
	r.report.Checkpoint.LastVerifiedMasterID = masterID.Hex()
	return nil
}

func (r *projectsBootstrapRunner) renewCheckpoint(ctx context.Context) error {
	if !r.checkpointAcquired || time.Now().UTC().Before(r.checkpointLeaseExpiry.Add(-projectsBootstrapLeaseDuration/2)) {
		return nil
	}
	now := time.Now().UTC()
	leaseExpiresAt := now.Add(projectsBootstrapLeaseDuration)
	result, err := r.database.Collection("migration_checkpoints").UpdateOne(ctx, bson.M{
		"_id":        r.report.Checkpoint.ID,
		"status":     "running",
		"leaseOwner": r.checkpointLeaseOwner,
	}, bson.M{"$set": bson.M{
		"leaseExpiresAt": leaseExpiresAt,
		"updatedAt":      now,
	}})
	if err != nil {
		return fmt.Errorf("renew projects bootstrap checkpoint: %w", err)
	}
	if result.MatchedCount != 1 {
		return errors.New("projects bootstrap checkpoint lease was lost")
	}
	r.checkpointLeaseExpiry = leaseExpiresAt
	return nil
}

func (r *projectsBootstrapRunner) withCheckpointHeartbeat(ctx context.Context, operation func(context.Context) error) error {
	if !r.checkpointAcquired {
		return operation(ctx)
	}
	operationContext, cancelOperation := context.WithCancelCause(ctx)
	defer cancelOperation(nil)
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(projectsBootstrapLeaseDuration / 3)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				done <- nil
				return
			case <-ticker.C:
				if err := r.refreshCheckpointLease(operationContext); err != nil {
					cancelOperation(err)
					done <- err
					return
				}
			}
		}
	}()

	operationErr := operation(operationContext)
	close(stop)
	if heartbeatErr := <-done; heartbeatErr != nil {
		return errors.Join(operationErr, heartbeatErr)
	}
	return operationErr
}

func (r *projectsBootstrapRunner) refreshCheckpointLease(ctx context.Context) error {
	now := time.Now().UTC()
	result, err := r.database.Collection("migration_checkpoints").UpdateOne(ctx, bson.M{
		"_id":        r.report.Checkpoint.ID,
		"status":     "running",
		"leaseOwner": r.checkpointLeaseOwner,
	}, bson.M{"$set": bson.M{
		"leaseExpiresAt": now.Add(projectsBootstrapLeaseDuration),
		"updatedAt":      now,
	}})
	if err != nil {
		return fmt.Errorf("heartbeat projects bootstrap checkpoint: %w", err)
	}
	if result.MatchedCount != 1 {
		return errors.New("projects bootstrap checkpoint lease was lost")
	}
	return nil
}

func (r *projectsBootstrapRunner) finishCheckpoint(status string) error {
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
			"counters":    r.checkpointCounters(),
			"conflicts":   r.report.Conflicts,
		},
		"$unset": bson.M{
			"leaseOwner":     "",
			"leaseExpiresAt": "",
		},
	})
	if err != nil {
		return fmt.Errorf("finalize projects bootstrap checkpoint: %w", err)
	}
	if result.MatchedCount != 1 {
		return errors.New("projects bootstrap checkpoint lease was lost before finalization")
	}
	r.report.Checkpoint.Status = status
	r.checkpointAcquired = false
	return nil
}
