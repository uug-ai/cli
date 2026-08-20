package actions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/uug-ai/cli/database"
	"github.com/uug-ai/models/pkg/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	projectsBootstrapExitOperational = 1
	projectsBootstrapExitData        = 2
	projectsBootstrapExitUsage       = 64
)

// projectsBootstrapCollection is the only domain collection this action writes.
// Everything else it touches is users.projectId and migration_checkpoints.
const projectsBootstrapCollection = "project"

// The default project is the reserved, hidden metadata document that Hub API
// mints lazily at _id == organisationId. The slug and name come from models
// rather than from literals here, so a migrated document and a lazily-minted one
// stay interchangeable even when those values change.
//
// The last action is ours: it is the audit marker that distinguishes a document
// this tool wrote from one Hub API minted ("project.default.created") or a user
// created through the API ("project.created").
const (
	projectsBootstrapDefaultSlug = models.DefaultProjectSlug
	projectsBootstrapDefaultName = models.DefaultProjectName
	projectsBootstrapLastAction  = "project.default.migrated"
)

type ProjectsBootstrapConfig struct {
	Mode                       string
	Stage                      string
	MongoDBURI                 string
	MongoDBHost                string
	MongoDBPort                string
	MongoDBSourceDatabase      string
	MongoDBDestinationDatabase string
	MongoDBDatabaseCredentials string
	MongoDBUsername            string
	MongoDBPassword            string
	MigrationVersion           int
	MigrationTimeoutMinutes    int
	Username                   string
	OrganisationID             string
	BatchSize                  int
	Resume                     bool
	Restart                    bool
	StopOnConflict             bool
	ReportFile                 string
}

type projectsBootstrapReport struct {
	Mode             string                              `json:"mode"`
	Stage            string                              `json:"stage"`
	Database         string                              `json:"database"`
	Scope            string                              `json:"scope"`
	MigrationVersion int                                 `json:"migrationVersion"`
	StartedAt        string                              `json:"startedAt"`
	CompletedAt      string                              `json:"completedAt"`
	Masters          projectsBootstrapMasterCounts       `json:"masters"`
	SubUsers         projectsBootstrapSubUserCounts      `json:"subUsers"`
	Users            projectsBootstrapUserCounts         `json:"users"`
	Organisations    projectsBootstrapOrganisationCounts `json:"organisations"`
	Projects         projectsBootstrapProjectCounts      `json:"projects"`
	Writes           projectsBootstrapWriteCounts        `json:"writes"`
	Verification     projectsBootstrapVerificationCounts `json:"verification"`
	Indexes          projectsBootstrapIndexStatus        `json:"indexes"`
	Checkpoint       projectsBootstrapCheckpoint         `json:"checkpoint"`
	Conflicts        []projectsBootstrapConflict         `json:"conflicts"`
	// Warnings keeps the report shape aligned with the organisations bootstrap
	// so operator tooling can parse both. It is always empty here: every
	// projects bootstrap finding is blocking, so nothing produces a warning.
	Warnings []projectsBootstrapWarning `json:"warnings"`
}

type projectsBootstrapMasterCounts struct {
	Scanned    int64 `json:"scanned"`
	Planned    int64 `json:"planned"`
	Complete   int64 `json:"complete"`
	Conflicted int64 `json:"conflicted"`
}

type projectsBootstrapSubUserCounts struct {
	Scanned         int64 `json:"scanned"`
	Planned         int64 `json:"planned"`
	Updated         int64 `json:"updated"`
	AlreadySelected int64 `json:"alreadySelected"`
	Orphaned        int64 `json:"orphaned"`
	Conflicted      int64 `json:"conflicted"`
}

type projectsBootstrapUserCounts struct {
	SelectionsPlanned    int64 `json:"selectionsPlanned"`
	MastersUpdated       int64 `json:"mastersUpdated"`
	SubUsersUpdated      int64 `json:"subUsersUpdated"`
	SelectionsPreserved  int64 `json:"selectionsPreserved"`
	MissingSelectedScope int64 `json:"missingSelectedScope"`
}

type projectsBootstrapOrganisationCounts struct {
	Scanned        int64 `json:"scanned"`
	SecondaryOwned int64 `json:"secondaryOwned"`
}

type projectsBootstrapProjectCounts struct {
	Planned        int64 `json:"planned"`
	Inserted       int64 `json:"inserted"`
	AlreadyPresent int64 `json:"alreadyPresent"`
	Completed      int64 `json:"completed"`
	Reconciled     int64 `json:"reconciled"`
	Conflicted     int64 `json:"conflicted"`
	LegacyDefaults int64 `json:"legacyDefaults"`
}

type projectsBootstrapWriteCounts struct {
	Attempted  int64 `json:"attempted"`
	Applied    int64 `json:"applied"`
	Reconciled int64 `json:"reconciled"`
	Failed     int64 `json:"failed"`
}

type projectsBootstrapVerificationCounts struct {
	Passed int64 `json:"passed"`
	Failed int64 `json:"failed"`
}

type projectsBootstrapIndexStatus struct {
	ProjectOrganisation bool `json:"projectOrganisation"`
	ProjectSlugUnique   bool `json:"projectSlugUnique"`
}

type projectsBootstrapCheckpoint struct {
	ID                   string `json:"id"`
	LastVerifiedMasterID string `json:"lastVerifiedMasterId,omitempty"`
	Status               string `json:"status"`
}

type projectsBootstrapConflict struct {
	Code       string `json:"code"`
	MasterID   string `json:"masterId,omitempty"`
	DocumentID string `json:"documentId,omitempty"`
	Message    string `json:"message"`
}

type projectsBootstrapWarning struct {
	Code       string `json:"code"`
	MasterID   string `json:"masterId,omitempty"`
	DocumentID string `json:"documentId,omitempty"`
	Message    string `json:"message"`
}

type projectsBootstrapRunner struct {
	config                ProjectsBootstrapConfig
	database              *mongo.Database
	now                   time.Time
	report                *projectsBootstrapReport
	stopRequested         <-chan struct{}
	strict                bool
	hasConflict           bool
	conflictCount         int64
	blockingConflictCount int64
	checkpointAcquired    bool
	checkpointLeaseOwner  string
	checkpointLeaseExpiry time.Time
	checkpointLastMaster  primitive.ObjectID
	// legacyDefaultsSeen memoizes the per-organisation legacy default scan so a
	// shared organisation is inspected, counted, and reported once per run
	// rather than once per principal that resolves to it.
	legacyDefaultsSeen map[primitive.ObjectID]int64
}

type projectsBootstrapError struct {
	code int
	err  error
}

var errProjectsBootstrapInterrupted = errors.New("projects bootstrap interrupted")

func (e *projectsBootstrapError) Error() string {
	return e.err.Error()
}

func (e *projectsBootstrapError) Unwrap() error {
	return e.err
}

func ProjectsBootstrapExitCode(err error) int {
	if err == nil {
		return 0
	}
	var bootstrapError *projectsBootstrapError
	if errors.As(err, &bootstrapError) {
		return bootstrapError.code
	}
	return projectsBootstrapExitOperational
}

// ProjectsBootstrap materializes the deterministic default Project document for
// every organisation owned by a master user and initializes users.projectId
// from users.organisationId. It mirrors OrganisationsBootstrap stage for stage
// and depends on that migration already being green for each tenant.
func ProjectsBootstrap(config ProjectsBootstrapConfig) error {
	config = normalizeProjectsBootstrapConfig(config)
	if err := validateProjectsBootstrapConfig(config); err != nil {
		return &projectsBootstrapError{code: projectsBootstrapExitUsage, err: err}
	}

	signalContext, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	ctx := context.Background()
	cancel := func() {}
	if config.MigrationTimeoutMinutes > 0 {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(config.MigrationTimeoutMinutes)*time.Minute)
	}
	defer cancel()

	var connection *database.DB
	if config.MongoDBURI != "" {
		connection = database.NewMongoDBURI(config.MongoDBURI)
	} else {
		connection = database.NewMongoDBHost(
			config.MongoDBHost,
			config.MongoDBPort,
			config.MongoDBDatabaseCredentials,
			config.MongoDBUsername,
			config.MongoDBPassword,
		)
	}
	client := connection.Client
	defer client.Disconnect(context.Background())
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return &projectsBootstrapError{code: projectsBootstrapExitOperational, err: fmt.Errorf("MongoDB projects bootstrap preflight failed: %w", err)}
	}

	hubDatabase := client.Database(config.MongoDBDestinationDatabase)
	for _, collection := range []string{"users", "organisation", projectsBootstrapCollection} {
		if _, err := hubDatabase.Collection(collection).EstimatedDocumentCount(ctx); err != nil {
			return &projectsBootstrapError{code: projectsBootstrapExitOperational, err: fmt.Errorf("cannot read %s: %w", collection, err)}
		}
	}

	startedAt := time.Now().UTC()
	report := projectsBootstrapReport{
		Mode:             config.Mode,
		Stage:            config.Stage,
		Database:         config.MongoDBDestinationDatabase,
		Scope:            projectsBootstrapScope(config),
		MigrationVersion: config.MigrationVersion,
		StartedAt:        startedAt.Format(time.RFC3339),
		Checkpoint: projectsBootstrapCheckpoint{
			ID:     projectsBootstrapCheckpointID(config),
			Status: "not-written",
		},
	}
	runner := projectsBootstrapRunner{
		config:        config,
		database:      hubDatabase,
		now:           startedAt,
		report:        &report,
		stopRequested: signalContext.Done(),
		strict:        config.Stage == "verify",
	}

	if err := runner.inspectIndexes(ctx); err != nil {
		return &projectsBootstrapError{code: projectsBootstrapExitOperational, err: fmt.Errorf("inspect project indexes: %w", err)}
	}
	if config.Mode == "live" && (!report.Indexes.ProjectOrganisation || !report.Indexes.ProjectSlugUnique) {
		runner.addConflict("project-index-missing", "", "", "live projects bootstrap requires the project organisationId and unique partial {organisationId, slug} indexes")
	}
	if config.Mode == "live" && runner.blockingConflictCount == 0 && !runner.interrupted() {
		if err := runner.preflightStage(ctx); err != nil {
			return &projectsBootstrapError{code: projectsBootstrapExitOperational, err: fmt.Errorf("projects bootstrap stage preflight failed: %w", err)}
		}
	}
	if runner.blockingConflictCount == 0 && !runner.interrupted() {
		if err := runner.acquireCheckpoint(ctx); err != nil {
			var bootstrapError *projectsBootstrapError
			if errors.As(err, &bootstrapError) {
				return bootstrapError
			}
			return &projectsBootstrapError{code: projectsBootstrapExitOperational, err: err}
		}
	}

	var runErr error
	if runner.interrupted() {
		runErr = errProjectsBootstrapInterrupted
	} else if runner.blockingConflictCount == 0 {
		runErr = runner.withCheckpointHeartbeat(ctx, func(stageContext context.Context) error {
			if err := runner.runStage(stageContext); err != nil {
				return err
			}
			if config.Mode == "live" {
				return runner.verifyStageFresh()
			}
			return nil
		})
	}
	if runErr != nil {
		if checkpointErr := runner.finishCheckpoint("failed"); checkpointErr != nil {
			runErr = errors.Join(runErr, checkpointErr)
		}
		report.CompletedAt = time.Now().UTC().Format(time.RFC3339)
		if reportErr := writeProjectsBootstrapReport(report, config.ReportFile); reportErr != nil {
			runErr = errors.Join(runErr, reportErr)
		}
		var bootstrapError *projectsBootstrapError
		if errors.As(runErr, &bootstrapError) {
			return bootstrapError
		}
		return &projectsBootstrapError{code: projectsBootstrapExitOperational, err: runErr}
	}
	checkpointStatus := "completed"
	if runner.hasConflict {
		checkpointStatus = "blocked"
	}
	if err := runner.finishCheckpoint(checkpointStatus); err != nil {
		return &projectsBootstrapError{code: projectsBootstrapExitOperational, err: err}
	}

	report.CompletedAt = time.Now().UTC().Format(time.RFC3339)
	if err := writeProjectsBootstrapReport(report, config.ReportFile); err != nil {
		return &projectsBootstrapError{code: projectsBootstrapExitOperational, err: err}
	}
	if runner.hasConflict {
		return &projectsBootstrapError{code: projectsBootstrapExitData, err: errors.New("projects bootstrap found identity conflicts or failed verification")}
	}
	return nil
}

func normalizeProjectsBootstrapConfig(config ProjectsBootstrapConfig) ProjectsBootstrapConfig {
	config.Mode = strings.ToLower(strings.TrimSpace(config.Mode))
	if config.Mode == "" {
		config.Mode = "dry-run"
	}
	config.Stage = strings.ToLower(strings.TrimSpace(config.Stage))
	config.MongoDBURI = strings.TrimSpace(config.MongoDBURI)
	config.MongoDBHost = strings.TrimSpace(config.MongoDBHost)
	config.MongoDBDestinationDatabase = strings.TrimSpace(config.MongoDBDestinationDatabase)
	if config.MongoDBDestinationDatabase == "" {
		config.MongoDBDestinationDatabase = strings.TrimSpace(config.MongoDBSourceDatabase)
	}
	config.Username = strings.TrimSpace(config.Username)
	config.OrganisationID = strings.TrimSpace(config.OrganisationID)
	config.ReportFile = strings.TrimSpace(config.ReportFile)
	if config.MigrationVersion == 0 {
		config.MigrationVersion = 1
	}
	if config.BatchSize == 0 {
		config.BatchSize = 500
	}
	return config
}

func validateProjectsBootstrapConfig(config ProjectsBootstrapConfig) error {
	if config.Mode != "dry-run" && config.Mode != "live" {
		return fmt.Errorf("mode must be dry-run or live, got %q", config.Mode)
	}
	switch config.Stage {
	case "owners", "sub-users":
	case "verify":
		if config.Mode == "live" {
			return errors.New("verify is read-only and requires -mode dry-run")
		}
	default:
		return fmt.Errorf("stage must be owners, sub-users, or verify, got %q", config.Stage)
	}
	if config.MongoDBDestinationDatabase == "" || strings.HasPrefix(config.MongoDBDestinationDatabase, "-") {
		return errors.New("an explicit MongoDB source or destination database is required")
	}
	if config.MongoDBURI == "" && config.MongoDBHost == "" {
		return errors.New("provide -mongodb-uri or -mongodb-host")
	}
	if config.MigrationVersion != 1 {
		return fmt.Errorf("unsupported migration version %d", config.MigrationVersion)
	}
	if config.BatchSize <= 0 {
		return errors.New("bootstrap-batch-size must be greater than zero")
	}
	if config.Username != "" && config.OrganisationID != "" {
		return errors.New("-username and -organisation-id are mutually exclusive")
	}
	if config.OrganisationID != "" {
		if _, err := primitive.ObjectIDFromHex(config.OrganisationID); err != nil {
			return fmt.Errorf("invalid organisation-id: %w", err)
		}
	}
	if config.Resume && config.Restart {
		return errors.New("-resume and -restart are mutually exclusive")
	}
	if (config.Resume || config.Restart) && config.Mode != "live" {
		return errors.New("-resume and -restart require -mode live")
	}
	return nil
}

func projectsBootstrapScope(config ProjectsBootstrapConfig) string {
	if config.OrganisationID != "" {
		return config.OrganisationID
	}
	if config.Username != "" {
		return "username:" + config.Username
	}
	return "all"
}

func projectsBootstrapCheckpointID(config ProjectsBootstrapConfig) string {
	return fmt.Sprintf(
		"projects-bootstrap:v%d:%s:%s:%s",
		config.MigrationVersion,
		config.MongoDBDestinationDatabase,
		config.Stage,
		projectsBootstrapScope(config),
	)
}

// addConflict records a tenant conflict. Every projects bootstrap conflict is
// blocking: unlike the organisations rollout there is no operator-review class
// of finding that a live run may proceed past.
func (r *projectsBootstrapRunner) addConflict(code, masterID, documentID, message string) {
	r.hasConflict = true
	r.conflictCount++
	r.blockingConflictCount++
	if len(r.report.Conflicts) < 100 {
		r.report.Conflicts = append(r.report.Conflicts, projectsBootstrapConflict{
			Code:       code,
			MasterID:   masterID,
			DocumentID: documentID,
			Message:    message,
		})
	}
}

func (r *projectsBootstrapRunner) interrupted() bool {
	if r.stopRequested == nil {
		return false
	}
	select {
	case <-r.stopRequested:
		return true
	default:
		return false
	}
}

func (r *projectsBootstrapRunner) runStage(ctx context.Context) error {
	switch r.config.Stage {
	case "owners":
		return r.runOwners(ctx)
	case "sub-users":
		return r.runSubUsers(ctx)
	case "verify":
		if err := r.runOwners(ctx); err != nil || (r.hasConflict && r.config.StopOnConflict) {
			return err
		}
		return r.runSubUsers(ctx)
	default:
		return nil
	}
}

// preflightStage runs the whole stage read-only before a live run acquires its
// lease. A single blocking conflict anywhere in scope prevents every write.
func (r *projectsBootstrapRunner) preflightStage(ctx context.Context) error {
	preflightConfig := r.config
	preflightConfig.Mode = "dry-run"
	preflightConfig.Resume = false
	preflightConfig.Restart = false
	preflightConfig.StopOnConflict = false
	preflightReport := projectsBootstrapReport{}
	preflight := projectsBootstrapRunner{
		config:        preflightConfig,
		database:      r.database,
		now:           r.now,
		report:        &preflightReport,
		stopRequested: r.stopRequested,
	}
	if err := preflight.runStage(ctx); err != nil {
		return err
	}
	if preflight.blockingConflictCount == 0 {
		return nil
	}
	r.report.Masters = preflightReport.Masters
	r.report.SubUsers = preflightReport.SubUsers
	r.report.Users = preflightReport.Users
	r.report.Organisations = preflightReport.Organisations
	r.report.Projects = preflightReport.Projects
	r.report.Writes = preflightReport.Writes
	r.report.Verification = preflightReport.Verification
	r.report.Conflicts = preflightReport.Conflicts
	r.report.Warnings = append(r.report.Warnings, preflightReport.Warnings...)
	r.hasConflict = true
	r.conflictCount = preflight.conflictCount
	r.blockingConflictCount = preflight.blockingConflictCount
	return nil
}

// verifyStageFresh re-reads the whole scope in strict mode after a live run, so
// "would create" becomes "conflict: missing" and a partially applied migration
// cannot report success.
func (r *projectsBootstrapRunner) verifyStageFresh() error {
	freshContext := context.Background()
	cancel := func() {}
	if r.config.MigrationTimeoutMinutes > 0 {
		freshContext, cancel = context.WithTimeout(freshContext, time.Duration(r.config.MigrationTimeoutMinutes)*time.Minute)
	}
	defer cancel()

	verificationConfig := r.config
	verificationConfig.Mode = "dry-run"
	verificationConfig.Resume = false
	verificationConfig.Restart = false
	verificationConfig.StopOnConflict = false
	verificationReport := projectsBootstrapReport{}
	verification := projectsBootstrapRunner{
		config:        verificationConfig,
		database:      r.database,
		now:           time.Now().UTC(),
		report:        &verificationReport,
		stopRequested: r.stopRequested,
		strict:        true,
	}
	if err := verification.runStage(freshContext); err != nil {
		return fmt.Errorf("fresh projects bootstrap verification failed: %w", err)
	}
	if verification.conflictCount == 0 {
		return nil
	}
	for _, conflict := range verificationReport.Conflicts {
		r.addConflict(conflict.Code, conflict.MasterID, conflict.DocumentID, conflict.Message)
	}
	if verificationReport.Verification.Failed == 0 {
		r.report.Verification.Failed++
	} else {
		r.report.Verification.Failed += verificationReport.Verification.Failed
	}
	return nil
}

func (r *projectsBootstrapRunner) inspectIndexes(ctx context.Context) error {
	projectIndexes, err := readOrganisationsBootstrapIndexes(ctx, r.database.Collection(projectsBootstrapCollection))
	if err != nil {
		return err
	}
	r.report.Indexes.ProjectOrganisation = organisationsBootstrapHasIndex(projectIndexes, bson.D{{Key: "organisationId", Value: int32(1)}}, false)
	r.report.Indexes.ProjectSlugUnique = projectsBootstrapHasDefaultSlugIndex(projectIndexes)
	return nil
}

// projectsBootstrapHasDefaultSlugIndex matches the compound unique partial index
// ensureProjectIndexes creates. The organisation helper only matches a
// single-key {slug: 1} index, which is a different contract.
func projectsBootstrapHasDefaultSlugIndex(indexes []organisationsBootstrapIndex) bool {
	expected := bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "slug", Value: int32(1)}}
	for _, index := range indexes {
		if !organisationsBootstrapHasIndex([]organisationsBootstrapIndex{index}, expected, true) {
			continue
		}
		if projectsBootstrapPartialSlugIsString(index.PartialFilterExpression) {
			return true
		}
	}
	return false
}

// projectsBootstrapPartialSlugIsString accepts both decoded shapes a driver may
// hand back for a nested partialFilterExpression sub-document.
func projectsBootstrapPartialSlugIsString(expression bson.M) bool {
	slug, exists := expression["slug"]
	if !exists {
		return false
	}
	switch typed := slug.(type) {
	case bson.M:
		return typed["$type"] == "string"
	case bson.D:
		for _, element := range typed {
			if element.Key == "$type" && element.Value == "string" {
				return true
			}
		}
	}
	return false
}

func writeProjectsBootstrapReport(report projectsBootstrapReport, reportFile string) error {
	output, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal projects bootstrap report: %w", err)
	}
	fmt.Println(string(output))
	if reportFile == "" {
		return nil
	}
	if err := os.WriteFile(reportFile, append(output, '\n'), 0o600); err != nil {
		return fmt.Errorf("write projects bootstrap report: %w", err)
	}
	return nil
}

func (r *projectsBootstrapRunner) masterFilter(ctx context.Context) (bson.M, error) {
	filter := bson.M{"$or": bson.A{
		bson.M{"user_id": bson.M{"$exists": false}},
		bson.M{"user_id": nil},
		bson.M{"user_id": ""},
	}}
	if r.config.OrganisationID != "" {
		id, _ := primitive.ObjectIDFromHex(r.config.OrganisationID)
		filter["_id"] = id
		return filter, nil
	}
	if r.config.Username == "" {
		return filter, nil
	}
	filter["username"] = r.config.Username
	count, err := r.database.Collection("users").CountDocuments(ctx, filter, options.Count().SetLimit(2))
	if err != nil {
		return nil, err
	}
	if count != 1 {
		return nil, &projectsBootstrapError{code: projectsBootstrapExitData, err: fmt.Errorf("username scope resolves to %d master users", count)}
	}
	return filter, nil
}

func (r *projectsBootstrapRunner) runOwners(ctx context.Context) error {
	filter, err := r.masterFilter(ctx)
	if err != nil {
		return err
	}
	filter = organisationsBootstrapResumeFilter(filter, r.checkpointLastMaster)
	cursor, err := r.database.Collection("users").Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetBatchSize(int32(r.config.BatchSize)))
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		if r.interrupted() {
			return errProjectsBootstrapInterrupted
		}
		if err := r.renewCheckpoint(ctx); err != nil {
			return err
		}
		user, parseErr := parseProjectsBootstrapUser(cursor.Current)
		if parseErr != nil {
			r.addConflict("invalid-master", "", "", parseErr.Error())
			r.report.Masters.Conflicted++
			if r.config.StopOnConflict {
				break
			}
			continue
		}
		r.report.Masters.Scanned++
		verifiedBefore := r.report.Verification.Passed
		if processErr := r.processOwner(ctx, user); processErr != nil {
			return processErr
		}
		if r.blockingConflictCount == 0 && verifiedBefore < r.report.Verification.Passed {
			if checkpointErr := r.advanceCheckpoint(ctx, user.ID); checkpointErr != nil {
				return checkpointErr
			}
		}
		if r.interrupted() {
			return errProjectsBootstrapInterrupted
		}
		if r.hasConflict && r.config.StopOnConflict {
			break
		}
	}
	if err := cursor.Err(); err != nil {
		return err
	}
	if r.report.Masters.Scanned == 0 && r.checkpointLastMaster.IsZero() && (r.config.Username != "" || r.config.OrganisationID != "") {
		r.addConflict("master-scope-not-found", "", "", "the requested scope does not resolve to a master user")
	}
	return nil
}

// processOwner materializes a default Project for every organisation the master
// owns — the canonical primary at _id == master._id plus every secondary
// {ownerId: master._id} — then initializes the master's own users.projectId.
func (r *projectsBootstrapRunner) processOwner(ctx context.Context, user projectsBootstrapUser) error {
	masterID := user.ID.Hex()
	if user.ParentState != organisationsBootstrapFieldEmpty {
		r.addConflict("invalid-master", masterID, masterID, "master user has a non-empty user_id")
		r.report.Masters.Conflicted++
		r.report.Verification.Failed++
		return nil
	}
	ready, err := r.organisationBootstrapReady(ctx, user.ID)
	if err != nil {
		return err
	}
	if !ready {
		r.addConflict("organisation-bootstrap-incomplete", masterID, masterID, "run organisations-bootstrap to green for this master before materializing default projects")
		r.report.Masters.Conflicted++
		r.report.Verification.Failed++
		return nil
	}

	targets, err := r.ownedOrganisations(ctx, user.ID)
	if err != nil {
		return err
	}
	for _, organisationID := range targets {
		if organisationID != user.ID {
			r.report.Organisations.SecondaryOwned++
		}
	}
	if user.SelectionState != organisationsBootstrapFieldValue {
		r.addConflict("organisation-bootstrap-incomplete", masterID, masterID, "master has no canonical organisationId")
		r.report.Masters.Conflicted++
		r.report.Verification.Failed++
		return nil
	}
	// The master may have switched into an organisation it does not own; that
	// selection still needs a default project because projectId mirrors it.
	if _, owned := targetIndex(targets, user.Selection); !owned {
		targets = append(targets, user.Selection)
	}

	projectChanged := false
	for _, organisationID := range targets {
		r.report.Organisations.Scanned++
		ok, changed, ensureErr := r.ensureDefaultProject(ctx, organisationID, user.ID)
		if ensureErr != nil {
			return ensureErr
		}
		if !ok {
			r.report.Masters.Conflicted++
			r.report.Verification.Failed++
			return nil
		}
		projectChanged = projectChanged || changed
	}

	selectionOK, selectionChanged, err := r.ensureUserProjectSelection(ctx, user, user.Selection, user.ID)
	if err != nil {
		return err
	}
	if !selectionOK {
		r.report.Masters.Conflicted++
		r.report.Verification.Failed++
		return nil
	}
	// SelectionsPlanned counts in both modes so the dry-run gate report shows
	// the volume of users.projectId writes a live run would perform;
	// MastersUpdated stays an applied-write counter.
	if selectionChanged {
		r.report.Users.SelectionsPlanned++
		if r.config.Mode == "live" {
			r.report.Users.MastersUpdated++
		}
	}
	if projectChanged || selectionChanged {
		r.report.Masters.Planned++
	} else {
		r.report.Masters.Complete++
	}
	r.report.Verification.Passed++
	return nil
}

func targetIndex(targets []primitive.ObjectID, id primitive.ObjectID) (int, bool) {
	for index := range targets {
		if targets[index] == id {
			return index, true
		}
	}
	return 0, false
}

// ownedOrganisations returns the canonical primary organisation plus every
// secondary organisation owned by the principal, sorted for deterministic
// write order. It reports no counters: the sub-users gate calls it too, and
// counting here would double-count every master.
func (r *projectsBootstrapRunner) ownedOrganisations(ctx context.Context, ownerID primitive.ObjectID) ([]primitive.ObjectID, error) {
	owned := map[primitive.ObjectID]struct{}{ownerID: {}}
	cursor, err := r.database.Collection("organisation").Find(ctx, bson.M{"ownerId": ownerID}, options.Find().SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var organisation struct {
			ID primitive.ObjectID `bson:"_id"`
		}
		if err := cursor.Decode(&organisation); err != nil {
			return nil, err
		}
		owned[organisation.ID] = struct{}{}
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}
	ids := make([]primitive.ObjectID, 0, len(owned))
	for id := range owned {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(left, right int) bool { return ids[left].Hex() < ids[right].Hex() })
	return ids, nil
}
