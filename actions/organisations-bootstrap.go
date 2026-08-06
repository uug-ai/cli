package actions

import (
	"bytes"
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
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	organisationsBootstrapExitOperational = 1
	organisationsBootstrapExitData        = 2
	organisationsBootstrapExitUsage       = 64
)

type OrganisationsBootstrapConfig struct {
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
	LegacyOrganisationPolicy   string
	Resume                     bool
	Restart                    bool
	StopOnConflict             bool
	ReportFile                 string
}

type organisationsBootstrapReport struct {
	Mode             string                                   `json:"mode"`
	Stage            string                                   `json:"stage"`
	Database         string                                   `json:"database"`
	Scope            string                                   `json:"scope"`
	MigrationVersion int                                      `json:"migrationVersion"`
	StartedAt        string                                   `json:"startedAt"`
	CompletedAt      string                                   `json:"completedAt"`
	Masters          organisationsBootstrapMasterCounts       `json:"masters"`
	SubUsers         organisationsBootstrapSubUserCounts      `json:"subUsers"`
	Users            organisationsBootstrapUserCounts         `json:"users"`
	Memberships      organisationsBootstrapMembershipCounts   `json:"memberships"`
	Organisations    organisationsBootstrapOrganisationCounts `json:"organisations"`
	Domains          organisationsBootstrapDomainCounts       `json:"domains"`
	Writes           organisationsBootstrapWriteCounts        `json:"writes"`
	Verification     organisationsBootstrapVerificationCounts `json:"verification"`
	Indexes          organisationsBootstrapIndexStatus        `json:"indexes"`
	Checkpoint       organisationsBootstrapCheckpoint         `json:"checkpoint"`
	Conflicts        []organisationsBootstrapConflict         `json:"conflicts"`
	Warnings         []organisationsBootstrapWarning          `json:"warnings"`
}

type organisationsBootstrapMasterCounts struct {
	Scanned          int64 `json:"scanned"`
	Planned          int64 `json:"planned"`
	AlreadyCanonical int64 `json:"alreadyCanonical"`
	Inserted         int64 `json:"inserted"`
	Canonicalized    int64 `json:"canonicalized"`
	Conflicted       int64 `json:"conflicted"`
}

type organisationsBootstrapSubUserCounts struct {
	Scanned         int64 `json:"scanned"`
	Planned         int64 `json:"planned"`
	Updated         int64 `json:"updated"`
	AlreadySelected int64 `json:"alreadySelected"`
	Orphaned        int64 `json:"orphaned"`
	Conflicted      int64 `json:"conflicted"`
}

type organisationsBootstrapUserCounts struct {
	MastersUpdated              int64 `json:"mastersUpdated"`
	SelectionsPreserved         int64 `json:"selectionsPreserved"`
	MissingSelectedOrganisation int64 `json:"missingSelectedOrganisation"`
}

type organisationsBootstrapMembershipCounts struct {
	Planned        int64 `json:"planned"`
	Inserted       int64 `json:"inserted"`
	AlreadyPresent int64 `json:"alreadyPresent"`
	Reconciled     int64 `json:"reconciled"`
	Conflicted     int64 `json:"conflicted"`
}

type organisationsBootstrapOrganisationCounts struct {
	SecondaryOwned  int64 `json:"secondaryOwned"`
	LegacyReported  int64 `json:"legacyReported"`
	LegacyAmbiguous int64 `json:"legacyAmbiguous"`
	Archived        int64 `json:"archived"`
	Deleted         int64 `json:"deleted"`
	Referenced      int64 `json:"referenced"`
}

type organisationsBootstrapDomainCounts struct {
	NonEmpty          int64 `json:"nonEmpty"`
	Distinct          int64 `json:"distinct"`
	MultipleMasters   int64 `json:"multipleMasters"`
	SubUserMismatches int64 `json:"subUserMismatches"`
}

type organisationsBootstrapWriteCounts struct {
	Attempted  int64 `json:"attempted"`
	Applied    int64 `json:"applied"`
	Reconciled int64 `json:"reconciled"`
	Failed     int64 `json:"failed"`
}

type organisationsBootstrapVerificationCounts struct {
	Passed int64 `json:"passed"`
	Failed int64 `json:"failed"`
}

type organisationsBootstrapIndexStatus struct {
	OrganisationOwner      bool `json:"organisationOwner"`
	OrganisationSlugUnique bool `json:"organisationSlugUnique"`
	MembershipUnique       bool `json:"membershipUnique"`
	MembershipStatus       bool `json:"membershipStatus"`
}

type organisationsBootstrapCheckpoint struct {
	ID                   string `json:"id"`
	LastVerifiedMasterID string `json:"lastVerifiedMasterId,omitempty"`
	Status               string `json:"status"`
}

type organisationsBootstrapConflict struct {
	Code       string `json:"code"`
	MasterID   string `json:"masterId,omitempty"`
	DocumentID string `json:"documentId,omitempty"`
	Message    string `json:"message"`
}

type organisationsBootstrapWarning struct {
	Code       string `json:"code"`
	MasterID   string `json:"masterId,omitempty"`
	DocumentID string `json:"documentId,omitempty"`
	Message    string `json:"message"`
}

type organisationsBootstrapRunner struct {
	config                OrganisationsBootstrapConfig
	database              *mongo.Database
	now                   time.Time
	report                *organisationsBootstrapReport
	stopRequested         <-chan struct{}
	strict                bool
	hasConflict           bool
	conflictCount         int64
	blockingConflictCount int64
	checkpointAcquired    bool
	checkpointLeaseOwner  string
	checkpointLeaseExpiry time.Time
	checkpointLastMaster  primitive.ObjectID
}

type organisationsBootstrapError struct {
	code int
	err  error
}

var errOrganisationsBootstrapInterrupted = errors.New("bootstrap interrupted")

func (e *organisationsBootstrapError) Error() string {
	return e.err.Error()
}

func (e *organisationsBootstrapError) Unwrap() error {
	return e.err
}

func OrganisationsBootstrapExitCode(err error) int {
	if err == nil {
		return 0
	}
	var bootstrapError *organisationsBootstrapError
	if errors.As(err, &bootstrapError) {
		return bootstrapError.code
	}
	return organisationsBootstrapExitOperational
}

func OrganisationsBootstrap(config OrganisationsBootstrapConfig) error {
	config = normalizeOrganisationsBootstrapConfig(config)
	if err := validateOrganisationsBootstrapConfig(config); err != nil {
		return &organisationsBootstrapError{code: organisationsBootstrapExitUsage, err: err}
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
		return &organisationsBootstrapError{code: organisationsBootstrapExitOperational, err: fmt.Errorf("MongoDB bootstrap preflight failed: %w", err)}
	}

	hubDatabase := client.Database(config.MongoDBDestinationDatabase)
	for _, collection := range []string{"users", "organisation", "organisation_users"} {
		if _, err := hubDatabase.Collection(collection).EstimatedDocumentCount(ctx); err != nil {
			return &organisationsBootstrapError{code: organisationsBootstrapExitOperational, err: fmt.Errorf("cannot read %s: %w", collection, err)}
		}
	}

	startedAt := time.Now().UTC()
	report := organisationsBootstrapReport{
		Mode:             config.Mode,
		Stage:            config.Stage,
		Database:         config.MongoDBDestinationDatabase,
		Scope:            organisationsBootstrapScope(config),
		MigrationVersion: config.MigrationVersion,
		StartedAt:        startedAt.Format(time.RFC3339),
		Checkpoint: organisationsBootstrapCheckpoint{
			ID:     organisationsBootstrapCheckpointID(config),
			Status: "not-written",
		},
	}
	runner := organisationsBootstrapRunner{
		config:        config,
		database:      hubDatabase,
		now:           startedAt,
		report:        &report,
		stopRequested: signalContext.Done(),
		strict:        config.Stage == "verify",
	}

	if err := runner.inspectDomains(ctx); err != nil {
		var bootstrapError *organisationsBootstrapError
		if errors.As(err, &bootstrapError) {
			return bootstrapError
		}
		return &organisationsBootstrapError{code: organisationsBootstrapExitOperational, err: fmt.Errorf("inspect bootstrap domains: %w", err)}
	}
	if err := runner.inspectIndexes(ctx); err != nil {
		return &organisationsBootstrapError{code: organisationsBootstrapExitOperational, err: fmt.Errorf("inspect bootstrap indexes: %w", err)}
	}
	if config.Mode == "live" && (!report.Indexes.OrganisationOwner || !report.Indexes.OrganisationSlugUnique || !report.Indexes.MembershipUnique || !report.Indexes.MembershipStatus) {
		runner.addConflict("bootstrap-index-missing", "", "", "live bootstrap requires the canonical organisation and organisation_users indexes")
	}
	if config.Mode == "live" && runner.blockingConflictCount == 0 && !runner.interrupted() {
		if err := runner.preflightStage(ctx); err != nil {
			return &organisationsBootstrapError{code: organisationsBootstrapExitOperational, err: fmt.Errorf("bootstrap stage preflight failed: %w", err)}
		}
	}
	if runner.blockingConflictCount == 0 && !runner.interrupted() {
		if err := runner.acquireCheckpoint(ctx); err != nil {
			var bootstrapError *organisationsBootstrapError
			if errors.As(err, &bootstrapError) {
				return bootstrapError
			}
			return &organisationsBootstrapError{code: organisationsBootstrapExitOperational, err: err}
		}
	}

	var runErr error
	if runner.interrupted() {
		runErr = errOrganisationsBootstrapInterrupted
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
		if reportErr := writeOrganisationsBootstrapReport(report, config.ReportFile); reportErr != nil {
			runErr = errors.Join(runErr, reportErr)
		}
		var bootstrapError *organisationsBootstrapError
		if errors.As(runErr, &bootstrapError) {
			return bootstrapError
		}
		return &organisationsBootstrapError{code: organisationsBootstrapExitOperational, err: runErr}
	}
	checkpointStatus := "completed"
	if runner.hasConflict {
		checkpointStatus = "blocked"
	}
	if err := runner.finishCheckpoint(checkpointStatus); err != nil {
		return &organisationsBootstrapError{code: organisationsBootstrapExitOperational, err: err}
	}

	report.CompletedAt = time.Now().UTC().Format(time.RFC3339)
	if err := writeOrganisationsBootstrapReport(report, config.ReportFile); err != nil {
		return &organisationsBootstrapError{code: organisationsBootstrapExitOperational, err: err}
	}
	if runner.hasConflict {
		return &organisationsBootstrapError{code: organisationsBootstrapExitData, err: errors.New("bootstrap found identity conflicts or failed verification")}
	}
	return nil
}

func normalizeOrganisationsBootstrapConfig(config OrganisationsBootstrapConfig) OrganisationsBootstrapConfig {
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
	config.LegacyOrganisationPolicy = strings.ToLower(strings.TrimSpace(config.LegacyOrganisationPolicy))
	if config.LegacyOrganisationPolicy == "" {
		config.LegacyOrganisationPolicy = "report"
	}
	if config.MigrationVersion == 0 {
		config.MigrationVersion = 1
	}
	if config.BatchSize == 0 {
		config.BatchSize = 500
	}
	return config
}

func validateOrganisationsBootstrapConfig(config OrganisationsBootstrapConfig) error {
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
	if config.LegacyOrganisationPolicy != "report" {
		if config.LegacyOrganisationPolicy == "archive-delete" {
			return errors.New("legacy-org-policy archive-delete is disabled until guarded archive and reference checks are implemented")
		}
		return fmt.Errorf("legacy-org-policy must be report or archive-delete, got %q", config.LegacyOrganisationPolicy)
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

func organisationsBootstrapScope(config OrganisationsBootstrapConfig) string {
	if config.OrganisationID != "" {
		return config.OrganisationID
	}
	if config.Username != "" {
		return "username:" + config.Username
	}
	return "all"
}

func organisationsBootstrapCheckpointID(config OrganisationsBootstrapConfig) string {
	return fmt.Sprintf(
		"organisations-bootstrap:v%d:%s:%s:%s",
		config.MigrationVersion,
		config.MongoDBDestinationDatabase,
		config.Stage,
		organisationsBootstrapScope(config),
	)
}

func (r *organisationsBootstrapRunner) addConflict(code, masterID, documentID, message string) {
	r.hasConflict = true
	r.conflictCount++
	if organisationsBootstrapConflictBlocks(code) {
		r.blockingConflictCount++
	}
	if len(r.report.Conflicts) < 100 {
		r.report.Conflicts = append(r.report.Conflicts, organisationsBootstrapConflict{
			Code:       code,
			MasterID:   masterID,
			DocumentID: documentID,
			Message:    message,
		})
	}
}

func organisationsBootstrapConflictBlocks(code string) bool {
	return code != "legacy-organisation-unresolved"
}

func (r *organisationsBootstrapRunner) addWarning(code, masterID, documentID, message string) {
	if len(r.report.Warnings) < 100 {
		r.report.Warnings = append(r.report.Warnings, organisationsBootstrapWarning{
			Code:       code,
			MasterID:   masterID,
			DocumentID: documentID,
			Message:    message,
		})
	}
}

func (r *organisationsBootstrapRunner) interrupted() bool {
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

func (r *organisationsBootstrapRunner) runStage(ctx context.Context) error {
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

func (r *organisationsBootstrapRunner) preflightStage(ctx context.Context) error {
	preflightConfig := r.config
	preflightConfig.Mode = "dry-run"
	preflightConfig.Resume = false
	preflightConfig.Restart = false
	preflightConfig.StopOnConflict = false
	preflightReport := organisationsBootstrapReport{}
	preflight := organisationsBootstrapRunner{
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
	r.report.Memberships = preflightReport.Memberships
	r.report.Organisations = preflightReport.Organisations
	r.report.Writes = preflightReport.Writes
	r.report.Verification = preflightReport.Verification
	r.report.Conflicts = preflightReport.Conflicts
	r.report.Warnings = append(r.report.Warnings, preflightReport.Warnings...)
	r.hasConflict = true
	r.conflictCount = preflight.conflictCount
	r.blockingConflictCount = preflight.blockingConflictCount
	return nil
}

func (r *organisationsBootstrapRunner) verifyStageFresh() error {
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
	verificationReport := organisationsBootstrapReport{}
	verification := organisationsBootstrapRunner{
		config:        verificationConfig,
		database:      r.database,
		now:           time.Now().UTC(),
		report:        &verificationReport,
		stopRequested: r.stopRequested,
		strict:        true,
	}
	if err := verification.runStage(freshContext); err != nil {
		return fmt.Errorf("fresh bootstrap verification failed: %w", err)
	}
	if verification.conflictCount == 0 {
		return nil
	}
	for _, conflict := range verificationReport.Conflicts {
		r.addConflict(conflict.Code, conflict.MasterID, conflict.DocumentID, conflict.Message)
	}
	if verification.blockingConflictCount > 0 && verificationReport.Verification.Failed == 0 {
		r.report.Verification.Failed++
	} else if verification.blockingConflictCount > 0 {
		r.report.Verification.Failed += verificationReport.Verification.Failed
	}
	return nil
}

func (r *organisationsBootstrapRunner) inspectDomains(ctx context.Context) error {
	filter, err := r.masterFilter(ctx)
	if err != nil {
		return err
	}
	cursor, err := r.database.Collection("users").Find(ctx, filter, options.Find().SetProjection(bson.M{
		"_id":    1,
		"domain": 1,
	}))
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	masterDomains := map[primitive.ObjectID]string{}
	domainMasters := map[string]int64{}
	for cursor.Next(ctx) {
		var document bson.Raw
		if err := cursor.Decode(&document); err != nil {
			return err
		}
		masterID, state := organisationsBootstrapObjectID(document, "_id")
		if state != organisationsBootstrapFieldValue {
			continue
		}
		domain, domainState := organisationsBootstrapString(document, "domain")
		if domainState == organisationsBootstrapFieldWrong {
			domain = ""
		}
		masterDomains[masterID] = domain
		if domain != "" {
			r.report.Domains.NonEmpty++
			domainMasters[domain]++
		}
	}
	if err := cursor.Err(); err != nil {
		return err
	}
	r.report.Domains.Distinct = int64(len(domainMasters))
	for _, count := range domainMasters {
		if count > 1 {
			r.report.Domains.MultipleMasters++
		}
	}
	if len(masterDomains) == 0 {
		return nil
	}

	subUserFilter := bson.M{"user_id": bson.M{"$exists": true, "$nin": bson.A{nil, ""}}}
	if len(masterDomains) == 1 {
		for masterID := range masterDomains {
			subUserFilter["user_id"] = masterID.Hex()
		}
	}
	subUsers, err := r.database.Collection("users").Find(ctx, subUserFilter, options.Find().SetProjection(bson.M{
		"_id":     1,
		"user_id": 1,
		"domain":  1,
	}))
	if err != nil {
		return err
	}
	defer subUsers.Close(ctx)
	for subUsers.Next(ctx) {
		var document bson.Raw
		if err := subUsers.Decode(&document); err != nil {
			return err
		}
		subUser, parseErr := parseOrganisationsBootstrapUser(document)
		if parseErr != nil || subUser.ParentState != organisationsBootstrapFieldValue {
			continue
		}
		masterDomain, exists := masterDomains[subUser.ParentID]
		if !exists || subUser.Domain == masterDomain {
			continue
		}
		r.report.Domains.SubUserMismatches++
		r.addWarning("sub-user-domain-mismatch", subUser.ParentID.Hex(), subUser.ID.Hex(), "sub-user login domain differs from its master; user_id remains authoritative")
	}
	return subUsers.Err()
}

func (r *organisationsBootstrapRunner) inspectIndexes(ctx context.Context) error {
	organisationIndexes, err := readOrganisationsBootstrapIndexes(ctx, r.database.Collection("organisation"))
	if err != nil {
		return err
	}
	membershipIndexes, err := readOrganisationsBootstrapIndexes(ctx, r.database.Collection("organisation_users"))
	if err != nil {
		return err
	}
	r.report.Indexes.OrganisationOwner = organisationsBootstrapHasIndex(organisationIndexes, bson.D{{Key: "ownerId", Value: int32(1)}}, false)
	r.report.Indexes.OrganisationSlugUnique = organisationsBootstrapHasSlugIndex(organisationIndexes)
	r.report.Indexes.MembershipUnique = organisationsBootstrapHasIndex(membershipIndexes, bson.D{{Key: "userId", Value: int32(1)}, {Key: "organisationId", Value: int32(1)}}, true)
	r.report.Indexes.MembershipStatus = organisationsBootstrapHasIndex(membershipIndexes, bson.D{{Key: "organisationId", Value: int32(1)}, {Key: "status", Value: int32(1)}}, false)
	return nil
}

func readOrganisationsBootstrapIndexes(ctx context.Context, collection *mongo.Collection) ([]bson.M, error) {
	cursor, err := collection.Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var indexes []bson.M
	if err := cursor.All(ctx, &indexes); err != nil {
		return nil, err
	}
	return indexes, nil
}

func organisationsBootstrapHasIndex(indexes []bson.M, expected bson.D, unique bool) bool {
	for _, index := range indexes {
		key, ok := index["key"].(bson.D)
		if !ok || len(key) != len(expected) {
			continue
		}
		matches := true
		for position := range expected {
			if key[position].Key != expected[position].Key || fmt.Sprint(key[position].Value) != fmt.Sprint(expected[position].Value) {
				matches = false
				break
			}
		}
		if !matches {
			continue
		}
		isUnique, _ := index["unique"].(bool)
		if !unique || isUnique {
			return true
		}
	}
	return false
}

func organisationsBootstrapHasSlugIndex(indexes []bson.M) bool {
	for _, index := range indexes {
		if !organisationsBootstrapHasIndex([]bson.M{index}, bson.D{{Key: "slug", Value: int32(1)}}, true) {
			continue
		}
		partial, ok := index["partialFilterExpression"].(bson.M)
		if !ok {
			continue
		}
		slug, ok := partial["slug"].(bson.M)
		if ok && slug["$type"] == "string" {
			return true
		}
	}
	return false
}

func writeOrganisationsBootstrapReport(report organisationsBootstrapReport, reportFile string) error {
	output, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal bootstrap report: %w", err)
	}
	fmt.Println(string(output))
	if reportFile == "" {
		return nil
	}
	if err := os.WriteFile(reportFile, append(output, '\n'), 0o600); err != nil {
		return fmt.Errorf("write bootstrap report: %w", err)
	}
	return nil
}

func (r *organisationsBootstrapRunner) masterFilter(ctx context.Context) (bson.M, error) {
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
		return nil, &organisationsBootstrapError{code: organisationsBootstrapExitData, err: fmt.Errorf("username scope resolves to %d master users", count)}
	}
	return filter, nil
}

func (r *organisationsBootstrapRunner) scopedMasterID(ctx context.Context) (primitive.ObjectID, bool, error) {
	if r.config.OrganisationID != "" {
		id, _ := primitive.ObjectIDFromHex(r.config.OrganisationID)
		return id, true, nil
	}
	if r.config.Username == "" {
		return primitive.NilObjectID, false, nil
	}
	filter, err := r.masterFilter(ctx)
	if err != nil {
		return primitive.NilObjectID, false, err
	}
	var document struct {
		ID primitive.ObjectID `bson:"_id"`
	}
	if err := r.database.Collection("users").FindOne(ctx, filter).Decode(&document); err != nil {
		return primitive.NilObjectID, false, err
	}
	return document.ID, true, nil
}

func (r *organisationsBootstrapRunner) runOwners(ctx context.Context) error {
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
			return errOrganisationsBootstrapInterrupted
		}
		if err := r.renewCheckpoint(ctx); err != nil {
			return err
		}
		user, parseErr := parseOrganisationsBootstrapUser(cursor.Current)
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
			return errOrganisationsBootstrapInterrupted
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

func (r *organisationsBootstrapRunner) processOwner(ctx context.Context, user organisationsBootstrapUser) error {
	masterID := user.ID.Hex()
	if user.ParentState != organisationsBootstrapFieldEmpty {
		r.addConflict("invalid-master", masterID, masterID, "master user has a non-empty user_id")
		r.report.Masters.Conflicted++
		r.report.Verification.Failed++
		return nil
	}
	if user.Username == "" {
		r.addConflict("missing-master-username", masterID, masterID, "master user has no username for the canonical organisation name")
		r.report.Masters.Conflicted++
		r.report.Verification.Failed++
		return nil
	}
	legacyCandidates, legacyInvalid, err := r.legacyOrganisationCandidates(ctx, user.ID, true)
	if err != nil {
		return err
	}
	if legacyInvalid {
		r.report.Masters.Conflicted++
		r.report.Verification.Failed++
		return nil
	}
	if len(legacyCandidates) > 1 {
		r.report.Organisations.LegacyAmbiguous += int64(len(legacyCandidates))
		r.report.Masters.Conflicted++
		r.report.Verification.Failed++
		r.addConflict("legacy-organisations-ambiguous", masterID, "", "multiple random-id legacy organisations reference this master")
		return nil
	}
	if len(legacyCandidates) == 1 {
		candidate := legacyCandidates[0]
		user.OrganisationName = candidate.Name
		user.OrganisationCreatedAt = candidate.CreatedAt
		user.OrganisationUpdatedAt = candidate.UpdatedAt
		r.report.Organisations.LegacyReported++
		r.report.Masters.Conflicted++
		r.addConflict("legacy-organisation-unresolved", masterID, "", "a random-id legacy organisation remains for operator review")
		if r.config.StopOnConflict {
			r.report.Verification.Failed++
			return nil
		}
	}

	canonicalReady, changed, err := r.ensureCanonicalOrganisation(ctx, user)
	if err != nil {
		return err
	}
	if !canonicalReady {
		r.report.Masters.Conflicted++
		r.report.Verification.Failed++
		return nil
	}
	if changed {
		r.report.Masters.Planned++
	} else {
		r.report.Masters.AlreadyCanonical++
	}

	targets, err := r.ownerMembershipTargets(ctx, user)
	if err != nil {
		return err
	}
	if targets == nil {
		r.report.Masters.Conflicted++
		r.report.Verification.Failed++
		return nil
	}
	for _, organisationID := range targets {
		if ok, ensureErr := r.ensureMembership(ctx, user.ID, organisationID, user.ID); ensureErr != nil {
			return ensureErr
		} else if !ok {
			r.report.Masters.Conflicted++
			r.report.Verification.Failed++
			return nil
		}
	}

	selectionOK, updated, err := r.ensureUserSelection(ctx, user, user.ID, user.ID)
	if err != nil {
		return err
	}
	if !selectionOK {
		r.report.Masters.Conflicted++
		r.report.Verification.Failed++
		return nil
	}
	if updated {
		r.report.Users.MastersUpdated++
	}
	r.report.Verification.Passed++
	return nil
}

func (r *organisationsBootstrapRunner) ensureCanonicalOrganisation(ctx context.Context, user organisationsBootstrapUser) (bool, bool, error) {
	collection := r.database.Collection("organisation")
	var stored bson.Raw
	err := collection.FindOne(ctx, bson.M{"_id": user.ID}).Decode(&stored)
	if errors.Is(err, mongo.ErrNoDocuments) {
		if r.strict {
			r.addConflict("canonical-organisation-missing", user.ID.Hex(), user.ID.Hex(), "canonical organisation is missing")
			return false, true, nil
		}
		if r.config.Mode == "dry-run" {
			return true, true, nil
		}
		document := organisationsBootstrapOrganisationDocument(user, r.now)
		r.report.Writes.Attempted++
		result, writeErr := collection.UpdateOne(ctx, bson.M{"_id": user.ID}, bson.M{"$setOnInsert": document}, options.Update().SetUpsert(true))
		if writeErr != nil {
			ready, _, reconcileErr := r.reconcileCanonicalOrganisation(ctx, user, true)
			if reconcileErr != nil {
				r.report.Writes.Failed++
				return false, true, errors.Join(writeErr, reconcileErr)
			}
			if !ready {
				r.report.Writes.Failed++
				return false, true, nil
			}
			r.report.Writes.Applied++
			return true, true, nil
		}
		if result.UpsertedCount == 1 {
			r.report.Writes.Applied++
			r.report.Masters.Inserted++
		}
		return r.reconcileCanonicalOrganisation(ctx, user, true)
	}
	if err != nil {
		return false, false, err
	}
	if !organisationsBootstrapContainerValid(stored, "audit") || (user.Timezone != "" && !organisationsBootstrapContainerValid(stored, "settings")) {
		r.addConflict("canonical-organisation-invalid", user.ID.Hex(), user.ID.Hex(), "organisation audit or settings has an incompatible BSON type")
		return false, false, nil
	}
	if !organisationsBootstrapExistingOrganisationTypesValid(stored, user.Timezone != "") {
		r.addConflict("canonical-organisation-invalid", user.ID.Hex(), user.ID.Hex(), "organisation has an incompatible canonical field type")
		return false, false, nil
	}

	ownerID, ownerState := organisationsBootstrapObjectID(stored, "ownerId")
	legacyOwner, legacyState := organisationsBootstrapString(stored, "owner_id")
	if ownerState == organisationsBootstrapFieldValue && ownerID != user.ID {
		r.addConflict("canonical-owner-conflict", user.ID.Hex(), user.ID.Hex(), "organisation at bootstrap id is owned by another principal")
		return false, false, nil
	}
	if ownerState == organisationsBootstrapFieldWrong || (legacyState == organisationsBootstrapFieldValue && legacyOwner != user.ID.Hex()) || legacyState == organisationsBootstrapFieldWrong {
		r.addConflict("canonical-owner-invalid", user.ID.Hex(), user.ID.Hex(), "organisation at bootstrap id has an invalid canonical or legacy owner")
		return false, false, nil
	}

	set := organisationsBootstrapMissingOrganisationFields(stored, user, r.now)
	if len(set) == 0 {
		return true, false, nil
	}
	if r.strict {
		r.addConflict("canonical-organisation-incomplete", user.ID.Hex(), user.ID.Hex(), "canonical organisation is missing required bootstrap fields")
		return false, true, nil
	}
	if r.config.Mode == "dry-run" {
		return true, true, nil
	}
	for _, field := range sortedOrganisationsBootstrapFields(set) {
		r.report.Writes.Attempted++
		result, writeErr := collection.UpdateOne(ctx, organisationsBootstrapMissingFieldFilter(user.ID, field), bson.M{"$set": bson.M{field: set[field]}})
		if writeErr != nil {
			matched, reconcileErr := r.organisationFieldMatches(ctx, user.ID, field, set[field])
			if reconcileErr != nil {
				r.report.Writes.Failed++
				return false, true, errors.Join(writeErr, reconcileErr)
			}
			if !matched {
				r.report.Writes.Failed++
				r.addConflict("canonical-write-inconclusive", user.ID.Hex(), user.ID.Hex(), "organisation field update did not reconcile")
				return false, true, nil
			}
			r.report.Writes.Applied++
			continue
		}
		if result.ModifiedCount == 1 {
			r.report.Writes.Applied++
		}
	}
	r.report.Masters.Canonicalized++
	return r.reconcileCanonicalOrganisation(ctx, user, true)
}

func (r *organisationsBootstrapRunner) reconcileCanonicalOrganisation(ctx context.Context, user organisationsBootstrapUser, changed bool) (bool, bool, error) {
	var stored bson.Raw
	if err := r.database.Collection("organisation").FindOne(ctx, bson.M{"_id": user.ID}).Decode(&stored); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			r.addConflict("canonical-write-inconclusive", user.ID.Hex(), user.ID.Hex(), "canonical organisation is missing after the write attempt")
			return false, changed, nil
		}
		return false, changed, err
	}
	ownerID, state := organisationsBootstrapObjectID(stored, "ownerId")
	if state != organisationsBootstrapFieldValue || ownerID != user.ID || len(organisationsBootstrapMissingOrganisationFields(stored, user, r.now)) != 0 || !organisationsBootstrapOrganisationFieldsValid(stored, user.Timezone != "") {
		r.addConflict("canonical-reconcile-failed", user.ID.Hex(), user.ID.Hex(), "canonical organisation did not reconcile to the required identity fields")
		return false, changed, nil
	}
	if changed {
		r.report.Writes.Reconciled++
	}
	return true, changed, nil
}

func (r *organisationsBootstrapRunner) organisationFieldMatches(ctx context.Context, organisationID primitive.ObjectID, field string, expected any) (bool, error) {
	var stored bson.Raw
	if err := r.database.Collection("organisation").FindOne(ctx, bson.M{"_id": organisationID}).Decode(&stored); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return false, nil
		}
		return false, err
	}
	expectedDocument, err := bson.Marshal(bson.M{"value": expected})
	if err != nil {
		return false, err
	}
	actualValue := stored.Lookup(splitOrganisationsBootstrapPath(field)...)
	expectedValue := bson.Raw(expectedDocument).Lookup("value")
	return actualValue.Type == expectedValue.Type && bytes.Equal(actualValue.Value, expectedValue.Value), nil
}

func (r *organisationsBootstrapRunner) ownerMembershipTargets(ctx context.Context, user organisationsBootstrapUser) ([]primitive.ObjectID, error) {
	targets := map[primitive.ObjectID]struct{}{user.ID: {}}
	cursor, err := r.database.Collection("organisation").Find(ctx, bson.M{"ownerId": user.ID}, options.Find().SetProjection(bson.M{"_id": 1}))
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
		if organisation.ID != user.ID {
			r.report.Organisations.SecondaryOwned++
		}
		targets[organisation.ID] = struct{}{}
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}

	if user.SelectionState == organisationsBootstrapFieldWrong {
		r.addConflict("invalid-user-selection", user.ID.Hex(), user.ID.Hex(), "organisation_id must be a BSON ObjectID")
		return nil, nil
	}
	if user.SelectionState == organisationsBootstrapFieldValue {
		accessible, accessErr := r.organisationAccessibleToUser(ctx, user.Selection, user.ID, user.ID)
		if accessErr != nil {
			return nil, accessErr
		}
		if !accessible {
			r.report.Users.MissingSelectedOrganisation++
			r.addConflict("invalid-user-selection", user.ID.Hex(), user.ID.Hex(), "preserved organisation_id does not reference an organisation owned by the master")
			return nil, nil
		}
		targets[user.Selection] = struct{}{}
		r.report.Users.SelectionsPreserved++
	}

	ids := make([]primitive.ObjectID, 0, len(targets))
	for id := range targets {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(left, right int) bool { return ids[left].Hex() < ids[right].Hex() })
	return ids, nil
}
