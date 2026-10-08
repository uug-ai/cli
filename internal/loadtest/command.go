package loadtest

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"
)

const usage = `Usage: cli load-test <prepare|run|verify|cleanup> [flags]

All commands are dry-run plans unless --execute --confirm-test-target is set.
Use only an isolated test stack. No production credentials are inherited.

prepare: seed a new run's fixtures and freeze its workload (rate/duration/counts).
run:     execute that stored workload once through monitor and sequence.
verify:  read back media and calendar correctness for the stored workload.
cleanup: delete a successfully finished run's owned data; retain its completion queue.

Connections: LOADTEST_MONGODB_URI and LOADTEST_RABBITMQ_URL.
The local legacy stack additionally needs --database Kerberos --allow-default-names.
`

func Command(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	if args[0] == "--help" || args[0] == "-h" {
		fmt.Fprint(stdout, usage)
		return 0
	}
	action := args[0]
	switch action {
	case "prepare", "run", "verify", "cleanup":
	default:
		fmt.Fprint(stderr, usage)
		return 2
	}
	cfg, err := parseFlags(action, args[1:], stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintf(stderr, "invalid load-test configuration: %s\n", safeError(err, cfg))
		return 2
	}
	output := stdout
	if cfg.ReportPath != "" {
		file, err := os.OpenFile(cfg.ReportPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			fmt.Fprintln(stderr, "cannot create report file (existing reports are never overwritten)")
			return 2
		}
		defer file.Close()
		output = io.MultiWriter(stdout, file)
	}
	emit := func(value any) bool {
		encoder := json.NewEncoder(output)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(value); err != nil {
			fmt.Fprintf(stderr, "write load-test report: %v\n", err)
			return false
		}
		return true
	}
	if !cfg.Execute {
		plan := struct {
			Schema   string   `json:"schema"`
			Action   string   `json:"action"`
			DryRun   bool     `json:"dryRun"`
			Database string   `json:"database"`
			Workload Manifest `json:"proposedWorkload"`
			Note     string   `json:"note"`
		}{Schema, action, true, cfg.Database, cfg.NewManifest(time.Now()),
			"prepare freezes the workload; run/verify/cleanup load it by run ID. No connections opened."}
		if !emit(plan) {
			return 1
		}
		return 0
	}
	fail := func(err error) int {
		fmt.Fprintf(stderr, "load-test %s: %s\n", action, safeError(err, cfg))
		return 1
	}
	store, err := NewMongoStore(ctx, cfg)
	if err != nil {
		return fail(err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), cfg.OperationTimeout)
		defer cancel()
		if err := store.Close(closeCtx); err != nil {
			fmt.Fprintln(stderr, "warning: closing load-test MongoDB client failed")
		}
	}()
	if action == "prepare" {
		manifest := cfg.NewManifest(time.Now())
		if err := store.Prepare(ctx, manifest); err != nil {
			return fail(err)
		}
		if !emit(manifest) {
			return 1
		}
		return 0
	}
	manifest, err := store.Load(ctx, cfg.RunID)
	if err != nil {
		return fail(err)
	}
	if err := validateManifest(manifest); err != nil {
		return fail(err)
	}
	if action == "verify" {
		if manifest.State == "running" || manifest.State == "preparing" || manifest.State == "cleaning" {
			return fail(errors.New("standalone verification must wait for preparation/execution/cleanup to finish"))
		}
		verifyCtx, cancel := context.WithTimeout(ctx, cfg.DrainTimeout)
		defer cancel()
		result, err := store.Verify(verifyCtx, manifest)
		if err != nil {
			return fail(err)
		}
		passed := result.Expected == manifest.Total && result.Found == result.Expected &&
			result.Missing == 0 && result.Duplicates == 0 && result.WrongScope == 0 &&
			result.WrongMetadata == 0 && result.MissingDates == 0
		if !emit(struct {
			RunID     string       `json:"runId"`
			State     string       `json:"runState"`
			Persisted Verification `json:"verification"`
			Passed    bool         `json:"persistenceChecksPassed"`
		}{manifest.RunID, manifest.State, result, passed}) || !passed {
			return 1
		}
		return 0
	}
	transport, err := NewRabbitTransport(ctx, cfg)
	if err != nil {
		return fail(err)
	}
	defer func() {
		if err := transport.Close(); err != nil {
			fmt.Fprintln(stderr, "warning: closing load-test RabbitMQ connection failed")
		}
	}()
	if action == "cleanup" {
		if manifest.State != "finished" || manifest.Report == nil || !manifest.Report.Passed {
			return fail(errors.New("cleanup requires a successfully finished run; retain failed/incomplete runs for inspection and reset only the dedicated local stack manually"))
		}
		if err := transport.Cleanup(ctx, manifest); err != nil {
			return fail(err)
		}
		if err := store.Cleanup(ctx, manifest); err != nil {
			return fail(err)
		}
		if !emit(struct {
			RunID                   string `json:"runId"`
			Cleaned                 bool   `json:"cleaned"`
			CompletionQueueRetained string `json:"completionQueueRetained"`
		}{manifest.RunID, true, CompletionQueue(manifest)}) {
			return 1
		}
		return 0
	}
	report, err := Run(ctx, cfg, manifest, store, transport)
	if !emit(report) {
		return 1
	}
	if err != nil {
		return fail(err)
	}
	if !report.Passed {
		return 1
	}
	return 0
}

func parseFlags(action string, args []string, stderr io.Writer) (Config, error) {
	cfg := DefaultConfig()
	cfg.MongoURI = os.Getenv("LOADTEST_MONGODB_URI")
	cfg.RabbitURL = os.Getenv("LOADTEST_RABBITMQ_URL")
	fs := flag.NewFlagSet("load-test "+action, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&cfg.RunID, "run-id", "", "Required unique run ID; completed workloads cannot be run twice")
	fs.StringVar(&cfg.Database, "database", cfg.Database, "Dedicated test MongoDB database")
	fs.IntVar(&cfg.Organisations, "organisations", cfg.Organisations, "Synthetic owner organisations (prepare only)")
	fs.IntVar(&cfg.ProjectsPerOrganisation, "projects", cfg.ProjectsPerOrganisation, "Projects per organisation (prepare only)")
	fs.IntVar(&cfg.DevicesPerProject, "devices", cfg.DevicesPerProject, "Devices per project (prepare only)")
	fs.IntVar(&cfg.HistoryPerDevice, "history-per-device", 0, "Historical media per device, seeded outside timing (prepare only)")
	fs.BoolVar(&cfg.Legacy, "legacy", false, "Seed legacy ownership without canonical organisation/project documents (prepare only)")
	fs.IntVar(&cfg.Rate, "rate", cfg.Rate, "Scheduled recordings/second, fixed arrival rate (prepare only)")
	fs.DurationVar(&cfg.Duration, "duration", cfg.Duration, "Traffic generation duration (prepare only)")
	fs.IntVar(&cfg.Concurrency, "concurrency", cfg.Concurrency, "Bounded publisher workers (prepare only)")
	fs.DurationVar(&cfg.DrainTimeout, "drain-timeout", cfg.DrainTimeout, "Maximum completion drain and verification budgets")
	fs.DurationVar(&cfg.OperationTimeout, "operation-timeout", cfg.OperationTimeout, "Individual administrative and publishing operation deadline")
	fs.DurationVar(&cfg.MaxP99, "max-p99", 0, "Optional scheduled-to-completion p99 threshold; 0 disables latency threshold")
	fs.StringVar(&cfg.DeploymentLabel, "deployment-label", "", "Operator-supplied image/version inventory label (prepare only; not independently verified)")
	fs.BoolVar(&cfg.Execute, "execute", false, "Open connections and execute; otherwise print a plan only")
	fs.BoolVar(&cfg.ConfirmTestTarget, "confirm-test-target", false, "Confirm this is a dedicated non-production test stack")
	fs.BoolVar(&cfg.AllowDefaultNames, "allow-default-names", false, "Allow Kerberos and / ONLY on dedicated isolated legacy instances")
	fs.StringVar(&cfg.ReportPath, "report", "", "Also write JSON to a new file (never overwrite)")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() != 0 {
		return cfg, errors.New("unexpected positional arguments")
	}
	if action != "prepare" {
		var invalid string
		fs.Visit(func(f *flag.Flag) {
			switch f.Name {
			case "organisations", "projects", "devices", "history-per-device", "legacy", "rate", "duration", "concurrency", "deployment-label":
				invalid = f.Name
			}
		})
		if invalid != "" {
			return cfg, fmt.Errorf("--%s is set during prepare; later commands use the stored manifest", invalid)
		}
	}
	return cfg, cfg.Validate()
}

func safeError(err error, cfg Config) string {
	text := err.Error()
	for _, raw := range []string{cfg.MongoURI, cfg.RabbitURL} {
		if raw == "" {
			continue
		}
		text = strings.ReplaceAll(text, raw, "<redacted connection URL>")
		if u, parseErr := url.Parse(raw); parseErr == nil && u.User != nil {
			if password, present := u.User.Password(); present && password != "" {
				text = strings.ReplaceAll(text, password, "<redacted>")
				text = strings.ReplaceAll(text, url.QueryEscape(password), "<redacted>")
			}
		}
	}
	return text
}
