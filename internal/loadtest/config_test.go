package loadtest

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestConfigSafety(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*Config)
	}{
		{"missing run", func(c *Config) { c.RunID = "" }},
		{"unsafe run", func(c *Config) { c.RunID = "../../prod" }},
		{"production database", func(c *Config) { c.Database = "Kerberos" }},
		{"root vhost", func(c *Config) { c.RabbitURL = "amqp://test:test@localhost/" }},
		{"unconfirmed execution", func(c *Config) { c.Execute = true }},
		{"missing live URLs", func(c *Config) { c.Execute, c.ConfirmTestTarget = true, true }},
		{"zero rate", func(c *Config) { c.Rate = 0 }},
		{"oversized run", func(c *Config) { c.Rate, c.Duration = 100000, time.Hour }},
		{"fractional count", func(c *Config) { c.Rate, c.Duration = 1, 1500*time.Millisecond }},
		{"unbounded concurrency", func(c *Config) { c.Concurrency = 257 }},
		{"oversized fixtures", func(c *Config) { c.Organisations, c.DevicesPerProject = 1000, 10000 }},
		{"oversized history", func(c *Config) { c.HistoryPerDevice = 100001 }},
		{"legacy nondefault projects", func(c *Config) { c.Legacy, c.ProjectsPerOrganisation = true, 2 }},
		{"zero drain", func(c *Config) { c.DrainTimeout = 0 }},
		{"Mongo URI database mismatch", func(c *Config) { c.MongoURI = "mongodb://localhost/Kerberos" }},
		{"Rabbit query options", func(c *Config) { c.RabbitURL = "amqp://localhost/loadtest_a?unexpected=value" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.RunID = "safe-test"
			test.edit(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("unsafe/invalid configuration accepted")
			}
		})
	}
}

func TestConfigExplicitLegacyStackException(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RunID, cfg.Database = "smoke", "Kerberos"
	cfg.Execute, cfg.ConfirmTestTarget, cfg.AllowDefaultNames = true, true, true
	cfg.MongoURI, cfg.RabbitURL = "mongodb://localhost:27028/?directConnection=true", "amqp://test:test@localhost:5673/"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg.Database = "production"
	if err := cfg.Validate(); err == nil {
		t.Fatal("default-name exception allowed an arbitrary production database")
	}
}

func TestDryRunDoesNotConnectOrExposeSecrets(t *testing.T) {
	t.Setenv("LOADTEST_MONGODB_URI", "mongodb://test:private-pass@does-not-exist.invalid:27017/")
	t.Setenv("LOADTEST_RABBITMQ_URL", "amqp://test:private-pass@does-not-exist.invalid/loadtest_local")
	for _, action := range []string{"prepare", "run", "verify", "cleanup"} {
		var out, stderr bytes.Buffer
		if code := Command(context.Background(), []string{action, "--run-id", "dry-run"}, &out, &stderr); code != 0 {
			t.Fatalf("%s code=%d: %s", action, code, stderr.String())
		}
		if !strings.Contains(out.String(), `"dryRun": true`) || strings.Contains(out.String(), "private-pass") || strings.Contains(out.String(), "does-not-exist") {
			t.Fatalf("unsafe or unexpected dry-run report: %s", out.String())
		}
	}
}

func TestWorkloadCannotChangeAfterPreparation(t *testing.T) {
	var stderr bytes.Buffer
	_, err := parseFlags("run", []string{"--run-id", "smoke", "--rate", "100"}, &stderr)
	if err == nil || !strings.Contains(err.Error(), "during prepare") {
		t.Fatalf("run workload override was accepted: %v", err)
	}
}

func TestNoGenericProductionEnvironmentFallback(t *testing.T) {
	t.Setenv("LOADTEST_MONGODB_URI", "")
	t.Setenv("LOADTEST_RABBITMQ_URL", "")
	t.Setenv("MONGODB_URI", "mongodb://production/")
	t.Setenv("RABBITMQ_HOST", "production")
	var stderr bytes.Buffer
	cfg, err := parseFlags("prepare", []string{"--run-id", "smoke"}, &stderr)
	if err != nil || cfg.MongoURI != "" || cfg.RabbitURL != "" {
		t.Fatalf("inherited a generic environment configuration: %v", err)
	}
}

func TestReportNeverOverwritesFile(t *testing.T) {
	path := t.TempDir() + "/report.json"
	for i, want := range []int{0, 2} {
		var out, stderr bytes.Buffer
		code := Command(context.Background(), []string{"prepare", "--run-id", "dry", "--report", path}, &out, &stderr)
		if code != want {
			t.Fatalf("attempt %d code=%d want=%d: %s", i, code, want, stderr.String())
		}
	}
}

func TestErrorRedaction(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MongoURI = "mongodb://test:p%40ssword@host/loadtest_local"
	message := safeError(errors.New("failed mongodb://test:p%40ssword@host/loadtest_local p@ssword"), cfg)
	if strings.Contains(message, "p@ssword") || strings.Contains(message, "p%40ssword") {
		t.Fatalf("secret in error: %s", message)
	}
}
