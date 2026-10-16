package loadtest

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var runIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,47}$`)
var databasePattern = regexp.MustCompile(`^loadtest_[a-zA-Z0-9_]+$`)

func DefaultConfig() Config {
	return Config{
		Database: "loadtest_local", Organisations: 1, ProjectsPerOrganisation: 1,
		DevicesPerProject: 10, Rate: 10, Duration: 10 * time.Second, Concurrency: 4,
		DrainTimeout: time.Minute, OperationTimeout: 10 * time.Second,
	}
}

func (c Config) Validate() error {
	if !runIDPattern.MatchString(c.RunID) {
		return errors.New("--run-id must be 1-48 lowercase letters, digits or hyphens, starting with a letter or digit")
	}
	if !databasePattern.MatchString(c.Database) && !(c.AllowDefaultNames && c.Database == "Kerberos") {
		return errors.New("--database must start with loadtest_; the isolated legacy stack requires --allow-default-names for Kerberos")
	}
	if c.Organisations < 1 || c.Organisations > 1000 ||
		c.ProjectsPerOrganisation < 1 || c.ProjectsPerOrganisation > 100 ||
		c.DevicesPerProject < 1 || c.DevicesPerProject > 10000 {
		return errors.New("fixture counts must be positive: organisations <=1000, projects <=100, devices <=10000")
	}
	devices := int64(c.Organisations) * int64(c.ProjectsPerOrganisation) * int64(c.DevicesPerProject)
	if devices > 100000 || c.HistoryPerDevice < 0 || c.HistoryPerDevice > 100000 ||
		devices*int64(c.HistoryPerDevice) > 1000000 {
		return errors.New("this runner supports at most 100,000 fixture devices and 1,000,000 historical media documents")
	}
	if c.Legacy && c.ProjectsPerOrganisation != 1 {
		return errors.New("--legacy requires --projects=1 (the deterministic default project)")
	}
	if c.Rate < 1 || c.Rate > 100000 || c.Duration < time.Millisecond || c.Duration > 24*time.Hour {
		return errors.New("--rate must be 1-100000 and --duration between 1ms and 24h")
	}
	total := float64(c.Rate) * c.Duration.Seconds()
	if total < 1 || total > 1000000 || math.Abs(total-math.Round(total)) > 0.000001 {
		return errors.New("rate times duration must be a whole number of events between 1 and 1,000,000")
	}
	if c.Concurrency < 1 || c.Concurrency > 256 {
		return errors.New("--concurrency must be between 1 and 256")
	}
	if c.DrainTimeout <= 0 || c.DrainTimeout > time.Hour ||
		c.OperationTimeout <= 0 || c.OperationTimeout > time.Minute || c.MaxP99 < 0 {
		return errors.New("drain timeout must be in (0,1h], operation timeout in (0,1m], and max-p99 nonnegative")
	}
	if c.Execute && !c.ConfirmTestTarget {
		return errors.New("live access requires --execute --confirm-test-target against a dedicated isolated test stack")
	}
	if c.MongoURI != "" {
		u, err := url.Parse(c.MongoURI)
		if err != nil || (u.Scheme != "mongodb" && u.Scheme != "mongodb+srv") || u.Host == "" || u.Fragment != "" {
			return errors.New("LOADTEST_MONGODB_URI must be a valid MongoDB connection URL")
		}
		if u.Path != "" && u.Path != "/" && strings.TrimPrefix(u.Path, "/") != c.Database {
			return errors.New("MongoDB URI database must match --database (or omit the URI database)")
		}
	}
	if c.RabbitURL != "" {
		u, err := url.Parse(c.RabbitURL)
		if err != nil || (u.Scheme != "amqp" && u.Scheme != "amqps") || u.Host == "" || u.Fragment != "" || u.RawQuery != "" {
			return errors.New("LOADTEST_RABBITMQ_URL must be an AMQP URL without query parameters or fragments")
		}
		vhost := strings.TrimPrefix(u.Path, "/")
		if !(strings.HasPrefix(vhost, "loadtest_") && len(vhost) > len("loadtest_")) &&
			!(c.AllowDefaultNames && (vhost == "" || vhost == "/")) {
			return errors.New("RabbitMQ vhost must start with loadtest_; the isolated legacy stack requires --allow-default-names for /")
		}
	}
	if c.Execute && (c.MongoURI == "" || c.RabbitURL == "") {
		return errors.New("live access requires LOADTEST_MONGODB_URI and LOADTEST_RABBITMQ_URL")
	}
	return nil
}

func (c Config) NewManifest(now time.Time) Manifest {
	return Manifest{
		Schema: Schema, RunID: c.RunID, CreatedAt: now.UTC(), Timestamp: now.Unix(),
		Organisations: c.Organisations, ProjectsPerOrganisation: c.ProjectsPerOrganisation,
		DevicesPerProject: c.DevicesPerProject, HistoryPerDevice: c.HistoryPerDevice,
		Legacy: c.Legacy, Rate: c.Rate, Duration: c.Duration, Concurrency: c.Concurrency,
		Total: int(math.Round(float64(c.Rate) * c.Duration.Seconds())), State: "prepared",
		DeploymentLabel: c.DeploymentLabel,
	}
}

func validateManifest(m Manifest) error {
	c := DefaultConfig()
	c.RunID, c.Organisations, c.ProjectsPerOrganisation = m.RunID, m.Organisations, m.ProjectsPerOrganisation
	c.DevicesPerProject, c.HistoryPerDevice, c.Legacy = m.DevicesPerProject, m.HistoryPerDevice, m.Legacy
	c.Rate, c.Duration, c.Concurrency = m.Rate, m.Duration, m.Concurrency
	if err := c.Validate(); err != nil {
		return fmt.Errorf("invalid stored manifest: %w", err)
	}
	if m.Schema != Schema || m.Timestamp <= 0 || m.Total != c.NewManifest(time.Now()).Total {
		return errors.New("invalid stored manifest schema, recording timestamp or event total")
	}
	return nil
}
