package loadtest

import (
	"context"
	"time"
)

const (
	EventQueue = "kcloud-event-queue"
	Schema     = "uug.ai/load-test/v1"
)

// Config contains CLI inputs. Connection URLs must never be included in reports.
type Config struct {
	RunID                   string
	Database                string
	MongoURI                string
	RabbitURL               string
	Organisations           int
	ProjectsPerOrganisation int
	DevicesPerProject       int
	HistoryPerDevice        int
	Legacy                  bool
	Rate                    int
	Duration                time.Duration
	Concurrency             int
	DrainTimeout            time.Duration
	OperationTimeout        time.Duration
	MaxP99                  time.Duration
	Execute                 bool
	ConfirmTestTarget       bool
	AllowDefaultNames       bool
	ReportPath              string
	DeploymentLabel         string
}

// Manifest freezes the fixture identity and workload across prepare/run/verify.
type Manifest struct {
	Schema                  string        `json:"schema" bson:"schema"`
	RunID                   string        `json:"runId" bson:"_id"`
	CreatedAt               time.Time     `json:"createdAt" bson:"createdAt"`
	Timestamp               int64         `json:"recordingTimestamp" bson:"recordingTimestamp"`
	Organisations           int           `json:"organisations" bson:"organisations"`
	ProjectsPerOrganisation int           `json:"projectsPerOrganisation" bson:"projectsPerOrganisation"`
	DevicesPerProject       int           `json:"devicesPerProject" bson:"devicesPerProject"`
	HistoryPerDevice        int           `json:"historyPerDevice" bson:"historyPerDevice"`
	Legacy                  bool          `json:"legacy" bson:"legacy"`
	Rate                    int           `json:"ratePerSecond" bson:"ratePerSecond"`
	Duration                time.Duration `json:"durationNs" bson:"durationNs"`
	Concurrency             int           `json:"concurrency" bson:"concurrency"`
	Total                   int           `json:"scheduledTotal" bson:"scheduledTotal"`
	State                   string        `json:"state" bson:"state"`
	DeploymentLabel         string        `json:"deploymentLabel,omitempty" bson:"deploymentLabel,omitempty"`
	Report                  *Report       `json:"report,omitempty" bson:"report,omitempty"`
}

type Observation struct {
	Index      int
	ReceivedAt time.Time
}

type Latency struct {
	Count int     `json:"count" bson:"count"`
	P50MS float64 `json:"p50Ms" bson:"p50Ms"`
	P95MS float64 `json:"p95Ms" bson:"p95Ms"`
	P99MS float64 `json:"p99Ms" bson:"p99Ms"`
	MaxMS float64 `json:"maxMs" bson:"maxMs"`
}

type Verification struct {
	Expected      int `json:"expected" bson:"expected"`
	Found         int `json:"found" bson:"found"`
	Missing       int `json:"missing" bson:"missing"`
	Duplicates    int `json:"duplicates" bson:"duplicates"`
	WrongScope    int `json:"wrongScope" bson:"wrongScope"`
	WrongMetadata int `json:"wrongMetadata" bson:"wrongMetadata"`
	MissingDates  int `json:"missingDates" bson:"missingDates"`
}

type Report struct {
	Schema               string       `json:"schema" bson:"schema"`
	RunID                string       `json:"runId" bson:"runId"`
	Workload             *Manifest    `json:"workload,omitempty" bson:"workload,omitempty"`
	Database             string       `json:"database" bson:"database"`
	DrainTimeoutMS       int64        `json:"drainTimeoutMs" bson:"drainTimeoutMs"`
	OperationTimeoutMS   int64        `json:"operationTimeoutMs" bson:"operationTimeoutMs"`
	MaxP99MS             float64      `json:"maxP99Ms" bson:"maxP99Ms"`
	StartedAt            time.Time    `json:"startedAt" bson:"startedAt"`
	FinishedAt           time.Time    `json:"finishedAt" bson:"finishedAt"`
	Scheduled            int          `json:"scheduled" bson:"scheduled"`
	Attempted            int          `json:"attempted" bson:"attempted"`
	Confirmed            int          `json:"confirmed" bson:"confirmed"`
	PublishErrors        int          `json:"publishErrors" bson:"publishErrors"`
	Missed               int          `json:"missed" bson:"missed"`
	Completed            int          `json:"completed" bson:"completed"`
	DuplicateCompletions int          `json:"duplicateCompletions" bson:"duplicateCompletions"`
	InvalidCompletions   int          `json:"invalidCompletions" bson:"invalidCompletions"`
	PendingCompletions   int          `json:"pendingCompletions" bson:"pendingCompletions"`
	DLQDepth             int          `json:"dlqDepth" bson:"dlqDepth"`
	OfferedRate          float64      `json:"offeredRate" bson:"offeredRate"`
	AchievedPublishRate  float64      `json:"achievedPublishRate" bson:"achievedPublishRate"`
	CompletionRate       float64      `json:"completionRate" bson:"completionRate"`
	Latency              Latency      `json:"scheduledToCompletion" bson:"scheduledToCompletion"`
	SchedulingLag        Latency      `json:"schedulingLag" bson:"schedulingLag"`
	Verification         Verification `json:"verification" bson:"verification"`
	Passed               bool         `json:"passed" bson:"passed"`
	Failures             []string     `json:"failures" bson:"failures"`
	DeploymentLabel      string       `json:"deploymentLabel,omitempty" bson:"deploymentLabel,omitempty"`
}

type Store interface {
	Prepare(context.Context, Manifest) error
	Load(context.Context, string) (Manifest, error)
	Claim(context.Context, string) error
	SaveReport(context.Context, Report) error
	Verify(context.Context, Manifest) (Verification, error)
	Cleanup(context.Context, Manifest) error
	Close(context.Context) error
}

// Each publishing worker owns one publisher/channel: confirms cannot cross workers.
type Publisher interface {
	Publish(context.Context, []byte) error
	Close() error
}

type Transport interface {
	Setup(context.Context, Manifest) error
	Publisher(context.Context) (Publisher, error)
	Observe(context.Context, Manifest, func(Observation)) error
	DLQDepth(context.Context) (int, error)
	PendingCompletions(context.Context, Manifest) (int, error)
	Cleanup(context.Context, Manifest) error
	Close() error
}
