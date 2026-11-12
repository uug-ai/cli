package loadtest

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/uug-ai/models/pkg/models"
)

type fakeStore struct {
	saved Report
	claim bool
}

func (*fakeStore) Prepare(context.Context, Manifest) error        { return nil }
func (*fakeStore) Load(context.Context, string) (Manifest, error) { return Manifest{}, nil }
func (s *fakeStore) Claim(context.Context, string) error {
	if s.claim {
		return errors.New("already claimed")
	}
	s.claim = true
	return nil
}
func (s *fakeStore) SaveReport(_ context.Context, r Report) error { s.saved = r; return nil }
func (*fakeStore) Verify(_ context.Context, m Manifest) (Verification, error) {
	return Verification{Expected: m.Total, Found: m.Total}, nil
}
func (*fakeStore) Cleanup(context.Context, Manifest) error { return nil }
func (*fakeStore) Close(context.Context) error             { return nil }

type fakeTransport struct {
	manifest     Manifest
	events       chan Observation
	delay        time.Duration
	duplicate    bool
	publishError bool
	dlq          int
	mu           sync.Mutex
	active       int
	peak         int
}

func (f *fakeTransport) Setup(_ context.Context, m Manifest) error {
	f.manifest = m
	f.events = make(chan Observation, m.Total*2)
	return nil
}
func (f *fakeTransport) Publisher(context.Context) (Publisher, error) { return f, nil }
func (f *fakeTransport) Publish(ctx context.Context, body []byte) error {
	f.mu.Lock()
	f.active++
	if f.active > f.peak {
		f.peak = f.active
	}
	f.mu.Unlock()
	defer func() { f.mu.Lock(); f.active--; f.mu.Unlock() }()
	if err := waitUntil(ctx, time.Now().Add(f.delay)); err != nil {
		return err
	}
	if f.publishError {
		return errors.New("simulated broker failure")
	}
	var event models.PipelineEvent
	if err := json.Unmarshal(body, &event); err != nil {
		return err
	}
	index := -1
	for i := 0; i < f.manifest.Total; i++ {
		if RecordingKey(f.manifest, i) == event.Payload.FileName {
			index = i
			break
		}
	}
	o := Observation{Index: index, ReceivedAt: time.Now()}
	f.events <- o
	if f.duplicate {
		f.events <- o
	}
	return nil
}
func (f *fakeTransport) Observe(ctx context.Context, _ Manifest, fn func(Observation)) error {
	for {
		select {
		case o := <-f.events:
			fn(o)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
func (f *fakeTransport) DLQDepth(context.Context) (int, error) { return f.dlq, nil }
func (f *fakeTransport) PendingCompletions(context.Context, Manifest) (int, error) {
	return len(f.events), nil
}
func (*fakeTransport) Cleanup(context.Context, Manifest) error { return nil }
func (*fakeTransport) Close() error                            { return nil }

func testRunConfig() (Config, Manifest) {
	cfg := DefaultConfig()
	cfg.RunID = "runner"
	cfg.Rate = 10
	cfg.Duration = 300 * time.Millisecond
	cfg.Concurrency = 2
	cfg.DrainTimeout = 30 * time.Millisecond
	return cfg, cfg.NewManifest(time.Now())
}

func TestRunnerCompletesAndPersistsReport(t *testing.T) {
	cfg, m := testRunConfig()
	store := &fakeStore{}
	report, err := Run(context.Background(), cfg, m, store, &fakeTransport{})
	if err != nil || !report.Passed || report.Completed != m.Total || report.Confirmed != m.Total {
		t.Fatalf("run err=%v report=%+v", err, report)
	}
	if !store.saved.Passed || report.Latency.Count != m.Total || report.SchedulingLag.Count != m.Total {
		t.Fatalf("missing report or latency accounting: %+v", report)
	}
}

func TestRunnerOverloadIsNotHiddenAsLowerOfferedRate(t *testing.T) {
	cfg, m := testRunConfig()
	m.Rate = 100
	m.Duration = 200 * time.Millisecond
	m.Total = 20
	m.Concurrency = 1
	cfg.OperationTimeout = time.Second
	transport := &fakeTransport{delay: 90 * time.Millisecond}
	report, err := Run(context.Background(), cfg, m, &fakeStore{}, transport)
	if err != nil {
		t.Fatal(err)
	}
	if report.Passed || report.Missed == 0 || report.Scheduled != 20 || report.Attempted+report.Missed != 20 {
		t.Fatalf("overload was hidden: %+v", report)
	}
	if transport.peak > 1 {
		t.Fatalf("concurrency unbounded: %d", transport.peak)
	}
}

func TestRunnerPublishFailureAndDLQFailThresholds(t *testing.T) {
	cfg, m := testRunConfig()
	report, err := Run(context.Background(), cfg, m, &fakeStore{}, &fakeTransport{publishError: true, dlq: 1})
	if err == nil {
		t.Fatal("publish cause was not surfaced")
	}
	if report.Passed || report.PublishErrors == 0 || report.Completed != 0 || report.DLQDepth != 1 {
		t.Fatalf("failure reported success: %+v", report)
	}
}

func TestRunnerCancellationProducesFailedReport(t *testing.T) {
	cfg, m := testRunConfig()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := &fakeStore{}
	report, err := Run(ctx, cfg, m, store, &fakeTransport{})
	if err == nil || report.Passed || !store.claim || store.saved.RunID != m.RunID {
		t.Fatalf("cancellation not accounted for: %v %+v", err, report)
	}
}

func TestLatencyAndThresholdAccounting(t *testing.T) {
	values := []time.Duration{time.Second, time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond}
	got := summarizeLatency(values)
	if got.Count != 4 || got.P50MS != 2 || got.P99MS != 1000 || got.MaxMS != 1000 {
		t.Fatalf("latency = %+v", got)
	}

	r := Report{Scheduled: 1, Completed: 1, Latency: got, Verification: Verification{Expected: 1, Found: 1}}
	evaluateReport(&r, 100*time.Millisecond)
	if r.Passed {
		t.Fatal("p99 threshold ignored")
	}
	r.Latency = Latency{Count: 1, P99MS: 1}
	r.DuplicateCompletions = 1
	evaluateReport(&r, 0)
	if r.Passed {
		t.Fatal("duplicate completion ignored")
	}
}

func TestRunnerDoesNotPassWithPendingDuplicateCompletions(t *testing.T) {
	cfg, m := testRunConfig()
	report, err := Run(context.Background(), cfg, m, &fakeStore{}, &fakeTransport{duplicate: true})
	if err != nil {
		t.Fatal(err)
	}
	if report.Passed || (report.DuplicateCompletions == 0 && report.PendingCompletions == 0) {
		t.Fatalf("duplicate delivery was hidden: %+v", report)
	}
}
