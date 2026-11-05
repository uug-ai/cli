package loadtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type runState struct {
	mu                sync.Mutex
	report            Report
	sent              []bool
	completed         []bool
	latencies         []time.Duration
	lags              []time.Duration
	start             time.Time
	lastObservation   time.Time
	firstPublishError error
}

// Run uses open-loop scheduling: generator saturation records missed arrivals,
// rather than silently reducing the offered rate or building an unbounded queue.
func Run(ctx context.Context, cfg Config, m Manifest, store Store, transport Transport) (Report, error) {
	if err := validateManifest(m); err != nil {
		return Report{}, err
	}
	if m.State != "prepared" {
		return Report{}, errors.New("run is not prepared; use a new run ID rather than replaying an ingestion benchmark")
	}
	if err := transport.Setup(ctx, m); err != nil {
		return Report{}, fmt.Errorf("prepare pipeline transport: %w", err)
	}
	if err := store.Claim(ctx, m.RunID); err != nil {
		return Report{}, fmt.Errorf("claim run: %w", err)
	}

	start := time.Now()
	workload := m
	workload.Report = nil
	s := runState{
		report: Report{Schema: Schema, RunID: m.RunID, StartedAt: start.UTC(),
			Scheduled: m.Total, OfferedRate: float64(m.Rate), DeploymentLabel: m.DeploymentLabel,
			Workload: &workload, Database: cfg.Database, DrainTimeoutMS: cfg.DrainTimeout.Milliseconds(),
			OperationTimeoutMS: cfg.OperationTimeout.Milliseconds(), MaxP99MS: float64(cfg.MaxP99) / float64(time.Millisecond)},
		sent: make([]bool, m.Total), completed: make([]bool, m.Total),
		start: start,
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	observeCtx, stopObserver := context.WithCancel(runCtx)
	defer stopObserver()
	observerDone := make(chan error, 1)
	changed := make(chan struct{}, 1)
	go func() {
		err := transport.Observe(observeCtx, m, func(o Observation) {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.lastObservation = time.Now()
			if o.Index < 0 || o.Index >= m.Total || !s.sent[o.Index] {
				s.report.InvalidCompletions++
			} else if s.completed[o.Index] {
				s.report.DuplicateCompletions++
			} else {
				s.completed[o.Index] = true
				s.report.Completed++
				latency := o.ReceivedAt.Sub(s.start.Add(eventOffset(o.Index, m.Rate)))
				if latency < 0 {
					s.report.InvalidCompletions++
				} else {
					s.latencies = append(s.latencies, latency)
				}
			}
			select {
			case changed <- struct{}{}:
			default:
			}
		})
		observerDone <- err
		if err != nil && observeCtx.Err() == nil {
			cancel()
		}
	}()

	// Establish publishers before the scheduled start; connection setup is not load.
	publishers := make([]Publisher, 0, m.Concurrency)
	var runErr error
	for i := 0; i < m.Concurrency; i++ {
		publisher, err := transport.Publisher(runCtx)
		if err != nil {
			runErr = fmt.Errorf("open publishing worker: %w", err)
			break
		}
		publishers = append(publishers, publisher)
	}
	if runErr == nil {
		start = time.Now()
		s.mu.Lock()
		s.start = start
		s.report.StartedAt = start.UTC()
		s.mu.Unlock()
		jobs := make(chan int, m.Concurrency)
		var workers sync.WaitGroup
		for _, publisher := range publishers {
			workers.Add(1)
			go func(p Publisher) {
				defer workers.Done()
				for index := range jobs {
					s.mu.Lock()
					s.sent[index] = true
					s.report.Attempted++
					s.lags = append(s.lags, time.Since(start.Add(eventOffset(index, m.Rate))))
					s.mu.Unlock()
					event, err := BuildEvent(m, index)
					var body []byte
					if err == nil {
						body, err = json.Marshal(event)
					}
					if err == nil {
						publishCtx, done := context.WithTimeout(runCtx, cfg.OperationTimeout)
						err = p.Publish(publishCtx, body)
						done()
					}
					s.mu.Lock()
					if err != nil {
						s.report.PublishErrors++
						if s.firstPublishError == nil {
							s.firstPublishError = err
						}
					} else {
						s.report.Confirmed++
					}
					s.mu.Unlock()
				}
			}(publisher)
		}
		for index := 0; index < m.Total; index++ {
			due := start.Add(eventOffset(index, m.Rate))
			if err := waitUntil(runCtx, due); err != nil {
				runErr = err
				break
			}
			// Do not catch up a stalled generator with an unintended traffic burst.
			if time.Since(due) >= time.Second/time.Duration(m.Rate) {
				continue
			}
			select {
			case jobs <- index:
			default:
			}
		}
		close(jobs)
		workers.Wait()
		// Keep the scheduled measurement window even when the last event was early.
		if err := waitUntil(runCtx, start.Add(m.Duration)); err != nil && runErr == nil {
			runErr = err
		}
	}
	for _, publisher := range publishers {
		if err := publisher.Close(); err != nil {
			runErr = errors.Join(runErr, errors.New("close publishing worker failed"))
		}
	}
	publishSeconds := time.Since(start).Seconds()
	if publishSeconds < m.Duration.Seconds() {
		publishSeconds = m.Duration.Seconds()
	}

	drain := time.NewTimer(cfg.DrainTimeout)
	quiet := time.NewTimer(250 * time.Millisecond)
	defer quiet.Stop()
drainLoop:
	for runErr == nil {
		s.mu.Lock()
		done := s.report.Completed == s.report.Attempted
		lastObservation := s.lastObservation
		s.mu.Unlock()
		if done && (lastObservation.IsZero() || time.Since(lastObservation) >= 250*time.Millisecond) {
			break
		}
		select {
		case <-runCtx.Done():
			runErr = runCtx.Err()
			break drainLoop
		case <-drain.C:
			break drainLoop
		case <-changed:
		case <-quiet.C:
			quiet.Reset(250 * time.Millisecond)
		}
	}
	drain.Stop()
	stopObserver()
	observeErr := <-observerDone
	if observeErr != nil && !errors.Is(observeErr, context.Canceled) {
		runErr = errors.Join(runErr, fmt.Errorf("completion observer: %w", observeErr))
	}
	s.mu.Lock()
	report := s.report
	report.Latency = summarizeLatency(s.latencies)
	report.SchedulingLag = summarizeLatency(s.lags)
	firstPublishError := s.firstPublishError
	s.mu.Unlock()
	if firstPublishError != nil {
		runErr = errors.Join(runErr, fmt.Errorf("first publish failure: %w", firstPublishError))
	}
	report.FinishedAt = time.Now().UTC()
	report.Missed = report.Scheduled - report.Attempted
	elapsed := report.FinishedAt.Sub(report.StartedAt).Seconds()
	if elapsed > 0 {
		report.CompletionRate = float64(report.Completed) / elapsed
	}
	report.AchievedPublishRate = float64(report.Confirmed) / publishSeconds

	// A failed/cancelled run still needs a bounded diagnostic report.
	checkCtx, checkCancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.DrainTimeout)
	defer checkCancel()
	report.Verification, observeErr = store.Verify(checkCtx, m)
	if observeErr != nil {
		runErr = errors.Join(runErr, fmt.Errorf("verify persisted media: %w", observeErr))
	}
	report.DLQDepth, observeErr = transport.DLQDepth(checkCtx)
	if observeErr != nil {
		runErr = errors.Join(runErr, fmt.Errorf("inspect DLQ: %w", observeErr))
	}
	report.PendingCompletions, observeErr = transport.PendingCompletions(checkCtx, m)
	if observeErr != nil {
		runErr = errors.Join(runErr, fmt.Errorf("inspect pending completions: %w", observeErr))
	}
	evaluateReport(&report, cfg.MaxP99)
	if runErr != nil {
		report.Failures = append(report.Failures, "runner or verification failed; inspect stderr")
		report.Passed = false
	}
	saveCtx, saveCancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.OperationTimeout)
	defer saveCancel()
	if err := store.SaveReport(saveCtx, report); err != nil {
		report.Passed = false
		report.Failures = append(report.Failures, "could not persist run report")
		runErr = errors.Join(runErr, fmt.Errorf("save report: %w", err))
	}
	return report, runErr
}

func eventOffset(index, rate int) time.Duration {
	return time.Duration(int64(index) * int64(time.Second) / int64(rate))
}

func waitUntil(ctx context.Context, due time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	delay := time.Until(due)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func summarizeLatency(values []time.Duration) Latency {
	if len(values) == 0 {
		return Latency{}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	percentile := func(p int) float64 {
		index := (len(values)*p+99)/100 - 1
		return float64(values[index]) / float64(time.Millisecond)
	}
	return Latency{Count: len(values), P50MS: percentile(50), P95MS: percentile(95),
		P99MS: percentile(99), MaxMS: float64(values[len(values)-1]) / float64(time.Millisecond)}
}

func evaluateReport(r *Report, maxP99 time.Duration) {
	r.Failures = nil
	if r.Missed != 0 {
		r.Failures = append(r.Failures, "generator missed scheduled arrivals")
	}
	if r.PublishErrors != 0 {
		r.Failures = append(r.Failures, "publish outcomes failed or are uncertain")
	}
	if r.Completed != r.Scheduled {
		r.Failures = append(r.Failures, "not all scheduled recordings reached completion before drain timeout")
	}
	if r.DuplicateCompletions != 0 || r.InvalidCompletions != 0 {
		r.Failures = append(r.Failures, "duplicate or invalid completions observed")
	}
	if r.PendingCompletions != 0 {
		r.Failures = append(r.Failures, "completion deliveries remain after the observation window")
	}
	v := r.Verification
	if v.Expected != r.Scheduled || v.Found != v.Expected || v.Missing != 0 ||
		v.Duplicates != 0 || v.WrongScope != 0 || v.WrongMetadata != 0 || v.MissingDates != 0 {
		r.Failures = append(r.Failures, "database correctness checks failed")
	}
	if r.DLQDepth != 0 {
		r.Failures = append(r.Failures, "test vhost DLQ is not empty")
	}
	if maxP99 > 0 && r.Latency.P99MS > float64(maxP99)/float64(time.Millisecond) {
		r.Failures = append(r.Failures, "scheduled-to-completion p99 exceeds threshold")
	}
	r.Passed = len(r.Failures) == 0
}
