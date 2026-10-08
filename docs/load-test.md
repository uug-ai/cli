# Pipeline ingestion load testing

`load-test` is a bounded, single-runner harness for the real
`event -> monitor -> sequence -> event -> completion` path. It sends synthetic
recording **metadata**, not video bytes. It does not run Vault uploads, inference,
notifications, or a production-capacity benchmark by itself.

For a disk-backed local MongoDB/RabbitMQ stack with pinned workers, see
[`deployment/loadtest`](../../deployment/loadtest/README.md) in the adjacent
deployment checkout. Without that checkout, use the
[deployment repository](https://github.com/uug-ai/deployment/tree/main/loadtest).

## Safety

- Every command defaults to a no-connection dry run. Live access requires both
  `--execute` and `--confirm-test-target`, including read-only verification.
- Only `LOADTEST_MONGODB_URI` and `LOADTEST_RABBITMQ_URL` configure connections.
  Generic application environment variables and attached DLQ files are not used.
- Normally the database and RabbitMQ vhost must start with `loadtest_`.
  The pinned legacy monitor hardcodes `Kerberos` and older queue libraries use
  `/`. `--allow-default-names --database Kerberos` is an explicit exception for
  **dedicated isolated MongoDB/RabbitMQ instances**, never shared production.
- Preparation refuses an existing populated database without the harness
  sentinel. It never adopts a production database, replaces existing plan
  settings, or creates fixtures for existing customer identities.
- Use synthetic data and test credentials. Reports omit connection URLs.
  A report path is created with owner-only permissions and never overwritten.
- A run requires live event/monitor/sequence consumers and no ready backlog or
  DLQ messages. The completion consumer is registered before publication.
  An exclusive broker lock prevents two harness runs sharing the vhost.
  AMQP ready counts cannot detect all unacknowledged traffic: the entire stack
  must be idle and test-only, not merely its queues empty at one instant.
- Failed/incomplete runs remain for investigation. Automatic cleanup is limited
  to successful runs and their owned resources; no database drop or shared-queue
  purge is performed. Completion queues must be empty and unused, and are retained
  to preserve late deliveries (conditional deletion is not portable across
  quorum-queue versions). Run IDs are one-shot and cannot be reused.
- Preparation, execution and cleanup share a database-wide exclusive lock, so
  large seeding/deletion operations cannot contaminate another run's measurements.
  Failed/interrupted operations retain that lock for investigation; it has no
  automatic expiry that could allow a second run alongside uncertain work.

## Commands

Set explicit connection URLs for your test environment. For the local stack:

```sh
export LOADTEST_MONGODB_URI='mongodb://127.0.0.1:27028/Kerberos?replicaSet=loadtest-rs&directConnection=true&readConcernLevel=majority&w=majority&journal=true&wtimeoutMS=10000'
export LOADTEST_RABBITMQ_URL='amqp://loadtest:disposable-loadtest-only@127.0.0.1:5673/'
```

The credentials above are public disposable local-stack credentials.

```sh
# Preview preparation without opening either connection.
go run . load-test prepare \
  --run-id smoke-001 --database Kerberos --allow-default-names \
  --organisations 1 --projects 1 --devices 2 \
  --rate 5 --duration 10s --concurrency 4

# Freeze the workload and create its fixtures, outside the timed phase.
go run . load-test prepare \
  --run-id smoke-001 --database Kerberos --allow-default-names \
  --organisations 1 --projects 1 --devices 2 \
  --rate 5 --duration 10s --concurrency 4 \
  --deployment-label 'event=v1.3.2 monitor=v1.3.19 sequence=v1.7.1 local' \
  --execute --confirm-test-target

# Run uses the stored rate/counts/duration, not a second set of workload flags.
go run . load-test run \
  --run-id smoke-001 --database Kerberos --allow-default-names \
  --drain-timeout 60s --max-p99 10s --report smoke-001.json \
  --execute --confirm-test-target

# Persistence checks alone; this does not reconstruct past completion latency.
go run . load-test verify \
  --run-id smoke-001 --database Kerberos --allow-default-names \
  --execute --confirm-test-target

# Preview, then explicitly clean up a successfully finished run.
go run . load-test cleanup \
  --run-id smoke-001 --database Kerberos --allow-default-names
go run . load-test cleanup \
  --run-id smoke-001 --database Kerberos --allow-default-names \
  --execute --confirm-test-target
```

The example's 10-second p99 gate is a smoke threshold, **not** a production SLO.
Keep each JSON report and record the resolved image digests and hardware alongside
it. `--deployment-label` is operator-provided text, not independently verified
image provenance.

### Preparation profiles

Workload flags are accepted only by `prepare`:

| Flag | Meaning |
| --- | --- |
| `--organisations` | Distinct synthetic owner organisations |
| `--projects` | Projects per organisation; first project has the deterministic default ID |
| `--devices` | Devices per project |
| `--legacy` | Omit canonical device/subscription ownership and organisation/project documents; requires one default project |
| `--history-per-device` | Historical media seeded before timing, default 0 |
| `--rate`, `--duration` | Fixed scheduled arrivals; their product must be an integer event count |
| `--concurrency` | Publishing workers, each with its own confirm channel |

Each recording gets a unique key. Repeating a run ID is refused: repeatedly
upserting the same keys would measure replay, not fresh ingestion.

Fixture timestamps are frozen at preparation. This first version exercises a
known recording day; it does not simulate midnight transitions, continuous
recording time advancement, marker-density profiles, or mixed binary rollout
orchestration. Legacy and non-default-project profiles test ownership without a
migration prerequisite.

History seeding creates metadata documents, not object-storage files. The harness
installs a small fixture index baseline, not a copy of production indexes.
For meaningful comparisons use equivalent index definitions, data sizes and
database/cache/storage characteristics; record any additional test indexes.

## Measurement and failure semantics

- Scheduling is open-loop. Slow workers do not silently lower the offered rate.
  A bounded worker queue and skipped overdue slots prevent an unlimited backlog
  inside the generator or an unintended catch-up burst.
- `scheduled`, `attempted`, `confirmed`, `missed` and `publishErrors` are separate.
  A confirmation timeout is an **uncertain delivery**, not an automatic republish.
- Completion comes from the run-specific terminal queue, after sequence has
  returned forward. Broker acceptance and media insertion alone are not
  completion. Invalid and duplicate completions fail the run. After the expected
  completions, the collector waits for a 250ms quiet interval within the drain
  budget and checks for queued completions after closing the consumer. Pending
  deliveries fail the run; deliveries arriving beyond this bounded observation
  window cannot be ruled out by the report.
- `scheduledToCompletion` includes generator scheduling delay and pipeline
  processing/queueing; `schedulingLag` identifies generator pressure.
  Missing completions are failures, not excluded successes hidden by latency
  percentiles.
- Reports embed the frozen workload and timeout/threshold settings without
  connection URLs. Publish rate uses the publishing window; completion rate
  uses the run window including drain/quiet time.
- Verification uses batched indexed reads after the timed/drain phase. It checks
  unique media, ownership, device/recording metadata and applicable calendar dates.
  It does not poll MongoDB for every event while generating load.
- `dlqDepth` is a ready-message snapshot of the dedicated vhost's shared DLQ,
  **not** per-recording attribution or a counter of every historical DLQ arrival.
- Full-run failures include missed arrivals, unconfirmed publishes, outstanding
  completions, duplicates, invalid scopes/metadata/dates, nonempty DLQ and an
  optional p99 violation. Exit codes: 0 success/dry plan, 1 run/backend/check
  failure, 2 invalid usage/configuration.
- Ctrl-C stops generation and attempts a bounded verification/report save.
  A killed process can leave a running manifest intentionally fenced; reset only
  the disposable stack after investigation, rather than reusing its run ID.

Limits bound this initial runner's memory and preparation work: 1,000,000 scheduled
events, 100,000 devices, 1,000,000 historical documents, 256 publisher workers and
a 24-hour scheduled window. These are validation limits, **not measured capacity**.
If generating the requested rate is itself too expensive, the report fails on
missed arrivals. Distributed scheduling and multi-generator report aggregation
are not implemented yet.

## Validation

```sh
GOWORK=off go test -race ./internal/loadtest
GOWORK=off go test ./actions -run TestLoadTest
go run . load-test --help
```

Unit tests cover configuration safety, deterministic fixtures, MongoDB
preparation/verification behavior, confirm/return handling, consumer validation,
overload accounting, cancellation and report thresholds. Live capacity testing
must be conducted separately against an explicitly isolated deployment.
