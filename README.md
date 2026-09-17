# uug-ai/cli

This repository contains CLI tools for performing specific automations.

- `vault-to-hub-migration`: Migrating data from a Vault database to a Hub database.
- `reprocess-media`: Re-queue hub media for analysis when analysis is missing.
- `audit-legacy-compat`: Auditing legacy data shape compatibility across domains.
- `migrate-legacy-media`: Backfilling legacy media documents to current media shape.
- `organisations-bootstrap`: Bootstrapping Phase 3 organisation identity and memberships in ordered stages.
- `organisations-backfill`: Auditing canonical organisation ownership before the Phase 4 resource backfill.
- `generate-default-labels`: Adding labels to existing users.
- `dlq`: Inspecting, replaying, recovering, and safely seeding dead-letter queues across supported providers.


## Run

You can run these jobs in your cluster. The benefit is that you do not need to expose anything, and use the internal Kubernetes dns.

```sh
kubectl apply -f jobs/vault-to-hub-migration-job.yaml
```

```sh
kubectl apply -f jobs/generate-default-labels-job.yaml
```

```sh
kubectl apply -f jobs/migrate-legacy-media-job.yaml
```

## Installation and contributing

1. Clone the repository:

   ```sh
   git clone https://github.com/uug-ai/cli.git
   cd cli
   ```

2. Install dependencies:

   ```sh
   go mod tidy
   ```

3. Run example. This will execute the `vault-to-hub-migration` action. Please have a look at the various options you can provide for each action.

   ```sh
   go run main.go -action vault-to-hub-migration \
                  ...options
   ```

## Usage

### Dead-letter queue administration

The provider-neutral `dlq` command uses the shared `uug-ai/queue`
administrative API and supports RabbitMQ, SQS, Kafka, and Azure Event Hubs.

| Command | Purpose | Mutates the queue by default? |
| --- | --- | --- |
| `dlq inspect` | Count and group dead letters by their recorded source queue. | No |
| `dlq replay` | Republish messages without changing their payloads. | No; add `--execute` |
| `dlq recover` | Validate and safely repair Hub pipeline events before replay. | No; add `--execute` |
| `dlq seed` | Add synthetic envelopes for non-production testing. | No; add `--execute` |

`inspect`, `replay`, and `recover` scan at most `--limit` messages. This is a
bounded scan, not a promise to drain the queue. Always run `replay`, `recover`,
or `seed` without `--execute` first, review the plan, and then repeat the same
command with `--execute`.

#### Configure a provider

Provider flags have matching environment variables where possible. Prefer
environment variables for secrets so credentials are not saved in shell
history. For example, a RabbitMQ session can be configured as follows:

```sh
export RABBITMQ_HOST='<rabbitmq-host>'
export RABBITMQ_USERNAME='<rabbitmq-username>'
export RABBITMQ_PASSWORD='<rabbitmq-password>'
export RABBITMQ_VHOST='/'
```

Use quotes around values containing shell metacharacters. The supported provider
settings are:

| Provider | Required settings | Optional settings |
| --- | --- | --- |
| RabbitMQ | `RABBITMQ_HOST`, `RABBITMQ_USERNAME`, `RABBITMQ_PASSWORD` | `RABBITMQ_VHOST`, `RABBITMQ_CA_CERT_FILE` |
| Kafka | `KAFKA_BROKER` | `KAFKA_GROUP_ID` and `KAFKA_USERNAME`, `KAFKA_PASSWORD`, `KAFKA_MECHANISM`, `KAFKA_SECURITY_PROTOCOL` for SASL |
| Azure Event Hubs | `AZURE_EVENTHUB_CONNECTION_STRING` and an existing, dedicated `KAFKA_GROUP_ID` | `AZURE_EVENTHUB_NAMESPACE` |
| SQS | `AWS_REGION` and the standard AWS credential chain | `SQS_ENDPOINT`, `SQS_SESSION_TOKEN`, `SQS_MESSAGE_GROUP_ID`, or the static credential flags |

`QUEUE_PROVIDER` and `DEAD_LETTER_QUEUE` can replace the common `--provider`
and `--dead-letter` flags. Command-line flags remain useful when operating
multiple queues from the same shell.

#### Dead-letter envelope

Provider-independent messages use the `uug.ai/dead-letter/v1` envelope:

```json
{
  "schema": "uug.ai/dead-letter/v1",
  "payload": "<base64-encoded original message>",
  "deadLetter": {
    "source": "kcloud-sequence-queue",
    "destination": "dead-letter-queue",
    "replayDestination": "kcloud-event-queue",
    "service": "hub-pipeline-sequence",
    "reason": "handler_error",
    "attempts": 1,
    "timestamp": "2026-09-17T12:56:43Z"
  }
}
```

The payload is base64 encoded by JSON so any original byte sequence can be
preserved. Replay publishes the decoded original payload, not the envelope.
Optional metadata records where and why the message was parked and allows
provider-neutral inspection and routing. Messages written before envelopes were
introduced are reported as `Legacy/unknown`; they can still be replayed with an
explicit `--destination`.

#### Inspect the queue

Inspect up to 100 messages:

```sh
go run . dlq inspect \
  --provider rabbitmq \
  --dead-letter dead-letter-queue \
  --limit 100
```

The report groups envelope messages by their recorded source and shows the
oldest and newest dead-letter timestamps in each group. `Scanned` is the number
read during this bounded operation, `TOTAL` is the number matching an optional
`--source` filter, and `Legacy/unknown` counts raw messages without recognized
dead-letter metadata.

To inspect only sequence failures:

```sh
go run . dlq inspect \
  --provider rabbitmq \
  --dead-letter dead-letter-queue \
  --source kcloud-sequence-queue \
  --limit 100
```

Inspection never publishes or settles messages.

#### Replay messages unchanged

Use `replay` when the original payload is still valid and needs no pipeline
repair. First run a dry run:

```sh
go run . dlq replay \
  --provider rabbitmq \
  --dead-letter dead-letter-queue \
  --source kcloud-monitor-queue \
  --limit 100
```

The dry run prints the planned count for each replay destination and finishes
with `No messages were moved`. Execute the reviewed plan by adding
`--execute`:

```sh
go run . dlq replay \
  --provider rabbitmq \
  --dead-letter dead-letter-queue \
  --source kcloud-monitor-queue \
  --limit 100 \
  --execute
```

The destination is selected in this order:

1. `--destination`, when explicitly provided by the operator.
2. The envelope's recorded `replayDestination`.
3. The envelope's recorded source, for older envelopes.

Legacy raw messages have no routing metadata and therefore require
`--destination`. A destination override is also useful when intentionally
routing older messages through a new entry point:

```sh
go run . dlq replay \
  --provider sqs \
  --dead-letter dead-letter-queue \
  --destination kcloud-monitor-queue \
  --limit 100 \
  --execute
```

Replay refuses to publish to the configured dead-letter destination. It
publishes each message before settling its dead-letter copy, so a publish
failure retains the source message. Kafka and Azure Event Hubs do not allow an
executed replay with `--source` because their offsets must be committed
contiguously; an unfiltered executed replay or a filtered dry run is supported.

#### Recover Hub pipeline events

Use `recover` instead of `replay` for Hub pipeline events that may contain
expired persistent-recording URLs or payload fields that are unsafe for the
deployed downstream versions. Recovery resumes the event at its current
`events[0]` stage and sends it through the event router; it does not rerun
stages already removed from `events`.

The router destination is mandatory. The following example scans sequence dead
letters and plans recovery in batches of 10:

```sh
go run . dlq recover \
  --provider rabbitmq \
  --dead-letter dead-letter-queue \
  --source kcloud-sequence-queue \
  --destination kcloud-event-queue \
  --limit 100 \
  --batch-size 10 \
  --batch-delay 2s \
  --timeout 1m \
  --legacy-user-ownership
```

Without `--execute`, recovery:

- validates every candidate and reports its current stage;
- reports recoverable candidates, on-demand bypasses, planned audit
  sanitizations, and planned historical tail suppressions;
- groups rejected messages by an `UNRECOVERABLE REASON`; and
- makes no Vault requests and moves no messages.

For an executed recovery containing persistent recordings, configure Vault and
repeat the reviewed command with `--execute`:

```sh
export KERBEROS_STORAGE_URI='https://<vault-host>/api'
export KERBEROS_STORAGE_ACCESS_KEY='<vault-access-key>'
export KERBEROS_STORAGE_SECRET='<vault-secret>'
export KERBEROS_STORAGE_PROVIDER='<fallback-provider>'

go run . dlq recover \
  --provider rabbitmq \
  --dead-letter dead-letter-queue \
  --source kcloud-sequence-queue \
  --destination kcloud-event-queue \
  --limit 100 \
  --batch-size 10 \
  --batch-delay 2s \
  --timeout 1m \
  --legacy-user-ownership \
  --execute
```

`--limit` bounds the whole run. `--batch-size` controls how many messages are
validated, refreshed, and published together. `--batch-delay` is applied
between executed batches, and `--timeout` applies separately to each batch.
If a later batch fails, earlier successful batches remain completed; the failed
batch is not published and its counters are not reported as completed work.

##### URL refresh behavior

For `request == "persist"`, each executed batch calls Vault's
`POST /api/storage/bulk` endpoint and replaces only `payload.signedUrl` plus any
required safety normalization. Storage provider selection uses the event's
`source`, then its `provider`, then `KERBEROS_STORAGE_PROVIDER`. Unknown JSON
fields are preserved.

For `request == "ondemand"`, recovery does not contact Vault because the
existing URL belongs to the on-demand flow. The payload is replayed byte for
byte when no safety normalization is needed. An on-demand-only execution can
omit all Vault settings. If a persistent candidate is encountered without
Vault settings, that batch stops before publication.

`KERBEROS_STORAGE_URI`, `KERBEROS_STORAGE_ACCESS_KEY`, and
`KERBEROS_STORAGE_SECRET` must be provided together. Equivalent
`--vault-uri`, `--vault-access-key`, and `--vault-secret` flags are available.
Vault must use HTTPS. Plain HTTP is accepted only for loopback development or
with the explicit `--vault-allow-insecure-http` override. Redirects, incomplete
bulk responses, invalid URLs, and oversized responses fail closed before
publishing the affected batch. Transport errors do not print the Vault
endpoint.

##### Pipeline safety behavior

Recovery applies the following checks before publishing:

- Every stage name is validated, including messages that would otherwise reach
  older workers that assume `events[1:]` exists.
- The embedded `monitorStage.user.audit` snapshot is removed. Model generations
  disagree on its shape, and resumed stages do not use it.
- Embedded storage URLs and existing on-demand signed URLs must be valid HTTP
  or HTTPS URLs.
- Malformed, unsupported, or conflicting messages remain in the dead-letter
  queue while other valid messages continue.
- Existing non-empty canonical `organisationId` and `projectId` values are
  never synthesized, removed, or overwritten.

Recordings older than `--historical-tail-max-age` (15 minutes by default)
resume only through stages before `throttler` and `notification`. This prevents
stale monitor snapshots from regressing throttle state and avoids expired
notifications. A message whose current stage is already `throttler` or
`notification` is retained. `--allow-historical-tail` disables this protection
and should be used only after explicitly accepting those downstream effects.

##### Legacy ownership compatibility

Some older sequence and analysis workers ignore canonical monitor ownership and
scope persistence from `monitorStage.user.id`. Add
`--legacy-user-ownership` whenever those versions are deployed.

Known canonical-aware releases begin with sequence `v1.6.27` and analysis
`v1.8.7`. Deployments containing older versions of either worker should use the
legacy compatibility flag.

With this flag, an event is compatible only when every existing canonical
ownership field agrees with the stable user/owner ID. Missing canonical fields
remain compatible with the legacy default-project interpretation:

```text
organisationId = projectId = monitorStage.user.id
```

An event with an existing non-default canonical project cannot be represented
by that legacy model without changing its ownership. Recovery reports it as
`legacy-owner-conflict` and retains it. Do not remove the flag merely to force
replay, replace the canonical project with the user ID, or replace the user ID
with the project ID. Upgrade or backport canonical ownership handling in the
downstream workers before recovering such messages.

Canonical-aware downstream versions should preserve existing canonical fields
and use the stable owner ID only as a fallback when those fields are absent.
They do not require `--legacy-user-ownership`.

##### Read the recovery report

| Field | Meaning |
| --- | --- |
| `Scanned` | Messages read from the bounded dead-letter scan. |
| `Matched` | Messages matching the optional source filter. |
| `Planned` | Valid transformations that would be, or were, published. |
| `Recovery candidates` | Valid pipeline messages considered for recovery. |
| `Legacy user audit sanitizations` | Embedded audit snapshots removed. |
| `Historical tail suppressions` | Events whose unsafe tail stages were removed. |
| `URL refresh bypassed` | Exact on-demand requests that kept their existing URL. |
| `URLs refreshed` | Persistent event URLs actually refreshed; always zero in a dry run. |
| `Unrecoverable` | Messages retained because validation failed; see the reason table above it. |
| `Replayed` | Messages successfully published and settled; always zero in a dry run. |
| `Retained` | Messages left in the dead-letter queue, including all messages in a dry run. |
| `Legacy/unknown` | Messages without a recognized dead-letter envelope. |
| `Unroutable` | Messages for which no safe replay destination was available. |

Each provider holds retained settlement metadata for the entire bounded
operation so a message is scanned at most once. Choose `--limit` as both a work
and memory safety bound. Kafka additionally retains messages after a skipped
message in the same partition because committing a later offset would also
commit the skipped message; other partitions continue.

#### Seed synthetic messages

Use `seed` only with non-production queues. First review the dry run:

```sh
go run . dlq seed \
  --provider rabbitmq \
  --dead-letter test-dead-letter-queue \
  --sources test-monitor-queue,test-analysis-queue \
  --count 10
```

Then add `--execute` to publish the synthetic envelopes:

```sh
go run . dlq seed \
  --provider rabbitmq \
  --dead-letter test-dead-letter-queue \
  --sources test-monitor-queue,test-analysis-queue \
  --count 10 \
  --execute
```

Synthetic payloads contain only a sequence number and source name. Replaying
them publishes those payloads to their recorded source destinations.

Run `go run . dlq inspect --help`, `go run . dlq replay --help`,
`go run . dlq recover --help`, or `go run . dlq seed --help` for the complete
flag reference.

### Organisation identity bootstrap

Run the Phase 3 identity migration in order: `owners`, `sub-users`, then the
read-only `verify` stage. Start each mutating stage in `dry-run`; existing
non-zero selections are preserved.

```sh
go run . -action organisations-bootstrap \
         -stage owners \
         -mode dry-run \
         -mongodb-uri "mongodb://<host>" \
         -mongodb-destination-database <database> \
         -legacy-org-policy report \
         -report-file organisations-bootstrap-owners.json
```

Repeat with `-stage sub-users` only after the owner stage verifies green, then
run `-stage verify -mode dry-run`. Scope canaries with either `-username` or
`-organisation-id`; those flags are mutually exclusive. Exit codes are `0` for
a green run, `2` for identity conflicts or failed verification, `1` for an
operational failure, and `64` for invalid or unsafe arguments.

Live owner and sub-user runs require the canonical organisation and membership
indexes. They acquire a stage/scope-specific lease in `migration_checkpoints`,
advance only after a complete tenant verifies, and support `-resume` or
`-restart`. Live writes are limited to `organisation`, `organisation_users`,
missing `users.organisationId` values, and the checkpoint. The destructive
`archive-delete` policy remains disabled until guarded archive/reference checks
are implemented.

### Organisation ownership backfill

This action currently implements the read-only Phase 3e/Phase 4 preflight. It
inventories canonical tenant presence, BSON types, legacy candidate coverage,
and current indexes for registered adapters. The `subscriptions` adapter also
resolves canonical-missing rows through persisted user/master identity, reports
bounded ownership conflicts and proposed writes, inventories observed document
shapes, and verifies required index key order. Live writes, checkpoint writes,
resume/restart, and index creation remain disabled until Phase 4.

```sh
go run . -action organisations-backfill \
         -mode dry-run \
         -mongodb-uri "mongodb://<host>" \
         -mongodb-destination-database <database> \
         -collection sites \
         -adapter-version v1 \
         -report-file organisations-backfill-sites.json
```

Use `-all` instead of `-collection` to inspect every ready adapter. The command
exits `0` when preflight passes, `2` for invalid canonical tenant data, `1` for
operational failures, and `64` for unsafe or invalid arguments.

Subscriptions support an organisation-scoped read-only canary. The scope
includes canonical rows for the organisation and canonical-missing rows whose
legacy payer resolves to that organisation; it does not assume
`user_id == organisation_id`.

```sh
go run . -action organisations-backfill \
         -mode dry-run \
         -mongodb-uri "mongodb://<host>" \
         -mongodb-destination-database <database> \
         -collection subscriptions \
         -adapter-version v1 \
         -organisation-id <organisation-object-id> \
         -report-file organisations-backfill-subscriptions.json
```

The subscription rollout index contracts are versioned in
`indexes/migration-hub-subscription-ownership-21-08-2026.txt`. Audit them before
the backfill with:

```sh
go run . -action check-indexes \
         -mongodb-uri "mongodb://<host>" \
         -mongodb-destination-database <database> \
         -collections subscriptions \
         -mode dry-run \
         -index-version migration-hub-subscription-ownership-21-08-2026
```

Access tokens use canonical string `organisationId`, then resolve legacy
creator `userId` through the persisted user's stable `user_id` parent or own
`_id`. Mutable user organisation selection is never ownership evidence.
Missing `projectId` resolves to the organisation's deterministic default;
explicit projects must belong to that organisation. Token credentials, scopes,
expiration, and audit provenance are preserved. The read-only report includes
optional normalization writes, observed shapes, conflicts, and ordered
`{organisationId, projectId, _id}`, `{userId, projectId, _id}`, and `{_id}`
index contracts. Runtime requires Hub API v1.9.63 or later.

Use `-organisation-id` with `-collection tokens` for a scoped canary. The
canary includes canonical tokens and canonical-missing tokens whose stable
creator identity resolves to that organisation.

```sh
go run . -action organisations-backfill \
         -mode dry-run \
         -mongodb-uri "mongodb://<host>" \
         -mongodb-destination-database <database> \
         -collection tokens \
         -adapter-version v1 \
         -organisation-id <organisation-object-id> \
         -report-file organisations-backfill-tokens-canary.json
```

Alerts use canonical `organisationId`, then `master_user_id`, then `user_id`
only when every higher-precedence field is absent. Their read-only resolver and
index contracts can be audited with:

```sh
go run . -action organisations-backfill \
         -mode dry-run \
         -mongodb-uri "mongodb://<host>" \
         -mongodb-destination-database <database> \
         -collection alerts \
         -adapter-version v1 \
         -report-file organisations-backfill-alerts.json

go run . -action check-indexes \
         -mongodb-uri "mongodb://<host>" \
         -mongodb-destination-database <database> \
         -collections alerts \
         -mode dry-run \
         -index-version migration-hub-alert-ownership-21-08-2026
```

Workflows and workflow runs have separate project-scoped resolvers. Definitions
use canonical `organisationId`, then `organisation_id`, then stable creator
`user_id` only when both organisation fields are absent. Runs use canonical
`organisationId`, then legacy tenant `userid`; accidental `user_id` is inventoried
but never becomes ownership. Explicit projects must belong to the resolved
organisation. Runs also validate case/media source ownership and database
workflow ownership when those relationships can be resolved; a missing config
workflow definition is not a conflict.

Workflow-run project stamping starts at hub-workflows v1.0.25. The complete
reader floor is Hub API PR 557, hub-workflows PR 61, and Hub Cleanup PR 44.
The run adapter reports the active project-scoped status query plus canonical,
legacy, and global recording/start retention indexes; obsolete grouping indexes
are no longer part of the contract. Audit the ordered definition and run index
families with:

```sh
go run . -action check-indexes \
         -mongodb-uri "mongodb://<host>" \
         -mongodb-destination-database <database> \
         -collections workflows,workflow_runs \
         -mode dry-run \
         -index-version migration-hub-workflow-ownership-26-08-2026
```

Analysis and detections are also project scoped. Analysis resolves canonical
`organisationId`, then legacy tenant `userid`, then `user_id`; missing projects
belong to the resolved organisation's deterministic default project. Detection
ownership is derived from the matching analysis document by recording `key`,
and persisted canonical ownership is checked against that trusted source. A
missing source or ownership mismatch is a conflict rather than guessed from
workflow, device, or actor fields. The detection writer floor includes Hub
Pipeline Analysis PR 95, which keeps legacy tolerance limited to the default
project while requiring exact non-default project matches.

```sh
go run . -action check-indexes \
         -mongodb-uri "mongodb://<host>" \
         -mongodb-destination-database <database> \
         -collections analysis,detections \
         -mode dry-run \
         -index-version migration-hub-analysis-detection-ownership-27-08-2026
```

The `notifications` collection contains two unrelated shapes. Personal mailbox
envelopes have `{user_id, data: [...]}` and are counted but excluded from tenant
normalization. Flat notification events are project scoped and resolve canonical
`organisationId`, then `alert_master_user`, then the persisted recipient
`userid` through its stable primary owner. `alert_user`, email, and message data
remain provenance or payload and never become ownership candidates. Unknown
shapes block an unscoped audit rather than being guessed.

```sh
go run . -action organisations-backfill \
         -mode dry-run \
         -mongodb-uri "mongodb://<host>" \
         -mongodb-destination-database <database> \
         -collection notifications \
         -adapter-version v1 \
         -report-file organisations-backfill-notifications.json

go run . -action check-indexes \
         -mongodb-uri "mongodb://<host>" \
         -mongodb-destination-database <database> \
         -collections notifications \
         -mode dry-run \
         -index-version migration-hub-notification-ownership-02-09-2026
```

`channels` is embedded configuration rather than a standalone collection,
`settings` is platform-global, and the deprecated `sequences` collection is no
longer written. They are explicit exclusions from organisation backfill rather
than incomplete adapters.

`audit_events` requires canonical BSON ObjectID `organisationId` and treats
`actorId` only as provenance. Organisation, membership, role, subscription,
and user actions remain organisation-only. Case, workflow, device, media, and
notification actions resolve project context from their authoritative target;
notification events may resolve through their referenced media source. Missing
or deleted targets, unknown action/target combinations, malformed metadata, and
target/source ownership disagreements are conflicts. A valid agreeing
`metadata.projectId` is evidence only. The report proposes only optional
`projectId` writes and never proposes organisation ownership from an actor.

The adapter reports BSON field types/shapes and exact ordered coverage for
`{organisationId,timestamp:-1}`, `{organisationId,actorId,timestamp:-1}`,
`{organisationId,targetType,targetId,timestamp:-1}`, and
`{organisationId,projectId,timestamp:-1}`.

### Vault to Hub Migration

This tool migrates data from a Vault database to a Hub database.

#### Flow (how media is selected and routed)

- **Missing sequence**: Vault media that is not present in any Hub sequence is sent through the standard pipeline (`monitor,sequence,...`) using the configured `-pipeline` stages.
- **Sequence present**: If a Hub sequence entry exists for a media item, it will be considered for **analysis-only** migration when `analysis` is included in `-pipeline`.
- **Analysis selection rules (analysis-only path)**:
  - If `analysis_id` is empty on the sequence image, the item is queued for analysis.
  - If `analysis_id` is present but the analysis document is missing, the item is queued for analysis.
  - If `analysis_id` is present and the analysis exists, the item is skipped.
  - If `-operation-count` is set and the analysis exists but has fewer `resolvedoperations` than the threshold, the analysis is deleted and the item is queued for analysis.
  - If no `analysis_id` exists but an analysis exists for the same media key, the item is skipped unless `-operation-count` is set and the resolved count is below the threshold (then it is deleted and reprocessed).

Notes:
- The analysis-only path assumes a valid `-vault-url` so thumbnail/dominantcolor/sprite workers can fetch the media.
- The analysis-only path skips monitor/sequence side effects (activity counters, sequence creation).

#### Command Line Arguments

- `-action`: The action to take (required). For migration, use `vault-to-hub-migration`.
- `-mongodb-uri`: The MongoDB URI (optional if host and port are provided).
- `-mongodb-host`: The MongoDB host (optional if URI is provided).
- `-mongodb-port`: The MongoDB port (optional if URI is provided).
- `-mongodb-source-database`: The source database name (required).
- `-mongodb-destination-database`: The destination database name (required).
- `-mongodb-database-credentials`: The database credentials (optional).
- `-mongodb-username`: The MongoDB username (optional).
- `-mongodb-password`: The MongoDB password (optional).
- `-username`: The username to filter data (required).
- `-queue`: The integration used to transfer events to the hub pipeline.
- `-vault-url`: Optional Vault API URL override (e.g. `https://vault.example.com/api`).
- `-start-timestamp`: The start timestamp for filtering data (required).
- `-end-timestamp`: The end timestamp for filtering data (required).
- `-timezone`: The timezone for converting timestamps (optional, default is `UTC`).
- `-pipeline`: The pipeline to execute (optional, default is `monitor,sequence`).
- `-operation-count`: Minimum resolved operations required to keep an existing analysis. If provided, analyses below this count are deleted and reprocessed (analysis-only path). Set to `0` to disable.
- `-batch-size`: The size of each batch (optional, default is `10`).
- `-batch-delay`: The delay between batches in milliseconds (optional, default is `1000`).
- `-mode`: You can choose to run a `dry-run` or `live`.

#### Example

To run the Vault to Hub migration, use the following command:

```sh
go run main.go -action vault-to-hub-migration \
               -mongodb-uri "mongodb+srv://<username>:<password>@<host>/<database>?retryWrites=true&w=majority&appName=<appName>" \
               -mongodb-source-database=<sourceDatabase> \
               -mongodb-destination-database=<destinationDatabase> \
               -queue <rabbitmq-integration> \
               -vault-url https://vault.kerberos.io/api \
               -username <username> \
               -start-timestamp <startTimestamp> \
               -end-timestamp <endTimestamp> \
               -timezone <timezone> \
               -pipeline 'monitor,sequence,analysis' \
               -operation-count 3 \
               -mode dry-run \
               -batch-size 100 \
               -batch-delay 1000
```

#### Output

            _    _ _    _  _____     _____ _ _
            | |  | | |  | |/ ____|   / ____(_) |
            | |  | | |  | | |  __   | |     _| |
            | |  | | |  | | | |_ |  | |    | | |
            | |__| | |__| | |__| |  | |____| | |
            \____/ \____/ \_____|   \_____|_|_|


      Starting Vault to Hub migration...
      2024/12/12 09:37:39 ====================================
      2024/12/12 09:37:39 Configuration:
      2024/12/12 09:37:39   MongoDB URI: mongodb+srv://xxxx
      2024/12/12 09:37:39   MongoDB Host:
      2024/12/12 09:37:39   MongoDB Port:
      2024/12/12 09:37:39   MongoDB Source Database: KerberosStorage
      2024/12/12 09:37:39   MongoDB Destination Database: Kerberos
      2024/12/12 09:37:39   MongoDB Database Credentials:
      2024/12/12 09:37:39   MongoDB Username:
      2024/12/12 09:37:39   MongoDB Password: ************
      2024/12/12 09:37:39   Queue: rabbitmq-xxxx
      2024/12/12 09:37:39   Username: xxxx
      2024/12/12 09:37:39   Start Time 2024-04-01 08:47:40 +0200 CEST
      2024/12/12 09:37:39   End Time 2025-04-06 17:41:00 +0200 CEST
      2024/12/12 09:37:39   Pipeline monitor,sequence,analysis
      2024/12/12 09:37:39 ====================================

      >> Please wait while we migrate the data. Press Ctrl+C to stop the process.
      Vault to Hub: delta complete   42s [====================================================================] 100%
      Transferring media    1s [====================================================================] 100%
      2024/12/12 09:38:26
      2024/12/12 09:38:26 >>Media transferred:
      2024/12/12 09:38:26
      2024/12/12 09:38:26   +---------------------------------------------------------------------------------------+-----------------+-----------------+-------------------------------------+
      2024/12/12 09:38:26   | File Name                                                                             | File Size       | Timestamp       | Device                              |
      2024/12/12 09:38:26   +---------------------------------------------------------------------------------------+-----------------+-----------------+-------------------------------------+
      2024/12/12 09:38:26   | xxxxxxxx/1733992176_6-967003_melle-insidegarage_200-200-400-400_25819_769.mp4         | 7261430         | 1733992211      | melle-insidegarage                  |
      2024/12/12 09:38:26   | xxxxxxxx/1733991780_6-967003_melle-garage_200-200-400-400_256_769.mp4                 | 14206272        | 1733991818      | melle-garage                        |
      2024/12/12 09:38:26   | xxxxxxxx/1733991781_6-967003_melle-street_200-200-400-400_819_769.mp4                 | 3603494         | 1733991817      | melle-street                        |
      2024/12/12 09:38:26   | xxxxxxxx/1733991587_6-967003_gb-side_200-200-400-400_342_769.mp4                      | 8167534         | 1733991611      | vSQBjrhqGGOXseLoidIBFhKeJjCjTM      |
      2024/12/12 09:38:26   | xxxxxxxx/1733991547_6-967003_gb-side_200-200-400-400_883_769.mp4                      | 12197049        | 1733991586      | vSQBjrhqGGOXseLoidIBFhKeJjCjTM      |
      2024/12/12 09:38:26   | xxxxxxxx/1733991501_6-967003_gb-side_200-200-400-400_1472_769.mp4                     | 12130152        | 1733991538      | vSQBjrhqGGOXseLoidIBFhKeJjCjTM      |
      2024/12/12 09:38:26   | xxxxxxxx/1733990945_6-967003_gb-side_200-200-400-400_6547_769.mp4                     | 7097803         | 1733990966      | vSQBjrhqGGOXseLoidIBFhKeJjCjTM      |
      2024/12/12 09:38:26   | xxxxxxxx/1733990903_6-967003_gb-side_200-200-400-400_9313_769.mp4                     | 7709073         | 1733990928      | vSQBjrhqGGOXseLoidIBFhKeJjCjTM      |
      2024/12/12 09:38:26   | xxxxxxxx/1733990868_6-967003_gb-frontdoor_200-200-400-400_158_769.mp4                 | 1882747         | 1733990885      | lQtymLrehWpHkTavfcNTFgwfDMoSfg      |
      2024/12/12 09:38:26   | xxxxxxxx/1733990742_6-967003_gb-side_200-200-400-400_214198_769.mp4                   | 7015592         | 1733990765      | vSQBjrhqGGOXseLoidIBFhKeJjCjTM      |
      2024/12/12 09:38:26   | xxxxxxxx/1733990698_6-967003_gb-frontdoor_200-200-400-400_155_769.mp4                 | 1487304         | 1733990713      | lQtymLrehWpHkTavfcNTFgwfDMoSfg      |
      2024/12/12 09:38:26   | xxxxxxxx/1733990683_6-967003_gb-frontdoor_200-200-400-400_177_769.mp4                 | 1966309         | 1733990701      | lQtymLrehWpHkTavfcNTFgwfDMoSfg      |
      2024/12/12 09:38:26   | xxxxxxxx/1733990499_6-967003_gb-frontdoor_200-200-400-400_176_769.mp4                 | 3430326         | 1733990530      | lQtymLrehWpHkTavfcNTFgwfDMoSfg      |
      2024/12/12 09:38:26   | xxxxxxxx/1733990314_6-967003_gb-frontdoor_200-200-400-400_168_769.mp4                 | 3667828         | 1733990347      | lQtymLrehWpHkTavfcNTFgwfDMoSfg      |
      2024/12/12 09:38:26   | xxxxxxxx/1733990130_6-967003_gb-frontdoor_200-200-400-400_153_769.mp4                 | 3839485         | 1733990165      | lQtymLrehWpHkTavfcNTFgwfDMoSfg      |
      2024/12/12 09:38:26   | xxxxxxxx/1733989945_6-967003_gb-frontdoor_200-200-400-400_151_769.mp4                 | 3590120         | 1733989979      | lQtymLrehWpHkTavfcNTFgwfDMoSfg      |
      2024/12/12 09:38:26   | xxxxxxxx/1733989761_6-967003_gb-frontdoor_200-200-400-400_164_769.mp4                 | 3730173         | 1733989794      | lQtymLrehWpHkTavfcNTFgwfDMoSfg      |
      2024/12/12 09:38:26   | xxxxxxxx/1733989573_6-967003_gb-frontdoor_200-200-400-400_154_769.mp4                 | 3878792         | 1733989606      | lQtymLrehWpHkTavfcNTFgwfDMoSfg      |
      2024/12/12 09:38:26   | xxxxxxxx/1733989397_6-967003_gb-frontdoor_200-200-400-400_166_769.mp4                 | 3846902         | 1733989430      | lQtymLrehWpHkTavfcNTFgwfDMoSfg      |
      2024/12/12 09:38:26   | xxxxxxxx/1733989379_6-967003_gb-frontdoor_200-200-400-400_162_769.mp4                 | 2107887         | 1733989400      | lQtymLrehWpHkTavfcNTFgwfDMoSfg      |
      2024/12/12 09:38:26   +---------------------------------------------------------------------------------------+-----------------+-----------------+-------------------------------------+


### Audit legacy compatibility

This tool performs a read-only compatibility audit over legacy data, and reports missing required/recommended fields by domain.

#### Command Line Arguments

- `-action`: The action to take (required). For compatibility audit, use `audit-legacy-compat`.
- `-mongodb-uri`: The MongoDB URI (optional if host and port are provided).
- `-mongodb-host`: The MongoDB host (optional if URI is provided).
- `-mongodb-port`: The MongoDB port (optional if URI is provided).
- `-mongodb-source-database`: The source database name (required if destination database is not set).
- `-mongodb-destination-database`: The destination database name (required if source database is not set).
- `-mongodb-database-credentials`: The database credentials (optional).
- `-mongodb-username`: The MongoDB username (optional).
- `-mongodb-password`: The MongoDB password (optional).
- `-username`: Optional username used to resolve organisation scope.
- `-organisation-id`: Optional organisation/user scope ID.
- `-start-timestamp`: Optional start timestamp for time-scoped collections.
- `-end-timestamp`: Optional end timestamp for time-scoped collections.
- `-domains`: Optional comma-separated domain list. Default: `media,analysis,users,devices,groups,sites,settings`.
- `-mode`: Accepted for consistency (`dry-run`/`live`), this action is read-only.

#### Example

```sh
go run main.go -action audit-legacy-compat \
               -mode dry-run \
               -mongodb-uri "mongodb+srv://<username>:<password>@<host>/<database>?retryWrites=true&w=majority&appName=<appName>" \
               -mongodb-destination-database=<database> \
               -organisation-id <organisationId> \
               -start-timestamp <startTimestamp> \
               -end-timestamp <endTimestamp> \
               -domains media,analysis,users
```

### Migrate legacy media

This tool backfills missing fields on legacy media documents and can insert missing media docs from analysis-shaped records.

#### Command Line Arguments

- `-action`: The action to take (required). For this migration, use `migrate-legacy-media`.
- `-mongodb-uri`: The MongoDB URI (optional if host and port are provided).
- `-mongodb-host`: The MongoDB host (optional if URI is provided).
- `-mongodb-port`: The MongoDB port (optional if URI is provided).
- `-mongodb-source-database`: Source database name (optional if destination database is set).
- `-mongodb-destination-database`: Destination database name (optional if source database is set).
- `-mongodb-database-credentials`: The database credentials (optional).
- `-mongodb-username`: The MongoDB username (optional).
- `-mongodb-password`: The MongoDB password (optional).
- `-organisation-id`: Recommended. Scope migration to one organisation.
- `-username`: Optional alternative to resolve organisation scope.
- `-start-timestamp`: Recommended. Use bounded windows for safer runs.
- `-end-timestamp`: Recommended. Use bounded windows for safer runs.
- `-migration-timeout-minutes`: Optional timeout for this action (default `60`). Set to `0` to disable timeout for very large datasets.
- `-skip-matched-count`: Optional performance flag (default `true`). Skips the initial `CountDocuments` pre-scan; report will show `matchedFilter: -1`.
- `-migration-version`: Optional migration-step version selector (default `1`, latest supported). Use this to branch future media migration behavior without changing CLI shape.
- `-check-migration-indexes`: Optional. Checks required indexes for this action and reports missing/existing.
- `-apply-migration-indexes`: Optional. Creates missing required indexes for this action.
- `-mode`: `dry-run` (recommended first) or `live`.
- `-generate-default-marker-options`: Optional. When set, generate default `marker_options` with category `classification`. If `-organisation-id` is provided, it targets that single org/user id. If omitted, it targets all users in `users`. This always seeds a built-in default classification list, then adds any extra discovered classifications from scoped media/analysis data.

#### Recommended run strategy

1. Run `dry-run` first with `-organisation-id` and a bounded timestamp window.
2. Review the report (`Needs`, `Cases`, examples).
3. Run `live` with the same scope.
4. Repeat per time window until complete.

#### Example

```sh
go run main.go -action migrate-legacy-media \
               -mode dry-run \
               -mongodb-uri "mongodb+srv://<username>:<password>@<host>/<database>?retryWrites=true&w=majority&appName=<appName>" \
               -mongodb-destination-database=<database> \
               -organisation-id <organisationId> \
               -migration-version 1 \
               -migration-timeout-minutes 60 \
               -skip-matched-count=true \
               -check-migration-indexes \
               -apply-migration-indexes \
               -generate-default-marker-options \
               -start-timestamp <startTimestamp> \
               -end-timestamp <endTimestamp>
```

### Generate default labels

This tool adds starting labels to existing users in the database.

#### Command Line Arguments

- `-action`: The action to take (required). For labels, use `generate-default-labels`.
- `-mongodb-uri`: The MongoDB URI (optional if host and port are provided).
- `-mongodb-host`: The MongoDB host (optional if URI is provided).
- `-mongodb-port`: The MongoDB port (optional if URI is provided).
- `-mongodb-source-database`: The source database name (required).
- `-mongodb-database-credentials`: The database credentials (optional).
- `-mongodb-username`: The MongoDB username (optional).
- `-mongodb-password`: The MongoDB password (optional).
- `-label-names`: The names of the labels to add. Comma separated. Will add predefined default values if not provided.
- `-username`: A specific user to add labels to (optional).
- `-mode`: You can choose to run a `dry-run` or `live`.

#### Example

To run the default label generation, use the following command:

```sh
go run main.go -action generate-default-labels \
               -mode dry-run \
               -mongodb-uri "mongodb+srv://<username>:<password>@<host>/<database>?retryWrites=true&w=majority&appName=<appName>" \
               -mongodb-source-database=<sourceDatabase> \
               -label-names=<labelNames> \

```

Add -username to add labels to just one specific user

### Reprocess media

This tool re-queues media for analysis when analysis has not been created yet.

#### Command Line Arguments

- `-action`: The action to take (required). For reprocessing, use `reprocess-media`.
- `-mongodb-uri`: The MongoDB URI (optional if host and port are provided).
- `-mongodb-host`: The MongoDB host (optional if URI is provided).
- `-mongodb-port`: The MongoDB port (optional if URI is provided).
- `-mongodb-source-database`: The Vault database name (required, used to fetch queue config).
- `-mongodb-destination-database`: Ignored for this action (reprocess runs within the source database).
- `-mongodb-database-credentials`: The database credentials (optional).
- `-mongodb-username`: The MongoDB username (optional).
- `-mongodb-password`: The MongoDB password (optional).
- `-queue`: The queue used to send analysis events (required).
- `-user-id`: The hub user ID to reprocess.
- `-start-timestamp`: The start timestamp for filtering media (required).
- `-end-timestamp`: The end timestamp for filtering media (required).
- `-timezone`: The timezone for converting timestamps (optional, default is `UTC`).
- `-mode`: You can choose to run a `dry-run` or `live`.
- `-batch-size`: The size of each batch (optional, default is `10`).
- `-batch-delay`: The delay between batches in milliseconds (optional, default is `1000`).

#### Example

```sh
go run main.go -action reprocess-media \
               -mongodb-uri "mongodb+srv://<username>:<password>@<host>/<database>?retryWrites=true&w=majority&appName=<appName>" \
               -mongodb-source-database=<vaultDatabase> \
               -mongodb-destination-database=<ignored> \
               -queue <analysis-queue> \
               -user-id <userId> \
               -start-timestamp <startTimestamp> \
               -end-timestamp <endTimestamp> \
               -timezone <timezone> \
               -mode dry-run \
               -batch-size 100 \
               -batch-delay 1000
```
