# Contracts, operations and troubleshooting

## Contracts

- Merchant source: SQLite table `merchants(merchant_id, merchant_name, region)`. Unique immutable ID.
- Orders: header CSV: `order_id,merchant_id,order_date,amount_cents,currency`. One immutable order
  per ID, positive integer paise, INR, parseable date, existing merchant. Invalid reference input
  aborts reference loading; it does not enter payment quarantine.
- Payments: newline-delimited JSON. `event_id`, `order_id`, ISO timestamp `event_time`, and nested
  `payment` with integer `amount_cents`, `currency`, `status`. Status is SUCCESS or FAILED.
  IDs are trimmed; currency/status are trimmed and uppercased. Times are normalized to UTC with
  millisecond precision. Each event is a payment attempt, not a mutable payment status record.
- Unknown extra JSON fields are ignored by the explicit schema. Missing/invalid required fields
  are rejected. Schema evolution requires an intentional schema, migration and test change.
- Event fingerprints use normalized business columns. Duplicate raw JSON formatting does not
  change business identity. If an event ID has different normalized content, quarantine the
  later processed version. The first commit wins; source processing order is not a business priority.
- Producer files must be immutable after publication. Write temporary file then atomically rename
  to `.json`. Never edit an already-processed file in place: Spark's file source does not represent CDC.

## Monitoring

`pipeline_run` records successful sink transactions: input, inserted, rejected and duplicate counts,
completion time and elapsed seconds. These numbers must balance. `failures/*.json` records exceptions
that reach the Python error handler, including gate failures; hard process termination cannot
write a failure log. Airflow task logs and OS/container logs cover those cases.

`data/evidence/stream_progress.json` is written after a bounded `stream --once` run completes.
Continuous processing prints committed microbatch summaries to stdout; Ctrl+C may not save
that final progress file. The warehouse audit ledger remains the source of committed-run evidence.
`report` is a snapshot, not a live dashboard. `check` checks invariants; it does not enforce a freshness SLA.
Synthetic event dates may be historical and must not be confused with ingestion time.

## Recovery scenarios

| Symptom | Diagnose | Recover |
|---|---|---|
| Quality gate failure | Inspect failure JSON and raw sources; compare threshold with actual reject ratio | Batch: fix source contract/data and create an immutable replacement input. Stream: fix code/policy and replay same microbatch; don't mutate published files |
| Crash during commit | Tests simulate this; transaction rolls back | Rerun the same command with unchanged root |
| Crash after commit but before checkpoint | Ledger already has the microbatch | Restart with same checkpoint; idempotent sink returns prior run |
| Unknown order | Source order not staged or reference snapshot stale | Load new orders using batch; restart stream to refresh its static reference |
| Conflicting event ID | Compare raw JSON with accepted record | Investigate upstream identity; corrections need a separate explicit correction model |
| Duplicate delivery | Counts show duplicates; totals should not grow | No manual cleanup needed |
| No new stream records | No new immutable file, producer finished, or checkpoint has processed it | Emit a new event, check logs and wait for commit |
| Database locked | Another local process holds a long write transaction | Stop overlapping writers, wait for current task; 60s DB timeout then retry |
| Java gateway failure | `java -version`, JAVA_HOME, memory and port-binding checks | Use Java 17 and WSL2/Linux, allocate more memory if needed |
| Airflow DAG absent | Check container startup and `airflow dags list-import-errors` | Confirm code exists at `/opt/flowledger/dags`, rebuild after code changes |
| Docker permission error | Named volume initialized with incompatible ownership | Fix volume ownership using the Airflow image user's UID; don't chmod your entire host |

### Poison batches and quarantine

If rejection exceeds the gate, even rejected rows roll back with the batch. To inspect them, examine
raw archived inputs and rerun in a separate diagnostic data root with a documented higher threshold.
This does not silently advance the primary stream. For this demo, after investigating a known bad
batch you may explicitly run `stream --once --max-bad-ratio 1` to commit quarantine and advance;
record why that exception was accepted. Do not make this your default quality policy.

### Reset and backfill

Safest full replay: use a brand-new `--root` and regenerate the synthetic inputs or stage a copy of
raw sources there. Do not copy the old checkpoint into a fresh warehouse or delete just the old
checkpoint while keeping the run ledger. `stream-v1:{batch_id}` is a single fixed stream namespace.
Backups must capture the checkpoint and warehouse consistently with all writers stopped.

Batch keys hash the complete order, merchant and payment snapshots. New source content gives a
new run key; event IDs prevent repeated older payments from being counted again. This is snapshot
batch ingestion, not an incremental source-offset algorithm. A production historical backfill
would use partitioned source intervals, isolated checkpoints and a versioned transform identifier.
The current content key does not include code version: logic changes need a deliberate new root
or versioned run-key migration, not a silent rerun expecting transformation to replace old facts.

## Production migration (planned, not implemented)

1. Replace local SQLite sink with PostgreSQL (transactional upserts) or a suitable transactional
   lake table; implement and test sink-specific replay semantics and concurrent writer behavior.
2. Use durable object storage for immutable raw inputs and checkpoints, with atomic-publication
   semantics appropriate to that storage rather than assuming local `os.replace` behavior.
3. Replace demo file producer with a real source connector/Kafka if needed. Define partitions,
   offsets and delivery guarantees; do not claim existing Kafka support.
4. Use deployed Airflow services and a production metadata database, secrets management,
   access controls, alert routing and measured freshness/error SLOs.
5. Scale joins without broadcasting large reference tables; avoid serial driver-side output.
6. Define retention, deduplication horizon, versioned schemas, correction events and migrations.
7. Replace static merchant dimension with SCD Type 2 only if historical attribution is required.

This project implements monitoring primitives and local tests, not high availability,
distributed transactions, compliance certification or live financial infrastructure.
