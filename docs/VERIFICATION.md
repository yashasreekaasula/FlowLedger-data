# Verification record

This describes tests executed while preparing the ZIP on 30 October 2026.
It proves these code paths ran in this environment; it is not evidence of the recipient's
personal experience. Reproduce them and keep your own dated results.

## Executed successfully

Environment: Linux, Python 3.12.14, OpenJDK 17.0.20, PySpark 3.5.7. Airflow 2.10.5 was
installed separately using its official Python 3.12 constraint file.

| Check | Observed result | Evidence |
|---|---|---|
| PySpark batch on 1,000 synthetic orders | 914 input records → 900 accepted, 4 rejected, 10 duplicates | `evidence/batch.log` |
| Same batch replay | `replayed: true`; no new facts or committed run | `evidence/batch_replay.log` |
| Structured Streaming available-now run | 5 new files → 5 accepted payments; total 905 | `evidence/stream.log` |
| Restart with same checkpoint | Total remains 905, six committed runs | `evidence/restart_assertions.json` |
| Existing event copied into new stream file | One duplicate, zero inserts; total remains 905 | `evidence/duplicate_stream.log` |
| Unit/integration tests | 11 passed, including actual Spark transformation | `evidence/tests.log` |
| Independent fresh-root multi-process verification | 100 orders / 90 batch payments, then 92 after two stream events; unchanged after restart/redelivery; temporary non-JSON file ignored | `evidence/user_verification.json` |
| Airflow DAG execution via `airflow dags test` | All three real tasks succeeded | `evidence/airflow_dag_test.log` |
| Warehouse accounting and referential checks | Passed | `evidence/check.log` |
| All four SQL analytics statements | Executed successfully | `evidence/analytics_results.json` |
| Report generation | Actual run metrics and HTML generated | `evidence/dashboard.html`, `evidence/metrics.json` |

A portable SQLite backup of the verified 1,000-order dataset is included at
`evidence/warehouse_snapshot.sqlite`. It contains 905 payment events, four quarantine records
and seven committed sink batches (one batch, five new-event microbatches, one duplicate delivery).
The source fixture in `examples/synthetic_sources/` corresponds to the initial batch;
stream events are synthetic records with runtime IDs and timestamps.

## Correctness fix found during verification

An initial restart check exposed loss of the final observed write across processes while database
connections were left open. The sink now uses an explicit context manager to commit/roll back and
close every connection. All reported results above are from the corrected implementation. The
independent process-level verification script guards against recurrence. No unverified claim about
the underlying filesystem or SQLite implementation is made.

Atomic publication was also hardened: temporary filenames are hidden, and the stream reads only
`*.json`. The independent test intentionally leaves an unfinished `.tmp` file in the inbox.

## Not verified here

- Docker image build, Compose startup, volume permissions on your machine, or Airflow web UI.
  Docker was not available in this environment. The actual Airflow CLI DAG test did run locally.
- A long-running Airflow scheduler, automatic daily triggers or retry timing under scheduler control.
- Continuous-mode latency SLA. Streaming was executed in bounded `availableNow` mode with genuine
  microbatches and durable checkpoints; the five-second continuous trigger is configured in code.
- Hosted GitHub Actions. A workflow is provided but has not run in your GitHub account.
- Large-scale throughput, cloud deployment, external payment APIs, Kafka, CDC or production load.
- Interactive dashboard appearance in a browser; its generation and data were verified.

Durations in logs describe this environment. They are not repeatable performance guarantees.
The Airflow `dags test` logical date is historical; its displayed DagRun duration is not a benchmark.
Docker uses Python 3.11, while the direct verification used Python 3.12. Both are supported targets
for the pinned tools, but the Docker variant must be tested on your computer.
