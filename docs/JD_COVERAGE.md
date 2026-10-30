# JD → executable implementation → personal evidence

| JD requirement | Implemented here | What you should show an interviewer |
|---|---|---|
| Reliable batch and near-real-time pipelines | `batch()` and `stream()` in pipeline.py; transactional sink | First batch output, new event arriving, checkpoint restart with unchanged totals |
| SQL and Python extraction, transformation, validation | sqlite3 merchant extraction, CSV read, Spark transforms, analytics.sql | Explain one extraction and run a join/CTE/window query |
| Structured and semi-structured multisource data | Relational merchant database, CSV orders, nested JSON payments | Trace a single order and payment from raw source to SQL result |
| Ingest, clean, transform, quality checks | Explicit schemas, trim/uppercase, UTC parsing, reference checks, quarantine, ratio gate | Inject invalid currency and show rejection; trigger quality failure and explain rollback |
| Apache Spark and PySpark | Actual Spark DataFrames, built-in expressions, broadcast join, Structured Streaming | Explain the saved physical plan and why Python UDFs are unnecessary here |
| Airflow workflows | Three dependent BashOperator tasks, retries, timeout, daily schedule | Screenshot graph, successful run and a failed/retried task from your own Docker execution |
| Data modeling and downstream reporting | Merchant dimension, order/payment facts, keys, views and dashboard | Explain fact grain, avoid counting FAILED payments as revenue, query reconciliation mismatches |
| Monitor, troubleshoot, optimize | Counts, timings, failure JSON, Spark plan, indexes, projection and broadcast | Diagnose failure, show recovery; measure your own performance before/after one change |
| Reliability, documentation, automation and collaboration | Tests, CI workflow, contracts and runbook | Your commit history, CI run, design decision and code-review discussion |

A repository cannot establish that you personally collaborated with a team, deployed to a cloud,
or operated a production system. Record real collaboration if it happens; otherwise present it
as an individual portfolio project. CI workflow is included, but a hosted CI run only exists after
pushing to your own repository with Actions enabled.

## Evidence checklist to build yourself

1. Record Python, Java and Spark versions and the command used.
2. Save the first-run metrics and raw data counts.
3. Save the batch replay showing `replayed: true` and unchanged payment count.
4. Record a new stream event, its committed run, and the updated SQL result.
5. Restart stream with the same checkpoint and show no duplicate payment.
6. Run the tests; inspect the injected-crash and quality-gate rollback tests.
7. Capture your own Airflow graph and task logs. Bundled verification does not claim this UI was run.
8. Change one feature (suggestions in LEARNING_GUIDE.md), add an independent expected-result test,
   and retain the diff, reason and before/after evidence.
9. Walk through one bug or failed assumption and the fix in your own words.

## Resume wording — use only after you complete the work

“Implemented and extended a payment reconciliation project using Python, SQL and PySpark,
with batch ingestion, checkpointed file-stream processing, transactional deduplication,
data-quality quarantine, and Airflow workflow orchestration.”

Add actual dataset size, measured throughput and your contribution only after measuring it.
Do not label it employment, production banking experience, Kafka, cloud deployment or certified
competence. Disclose AI assistance if asked and focus on the parts you understand and changed.
