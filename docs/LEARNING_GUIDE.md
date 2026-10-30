# Understand the project from the beginning

## 1. The real problem

A shop says an order costs ₹100. A payment system says it received ₹90. Someone must detect the
₹10 mismatch. Another order might have no payment, and another might have a failed attempt.
This project brings those different records together so that these problems become visible.

## 2. Follow one record

1. A CSV row says `O0000001` belongs to merchant `M2` and has a price in paise.
2. A nested JSON record says a payment attempt happened for that order.
3. Spark extracts fields from the JSON's `payment` object.
4. It trims spaces, normalizes status/currency and converts the timestamp to UTC.
5. It checks that the order exists and the amount/status/currency satisfy the contract.
6. Valid data enters `fact_payment`. Invalid data enters `quarantine` with its reason and raw JSON.
7. SQL sums only successful payments for each order and compares the total with the expected amount.
8. The report shows MATCHED, UNPAID or MISMATCH.

Money uses integers: ₹100.50 is 10050 paise. We do not sum floating-point amounts in the facts.
The reporting view divides by 100 only for display.

## 3. What each tool actually does

| Tool/concept | Plain explanation | Here |
|---|---|---|
| Python | General instructions and connecting systems | Generate, extract, commit, CLI and reports |
| SQL | Ask questions of tables | Reconciliation, regional revenue, constraints and rankings |
| Spark | Processes data through a computation plan | Reads, parses, validates and joins DataFrames |
| PySpark | Python interface to Spark | `transform()` uses Spark built-in functions |
| Airflow | Decides which job runs next and retries failures | Batch → check → report |
| Batch | Process a bounded collection of records | Existing payment files |
| Streaming | Keep looking for new arriving records | New files processed as microbatches |
| Checkpoint | Remember source progress across restarts | `data/checkpoint/` |
| Idempotency | Repeating work does not duplicate its effect | Payment key and run ledger |
| Quarantine | Separate bad records while preserving evidence | Reason + original JSON |
| Dimension | Descriptive information | Merchant name and region |
| Fact | Business events or measurable records | Order and payment attempt |

## 4. Why a checkpoint is not enough

Imagine the database commits a payment and Spark crashes before recording that the microbatch
finished. On restart, Spark may send that batch again. A normal INSERT would duplicate it.
Here the same run key returns the already-committed result. If the event arrives in a different
batch, its unique event ID still prevents duplication. The transaction also prevents a half-written
batch from appearing successful.

This guarantee depends on keeping the database and checkpoint together and using this one stream
namespace. It is not an unrestricted “exactly once everywhere” guarantee.

## 5. Why some records stop the pipeline

Four bad records among hundreds may be a normal quality issue: keep the good records and
quarantine the bad ones. If most records suddenly have unknown fields, the source may have changed.
The 20% ratio gate stops the batch so the problem is noticed. Its transaction is rolled back;
the failed job has no committed run row. The raw source and failure log remain available.

## 6. Optimization you can explain

- Broadcast the small order-ID reference so Spark can avoid shuffling the large payment side
  for this join. This is appropriate only while the reference is small.
- Use built-in Spark expressions rather than a row-by-row Python UDF.
- Select required columns rather than moving every source attribute downstream.
- Index payment order IDs and event times for serving queries.
- Use `toLocalIterator()` rather than collecting every payment row into a Python list. However,
  the database sink is still serial and a partition can still be large: this is not cluster-scale output.
- Four shuffle partitions and `local[2]` are conservative demo settings, not universal tuning.

Open `data/evidence/spark_plan.txt` and find the broadcast join. Inspect plan changes after you
remove the broadcast hint; measure runtime on a fresh data root. Do not compare a first run against
an idempotent replay and call that a speedup.

## 7. Your ownership exercises (complete at least three)

**Exercise A — stronger contract.** Reject blank merchant IDs and unexpected future payment dates.
Write one failing test first, implement validation, then show the test passing. Decide whether
future dates should fail or quarantine, and document why.

**Exercise B — refunds.** Introduce an explicit refund event with a reference to an original
successful payment. Model it separately so retrying a refund cannot decrease revenue twice.
Test full, partial, duplicate and excessive refunds. Do not simply allow negative payment amounts.

**Exercise C — analytics.** Add a SQL view for merchants whose failed-payment rate exceeds 10%,
with a minimum attempt count so a one-payment merchant does not cause a misleading alert.

**Exercise D — performance.** Generate 10,000 and 100,000 orders in separate new roots. Record
wall time, rows/second and machine details. Identify whether Spark startup, join or serial SQLite
writes dominate. Show measurements, not guessed percentage improvements.

**Exercise E — orchestrator recovery.** Set a deliberately strict quality threshold in the DAG,
trigger a run and inspect the retry. Fix the threshold using the documented source reject ratio,
rerun and capture the successful graph. Never hide bad data merely to obtain a green run.

**Exercise F — collaboration.** Ask a peer to review your refund schema or validation policy.
Record the issue, their actual feedback, your change and a test in GitHub.

## 8. Interview walkthrough (about five minutes)

- 0:00–0:40: Explain the unpaid/mismatched-payment problem and source formats.
- 0:40–1:30: Trace one nested JSON record through Spark validation into a fact table.
- 1:30–2:20: Show a batch run and a new streaming event.
- 2:20–3:10: Demonstrate replay and explain the crash-between-commit-and-checkpoint scenario.
- 3:10–4:00: Show SQL reconciliation and one quarantine reason.
- 4:00–4:40: Show your own Airflow DAG execution and a retry.
- 4:40–5:00: Explain one limitation and one change you personally made.

Questions you should answer without reading: What is the grain of each table? Why not float
money? Why can a LEFT JOIN expose unpaid orders? Does a failed payment count as revenue?
Why isn't a file stream Kafka? What happens if only the checkpoint is deleted? Why might the
first committed version of a conflicting event be nondeterministic when its source order changes?
How would you redesign for many writers and a very large reference table?
