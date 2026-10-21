# PySpark Data Quality & Reconciliation Framework

A reusable, **contract-driven** PySpark ingestion framework. Onboarding a feed means
writing a YAML contract: schema, keys, data-quality rules with thresholds, load
mode and reconciliation measures. The framework handles the rest, and the same
code runs locally, on EMR, or as an **AWS Glue 5.0** job orchestrated by **Airflow**.

```
landing (S3/CSV/JSON/Parquet)
   │  explicit schema, PERMISSIVE + _corrupt_record (bad rows kept, never silently nulled)
   ▼
 DQ engine ── implicit rules (parse errors, NOT NULL columns, primary-key uniqueness)
   │          + contract rules: in_set, range, regex, expression, not_future,
   │            referential (broadcast join to a reference dataset), min_rows, freshness
   │          all row rules evaluated in ONE pass, failure counts in ONE aggregation
   ├──► quarantine/_run_date=…  (row + list of failed rules), always written
   ├──► thresholds per rule (max_failure_pct) ── breached? ─► DQFailure, nothing loaded
   ▼
 load ── append: dynamic partition overwrite (idempotent re-runs)
   │     scd2:   hash-based SCD2 merge → new immutable snapshot + _LATEST pointer
   ▼
 reconciliation ── row count + measure sums + per-key SHA-256 row hash (full outer join):
   │               missing_in_target / unexpected_in_target / value_mismatch
   ▼
 audit (JSON, partitioned by contract) ──► Athena / CloudWatch metrics
```

## Example: card transactions (`contracts/`)

* `merchants.yml`: reference data loaded as **SCD2**, so each transaction can be
  joined to the merchant attributes valid on its date.
* `card_transactions.yml`: daily authorisations, appended by `_run_date` partition. It
  includes a referential rule to merchants, masked-PAN regex, amount range, currency
  set, a not-in-future rule, volume and freshness checks, and warn-only rules that
  flag rows but keep them.

`scripts/gen_data.py` generates two business days of landing files with injected
defects at known rates (negative amounts, bad currencies, unmasked card numbers,
unknown merchants, nulls, malformed numbers, resent duplicates) and records the exact
counts in `_defects.json`.

## Results (local run, 2 × 20,015 rows)

| Check | Result |
|---|---|
| Rows quarantined, day 1 / day 2 | **493 / 505**, exactly equal to the injected error-severity defects |
| Reconciliation, both days | 19,522 / 19,510 rows matched, 0 missing, 0 unexpected, 0 value mismatches, amount totals equal |
| Re-run of day 2 | idempotent: 39,032 rows in target, 39,032 distinct `txn_id` |
| Merchant SCD2 after day 2 | 510 current versions, 25 closed (changed merchants), re-run is a no-op |
| Audit | one record per contract per run (6) |

## Design decisions

* **Rows are never silently lost.** The explicit schema plus `_corrupt_record` turns a
  malformed value into a quarantined row with `parse_error`. Spark's default would
  null it out.
* **Thresholds, not zero tolerance.** A trickle of bad rows is quarantined and the batch
  proceeds. A defect *rate* above `max_failure_pct` means something broke upstream,
  so the batch fails before anything is loaded. The quarantine and the audit record
  are still written, so the source team has the evidence.
* **Idempotency.** Appends use dynamic partition overwrite, so a re-run replaces its
  own partitions and nothing else. SCD2 writes a new snapshot directory each time and
  moves a `_LATEST` pointer. A table is never overwritten in place (Spark would read
  files it is deleting), and the last 7 snapshots are kept for rollback. A test
  re-runs a batch with the *same* batch ID to prove this.
* **Reconciliation proves the load.** The target is read back after the write and
  compared key by key using null-safe SHA-256 row hashes, plus count and sum totals.
* **Portable.** Filesystem operations go through the Hadoop FileSystem API, so the same
  code works on local disk, HDFS and S3 without boto3.

## Run

```bash
pip install -e .[dev]            # needs Java 17 for local Spark
python scripts/gen_data.py --out data/landing --dates 2026-10-19 2026-10-20
python -m dqframe run contracts/merchants.yml contracts/card_transactions.yml \
    --run-date 2026-10-19 --landing data/landing --lake data/lake
pytest
```

AWS: CI packages `dqframe.zip`, `glue_entry.py` and the contracts. The Airflow DAG
(`airflow/dags/card_ingestion.py`) waits for the landing `_SUCCESS` marker, runs the
merchants Glue job, then transactions with the merchant reference, then publishes
per-rule failure rates to CloudWatch.

## What was verified

* `pytest`: 21 tests on a real local Spark 3.5 session. They cover contract validation,
  every rule type, error vs warn and threshold breaches, referential nulls,
  reconciliation classification and null safety, the SCD2 lifecycle and idempotency,
  snapshot rewrite under the same batch ID with retention, a full two-day end-to-end
  run plus re-run with exact quarantine counts, and a threshold breach that loads
  nothing but keeps the quarantine and audit.
* **Not run on AWS.** The Glue entrypoint and the Airflow DAG are not executed in
  this repo's tests. CI checks that the DAG imports cleanly in Airflow 2.10 and has
  the expected task order. Running it needs a Glue role, S3 buckets and an MWAA
  environment.
