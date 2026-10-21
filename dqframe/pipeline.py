"""One contract, one batch: read → DQ → quarantine → load → reconcile → audit."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import engine, io, recon, scd2
from .contracts import Contract


class DQFailure(RuntimeError):
    def __init__(self, contract: str, breaches: list[dict]):
        super().__init__(f"{contract}: data-quality thresholds breached: "
                         + "; ".join(f"{b['rule']} ({b.get('failed_pct', b.get('observed'))})" for b in breaches))
        self.breaches = breaches


class ReconciliationFailure(RuntimeError):
    pass


@dataclass
class RunResult:
    contract: str
    batch_id: str
    status: str
    rows_in: int
    rows_valid: int
    rows_quarantined: int
    audit: dict


def run(spark: SparkSession, c: Contract, run_date: str, batch_id: str,
        refs: dict[str, DataFrame] | None = None, audit_path: str | None = None) -> RunResult:
    t0 = time.time()
    audit: dict = {"contract": c.name, "batch_id": batch_id, "run_date": run_date, "load_mode": c.load_mode}
    status = "FAILED"
    try:
        src = io.read_source(spark, c, batch_id, run_date)
        dq = engine.apply(src, c, refs)
        valid_n = dq.valid.count()
        quarantined_n = dq.total - valid_n
        audit.update(rows_in=dq.total, rows_valid=valid_n, rows_quarantined=quarantined_n,
                     rules=dq.rule_metrics, dataset_checks=dq.dataset_metrics)

        if quarantined_n:
            # Quarantine is written even when the batch then fails, so the rejected
            # rows and the reasons for rejection are available for the source team.
            q = dq.quarantine.withColumn("_dq_errors", F.concat_ws(",", "_dq_errors")) \
                             .withColumn("_dq_warnings", F.concat_ws(",", "_dq_warnings"))
            io.write_partitions(q, f"{c.target_path.rstrip('/')}_quarantine", ("_run_date",))
        if not dq.passed:
            raise DQFailure(c.name, dq.breaches)

        valid = dq.valid.drop("_dq_warnings")
        attrs = [col.name for col in c.columns]
        if c.load_mode == "append":
            io.write_partitions(valid, c.target_path, c.partition_by)
            landed = spark.read.parquet(c.target_path).where(F.col("_batch_id") == batch_id)
            r = recon.reconcile(valid, landed, list(c.primary_key), attrs, list(c.recon_sums))
        else:
            current = io.read_snapshot(spark, c.target_path)
            dim = scd2.merge(current, valid.select(*attrs), list(c.business_key), list(c.tracked_columns), run_date)
            io.write_snapshot(spark, dim, c.target_path, batch_id)
            live = io.read_snapshot(spark, c.target_path).where("is_current")
            batch_keys = valid.select(*c.business_key)
            r = recon.reconcile(valid, live.join(batch_keys, list(c.business_key), "left_semi"),
                                list(c.business_key), list(c.tracked_columns))
        audit["reconciliation"] = r
        if not r["passed"]:
            raise ReconciliationFailure(f"{c.name}: target does not reconcile with source: "
                                        + json.dumps({k: r[k] for k in ('missing_in_target', 'unexpected_in_target',
                                                                        'value_mismatch')}))
        status = "SUCCEEDED"
        return RunResult(c.name, batch_id, status, dq.total, valid_n, quarantined_n, audit)
    except Exception as exc:
        audit["error"] = str(exc)[:2000]
        raise
    finally:
        audit.update(status=status, seconds=round(time.time() - t0, 2))
        if audit_path:
            write_audit(spark, audit_path, audit)


def write_audit(spark: SparkSession, path: str, record: dict) -> None:
    """Append one JSON document per run. As a Glue/Athena table this becomes the
    DQ dashboard: pass rates per rule per day, volumes, recon results."""
    payload = json.dumps(record, default=str)
    spark.createDataFrame([(record["contract"], record["run_date"], payload)], "contract string, run_date string, record string") \
         .write.mode("append").partitionBy("contract").json(path)
