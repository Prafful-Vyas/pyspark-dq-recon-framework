"""Local runner: `python -m dqframe run contracts/x.yml --run-date 2026-10-20 --landing data/landing --lake data/lake`."""

from __future__ import annotations

import argparse
import json
import sys
import uuid

from pyspark.sql import SparkSession

from . import contracts, pipeline


def spark_session(app: str = "dqframe") -> SparkSession:
    return (SparkSession.builder.appName(app).master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .getOrCreate())


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="dqframe")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("run")
    p.add_argument("contracts", nargs="+", help="contract files, in dependency order")
    p.add_argument("--run-date", required=True)
    p.add_argument("--landing", required=True)
    p.add_argument("--lake", required=True)
    args = ap.parse_args(argv)

    spark = spark_session()
    subs = {"landing": args.landing, "lake": args.lake, "run_date": args.run_date}
    refs, rc = {}, 0
    for path in args.contracts:
        c = contracts.load(path, **subs)
        batch = f"{args.run_date.replace('-', '')}-{uuid.uuid4().hex[:8]}"
        try:
            res = pipeline.run(spark, c, args.run_date, batch, refs, audit_path=f"{args.lake}/_audit")
            print(json.dumps({"contract": c.name, "status": res.status, "in": res.rows_in,
                              "valid": res.rows_valid, "quarantined": res.rows_quarantined,
                              "recon": {k: res.audit["reconciliation"][k] for k in
                                        ("matched", "missing_in_target", "value_mismatch", "passed")}}))
        except (pipeline.DQFailure, pipeline.ReconciliationFailure) as exc:
            print(json.dumps({"contract": c.name, "status": "FAILED", "error": str(exc)}))
            rc = 1
            break
        # Later contracts may reference this dataset (e.g. transactions -> merchants).
        refs[c.name] = (spark.read.parquet(c.target_path) if c.load_mode == "append"
                        else pipeline.io.read_snapshot(spark, c.target_path).where("is_current"))
    spark.stop()
    return rc


if __name__ == "__main__":
    sys.exit(main())
