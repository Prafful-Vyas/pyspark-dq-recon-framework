"""AWS Glue 5.0 (Spark 3.5) job entrypoint.

Deploy this file as the job script, the `dqframe` package as `--extra-py-files`
(a zip), and the contract YAMLs as `--extra-files`. Job parameters:

  --contract card_transactions.yml   --run_date 2026-10-20
  --landing  s3://bank-landing/cards  --lake s3://bank-lake/cards
  --references merchants=s3://bank-lake/cards/merchants   (optional, comma-separated)

A DQ or reconciliation failure raises, so the Glue run is FAILED and the Airflow
task fails with it. The quarantine and audit record are still written first.
"""

import sys

from awsglue.context import GlueContext  # type: ignore[import-not-found]  # provided by the Glue runtime
from awsglue.job import Job  # type: ignore[import-not-found]
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.context import SparkContext

from dqframe import contracts, io, pipeline


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "JOB_RUN_ID", "contract", "run_date", "landing", "lake"])
    opt = getResolvedOptions(sys.argv, ["references"]) if "--references" in sys.argv else {"references": ""}
    glue = GlueContext(SparkContext.getOrCreate())
    spark = glue.spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    c = contracts.load(args["contract"], landing=args["landing"], lake=args["lake"], run_date=args["run_date"])
    refs = {}
    for item in filter(None, opt["references"].split(",")):
        name, path = item.split("=", 1)
        snap = io.read_snapshot(spark, path)
        refs[name] = snap.where("is_current") if snap is not None else spark.read.parquet(path)

    pipeline.run(spark, c, args["run_date"], args["JOB_RUN_ID"], refs, audit_path=f"{args['lake']}/_audit")
    job.commit()


if __name__ == "__main__":
    main()
