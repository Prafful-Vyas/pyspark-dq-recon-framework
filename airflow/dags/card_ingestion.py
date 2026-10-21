"""Daily card-data ingestion on AWS Glue, orchestrated by Airflow (MWAA).

    landing _SUCCESS marker ─► merchants (SCD2) ─► card_transactions (append, refs merchants)
                                                   ─► publish DQ summary to CloudWatch

Each Glue run executes dqframe/glue_entry.py for one contract. A breached DQ
threshold or failed reconciliation fails the Glue run, so the task fails and nothing
downstream reads a bad batch. Retries are for infrastructure errors only: a DQ
failure re-run against the same files fails the same way.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

LANDING = Variable.get("cards_landing", default_var="s3://bank-landing/cards")
LAKE = Variable.get("cards_lake", default_var="s3://bank-lake/cards")
ARTIFACTS = Variable.get("dqframe_artifacts", default_var="s3://bank-artifacts/dqframe")
GLUE_ROLE = Variable.get("dqframe_glue_role", default_var="dqframe-glue")


def glue_job(task_id: str, contract: str, references: str = "") -> GlueJobOperator:
    args = {
        "--contract": contract,
        "--run_date": "{{ ds }}",
        "--landing": LANDING,
        "--lake": LAKE,
        "--extra-py-files": f"{ARTIFACTS}/dqframe.zip",
        "--extra-files": f"{ARTIFACTS}/contracts/{contract}",
        "--enable-metrics": "true",
        "--enable-continuous-cloudwatch-log": "true",
    }
    if references:
        args["--references"] = references
    return GlueJobOperator(
        task_id=task_id,
        job_name=f"dqframe-{task_id}",
        script_location=f"{ARTIFACTS}/glue_entry.py",
        iam_role_name=GLUE_ROLE,
        create_job_kwargs={"GlueVersion": "5.0", "WorkerType": "G.1X", "NumberOfWorkers": 4},
        script_args=args,
        wait_for_completion=True,
        verbose=True,
    )


@dag(
    schedule="0 3 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,  # SCD2 snapshots must be applied in date order
    default_args={"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["cards", "dq"],
)
def card_ingestion():
    landed = S3KeySensor(
        task_id="landing_complete",
        bucket_key=f"{LANDING}/card_transactions/{{{{ ds }}}}/_SUCCESS",
        mode="reschedule",
        poke_interval=300,
        timeout=6 * 3600,
    )
    merchants = glue_job("merchants", "merchants.yml")
    transactions = glue_job("card_transactions", "card_transactions.yml",
                            references=f"merchants={LAKE}/merchants")

    @task
    def publish_dq_metrics(ds=None):
        """Read today's audit records and publish rule failure rates as CloudWatch
        metrics, so dashboards and alarms do not depend on Athena."""
        s3 = AwsBaseHook(client_type="s3").get_client_type()
        cw = AwsBaseHook(client_type="cloudwatch").get_client_type()
        bucket, prefix = LAKE.removeprefix("s3://").split("/", 1)
        metrics = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}/_audit/"):
            for obj in page.get("Contents", []):
                for line in s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read().decode().splitlines():
                    rec = json.loads(json.loads(line)["record"])
                    if rec["run_date"] != ds:
                        continue
                    dims = [{"Name": "Contract", "Value": rec["contract"]}]
                    metrics.append({"MetricName": "RowsQuarantined", "Dimensions": dims,
                                    "Value": rec.get("rows_quarantined", 0)})
                    metrics += [{"MetricName": "RuleFailurePct",
                                 "Dimensions": dims + [{"Name": "Rule", "Value": r["rule"]}],
                                 "Value": r["failed_pct"]} for r in rec.get("rules", [])]
        for i in range(0, len(metrics), 1000):
            cw.put_metric_data(Namespace="DataQuality/Cards", MetricData=metrics[i:i + 1000])
        return len(metrics)

    landed >> merchants >> transactions >> publish_dq_metrics()


card_ingestion()
